// MTS-Link collaborative whiteboard transport - smuggles packets through cursor updates.
// Script-transport port of transport/mtslink/mtslink.go.
//
// Two peers open the same MTS-Link board and tunnel packets through cursor
// position updates in the real-time collaboration protocol, encoded as base64
// strings in cursor X coordinates.

var UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36";
var PING_INTERVAL_MS = 30000;
var READ_TIMEOUT_MS = 90000;
var MAX_RECONNECT_ATTEMPTS = 999999;

var wizard = require("../../lib/wizard.js");
var wizardServer = null;
var urlFromParams = false;

var boardURL = "";
var running = false;
var sock = null;
var baseInfo = null; // { token, boardUID, clientUID, wsDomain, signature, etc }
var sessionUID = "";
var reconnectAttempt = 0;
var reconnectTimer = null;
var pingTimer = null;
var connectedAt = null;

// Generate random guest name
function generateRandomName() {
  var adjectives = ["Fast", "Brave", "Silent", "Quick", "Smart"];
  var nouns = ["Tiger", "Eagle", "Wolf", "Falcon", "Lion"];

  var adj = adjectives[Math.floor(Math.random() * adjectives.length)];
  var noun = nouns[Math.floor(Math.random() * nouns.length)];
  var num = Math.floor(Math.random() * 1000);

  return adj + noun + num;
}

// Extract baseInfo from HTML page
function extractBaseInfo(html) {
  var startIdx = html.indexOf("const baseInfo = ");
  if (startIdx === -1) throw new Error("baseInfo not found in HTML");
  startIdx += "const baseInfo = ".length;

  // Find end of object by counting braces
  var braceCount = 0;
  var inString = false;
  var escape = false;
  var endIdx = -1;

  for (var i = startIdx; i < html.length; i++) {
    var char = html[i];

    if (escape) {
      escape = false;
      continue;
    }
    if (char === "\\") {
      escape = true;
      continue;
    }
    if (char === '"') {
      inString = !inString;
      continue;
    }
    if (inString) continue;

    if (char === "{") {
      braceCount++;
    } else if (char === "}") {
      braceCount--;
      if (braceCount === 0) {
        endIdx = i + 1;
        break;
      }
    }
  }

  if (endIdx === -1) throw new Error("failed to find end of baseInfo");

  var jsonStr = html.substring(startIdx, endIdx);

  // Convert JS object to JSON (add quotes to keys)
  jsonStr = jsonStr.replace(/(\s+)(\w+)(\s*):/g, '$1"$2"$3:');

  return JSON.parse(jsonStr);
}

// buildWsURL / buildInit: the board's WebSocket address and the init frame, pure and shared by the transport and the
// setup check so a green check walks the transport's own path. Wire names mirror native initMessage struct tags in
// mtslink.go: boardUID is `board_uid` (snake), everything else camelCase; the server closes (1006) on wrong case.
function buildWsURL(baseInfo) {
  return baseInfo.wsDomain + "/ws/1?clientUID=" + baseInfo.clientUID + "&locale=en";
}

function buildInit(baseInfo) {
  return {
    type: "init",
    board_uid: baseInfo.boardUID,
    token: baseInfo.clientUID,
    boardAccessToken: "",
    jwt: "",
    clientUID: baseInfo.clientUID,
    prefix: baseInfo.fePrefix || "",
    signature: baseInfo.signature || "",
    temporary: baseInfo.temporary || false,
    guestName: generateRandomName()
  };
}

// Authorization: fetch board page
async function authorize(url) {
  var res = await http.fetch({
    url: url,
    method: "GET",
    headers: {
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "User-Agent": UA,
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none"
    }
  });

  if (res.status !== 200) {
    throw new Error("board request: HTTP " + res.status);
  }

  var info = extractBaseInfo(res.body);
  return info;
}

// Reconnect backoff
function reconnectBackoff(n) {
  if (n < 1) n = 1;
  var shift = n - 1;
  if (shift > 5) shift = 5;
  var d = 500 * Math.pow(2, shift);
  if (d > 15000) d = 15000;
  d += Math.floor(Math.random() * (d / 2 + 1));
  return d;
}

function scheduleReconnect(attempt) {
  var next = attempt + 1;
  if (!running || next >= MAX_RECONNECT_ATTEMPTS) return;
  if (reconnectTimer) return;

  var d = reconnectBackoff(next);
  reconnectTimer = setTimeout(function() {
    reconnectTimer = null;
    if (!running) return;
    connectToBoard(next);
  }, d);
}

// Connect to WebSocket
async function connectToBoard(attempt) {
  reconnectAttempt = attempt;

  if (attempt === 0) {
    setState("connecting");
  } else {
    setState("reconnecting");
  }

  try {
    if (!baseInfo) {
      console.log("[MTS] authorizing...");
      baseInfo = await authorize(boardURL);
      console.log("[MTS] auth OK, full baseInfo:", JSON.stringify(baseInfo, null, 2));
    }

    var wsURL = buildWsURL(baseInfo);
    console.log("[MTS] connecting to:", wsURL);

    // Add timeout to ws.open
    var connectPromise = ws.open(wsURL, {
      "Origin": "https://my.mts-link.ru",
      "Cache-Control": "no-cache",
      "Accept-Language": "en-US,en;q=0.9",
      "Pragma": "no-cache",
      "User-Agent": UA
    }, { readTimeoutMs: READ_TIMEOUT_MS });

    var timeoutPromise = new Promise(function(resolve, reject) {
      setTimeout(function() {
        reject(new Error("WebSocket connection timeout after 10s"));
      }, 10000);
    });

    sock = await Promise.race([connectPromise, timeoutPromise]);

    console.log("[MTS] WebSocket connected");

    sock.onmessage = function(data) {
      try {
        console.log("[MTS] onmessage called! type:", typeof data, "instanceof ArrayBuffer:", data instanceof ArrayBuffer);
        onMessage(data);
      } catch (e) {
        console.error("[MTS] onmessage exception:", e, e.stack);
      }
    };

    sock.onclose = function(reason) {
      try {
        console.log("[MTS] onclose called! reason:", reason);
        onClose(reason);
      } catch (e) {
        console.error("[MTS] onclose exception:", e, e.stack);
      }
    };

    // Send init message (see buildInit for the wire-name notes).
    var initMsg = buildInit(baseInfo);
    console.log("[MTS] sending init message:", JSON.stringify(initMsg, null, 2));

    // Try sending as text first
    var jsonStr = JSON.stringify(initMsg);
    sock.send(jsonStr);

    console.log("[MTS] init sent (text), waiting for initResponse...");
    // State transition to "connected" happens in onMessage when initResponse arrives

  } catch (e) {
    setState("reconnecting", String(e));
    scheduleReconnect(attempt);
  }
}

function startPingLoop() {
  if (pingTimer) clearInterval(pingTimer);

  pingTimer = setInterval(function() {
    if (!running || !sock) {
      if (pingTimer) clearInterval(pingTimer);
      return;
    }

    try {
      var pingMsg = { type: "pingRequest" };
      sock.send(JSON.stringify(pingMsg));
    } catch (e) {
      console.error("[MTS] ping error:", e);
    }
  }, PING_INTERVAL_MS);
}

function onMessage(data) {
  console.log("[MTS] received message, type:", typeof data, "instanceof ArrayBuffer:", data instanceof ArrayBuffer);

  // MTS uses binary messages - need to decode to string first
  if (data instanceof ArrayBuffer) {
    try {
      var str = text.decode(data);
      var msg = JSON.parse(str);
      console.log("[MTS] parsed binary JSON message:", msg.type, msg.subtype || "");

      if (msg.type === "initResponse") {
        if (msg.sessionUID) {
          sessionUID = msg.sessionUID;
          console.log("[MTS] got sessionUID:", sessionUID);
        }
        onReady();
      } else if (msg.type === "fast" && msg.subtype === "view") {
        handleFastView(msg.data);
      }
    } catch (e) {
      console.error("[MTS] binary message parse error:", e);
    }
  } else if (typeof data === "string") {
    try {
      var msg = JSON.parse(data);
      console.log("[MTS] parsed text JSON message:", msg.type, msg.subtype || "");

      if (msg.type === "initResponse") {
        if (msg.sessionUID) {
          sessionUID = msg.sessionUID;
          console.log("[MTS] got sessionUID:", sessionUID);
        }
        onReady();
      } else if (msg.type === "fast" && msg.subtype === "view") {
        handleFastView(msg.data);
      }
    } catch (e) {
      console.error("[MTS] text message parse error:", e);
    }
  }
}

// onReady: initResponse has arrived, mark transport connected so Session can
// start sending. Idempotent - subsequent initResponses (shouldn't happen, but
// in case of reconnect race) are no-ops.
function onReady() {
  if (connectedAt) return;
  connectedAt = Date.now();
  reconnectAttempt = 0;
  startPingLoop();
  setState("connected");
}

function handleFastView(data) {
  if (!data) return;

  // Skip our own messages
  if (data.token === baseInfo.clientUID) return;

  // Extract base64 from cursor X coordinate
  var x = data.cursorPosition ? data.cursorPosition.x : null;
  if (!x || typeof x !== "string") return;

  try {
    var decoded = base64.decode(x);
    if (decoded.byteLength > 0) {
      emit(decoded);
    }
  } catch (e) {
    // Not valid base64, ignore
  }
}

function onClose(reason) {
  console.log("[MTS] WebSocket closed, reason:", reason);
  setState("reconnecting", reason);
  if (pingTimer) {
    clearInterval(pingTimer);
    pingTimer = null;
  }

  // Reset backoff if connection was healthy
  var next = reconnectAttempt + 1;
  if (connectedAt && (Date.now() - connectedAt) > 15000) {
    next = 0;
  }
  connectedAt = null;
  sessionUID = "";
  sock = null;

  if (running) {
    scheduleReconnect(next);
  }
}

// ---- setup wizard ----
//
// Transport.setup (an app runs it while a node is being made) and open() without a link serve the wizard page on a
// loopback port and ask the app to show it. The page checks the board here (/api/check) and hands back {client, node}
// through window.openfluxSubmit; the answer for this side returns as cookiesApplied.

var WIZARD_LABELS = {
  link: "Ссылка",
  page: "Страница доски",
  board: "Данные доски",
  socket: "Соединение с сервером"
};

var WIZARD_CFG = {
  service: "MTS-Link",
  title: "Подключение через MTS-Link",
  tagline: "Мастер настройки туннеля через доску",
  link: {
    title: "Шаг 1: Доска",
    desc: "Клиент и нода зайдут гостями на одну доску MTS-Link.",
    label: "Ссылка на доску",
    placeholder: "https://my.mts-link.ru/…",
    check: "Проверить доску",
    checking: "Проверяю доску…",
    found: "Доска найдена",
    summary: "Доска",
    howTitle: "Где взять ссылку и что нужно знать",
    how: [
      "В MTS-Link создайте доску и включите для неё доступ по ссылке (гость может редактировать).",
      "Скопируйте адрес открытой доски из адресной строки.",
      "Одна доска держит одну пару «клиент и нода». Для второй пары нужна вторая доска.",
      "Проверка заходит на доску гостем так же, как туннель, и тут же уходит: на доске ничего не остаётся."
    ]
  },
  create: {
    tab: "Создать на сайте",
    have: "Есть ссылка",
    desc: "Откроется MTS-Link: войдите, создайте доску, включите доступ по ссылке и откройте её. Когда доска откроется, нажмите «Это она».",
    button: "Открыть MTS-Link",
    waiting: "Создайте доску и откройте её в окне…",
    url: "https://my.mts-link.ru/",
    title: "MTS-Link",
    match: "^https://[^/]*mts-link\\.ru/.*(board|whiteboard)",
    hint: "Откройте доску и нажмите «Это она»"
  },
  mode: {
    title: "Шаг 2: Режим",
    desc: "Как клиент и нода будут связываться через доску.",
    onlyClient: "Нода уже работает на этой доске: настроить только клиента",
    note: "На одной доске работает одна пара. Вторую ноду на ту же доску не ставьте: пакеты смешаются."
  },
  options: []
};

// checkBoard walks the board the way the transport does (the page, the board's data, one socket that joins as a guest)
// and leaves again: it keeps nothing of the transport's own state, so it can run next to a live one. It reuses the
// transport's own pure builders (extractBaseInfo, buildWsURL, buildInit) so a green check means the transport's path
// is open.
async function checkBoard(raw, rep) {
  var link = String(raw || "").trim();
  if (!/^https:\/\/[^\/\s]+\//i.test(link)) {
    throw new wizard.StepError("link", "not an https address of a page");
  }
  rep("link", true, wizard.hostName(link));

  var page = await wizard.send("page", {
    url: link,
    method: "GET",
    headers: {
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "User-Agent": UA,
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none"
    }
  });
  if (page.status !== 200) {
    throw new wizard.StepError("page", "board page HTTP " + page.status, page.status);
  }
  rep("page", true, "HTTP 200");

  var baseInfo;
  try {
    baseInfo = extractBaseInfo(page.body);
  } catch (e) {
    throw new wizard.StepError("board", wizard.errText(e));
  }
  if (!baseInfo.boardUID || !baseInfo.clientUID || !baseInfo.wsDomain) {
    throw new wizard.StepError("board", "the page has no board data");
  }
  rep("board", true, "board data found");

  var s = await wizard.openSocket("socket", buildWsURL(baseInfo), {
    "Origin": "https://my.mts-link.ru",
    "Cache-Control": "no-cache",
    "Accept-Language": "en-US,en;q=0.9",
    "Pragma": "no-cache",
    "User-Agent": UA
  }, { readTimeoutMs: 20000 });
  try {
    s.send(JSON.stringify(buildInit(baseInfo)));
    var got = await s.wait(function (m) {
      var str = (m instanceof ArrayBuffer) ? text.decode(m) : (typeof m === "string" ? m : "");
      if (str.indexOf("initResponse") === -1) return false;
      try { return JSON.parse(str).type === "initResponse"; } catch (e) { return false; }
    }, 15000);
    if (got === null) throw new wizard.StepError("socket", "the board did not answer the guest join");
    rep("socket", true, wizard.hostName(baseInfo.wsDomain));
  } finally {
    s.close();
  }
  return {
    title: WIZARD_CFG.link.found,
    lines: [
      "Сервер: " + wizard.hostName(baseInfo.wsDomain),
      "Доска приняла вход гостя"
    ],
    url: link
  };
}

// explainFailure says in words what a failed step means for the board; "" leaves the kit's plain "refused at this step".
function explainFailure(e) {
  var st = e.status ? " (" + e.status + ")" : "";
  if (e.step === "link") return "Нужна ссылка на доску MTS-Link";
  if (e.step === "page") return "Страница доски не открывается" + st + ": доски нет или она закрыта";
  if (e.step === "board") return "На странице нет данных доски MTS-Link: ссылка устарела или это не доска";
  if (e.step === "socket") return "Доска MTS-Link не ответила на вход гостя" + st + ": проверьте, что доска открыта и доступна по ссылке";
  return "";
}

function startWizard(values) {
  if (wizardServer) return;
  wizardServer = wizard.serve({
    explain: explainFailure,
    tag: "[MTS]",
    service: "MTS-Link",
    labels: WIZARD_LABELS,
    reason: "Подключение через MTS-Link",
    cfg: WIZARD_CFG,
    values: { url: String((values && values.url) || "") },
    check: checkBoard
  });
}

// launch connects to the board the settings or the page left.
function launch() {
  running = true;
  baseInfo = null;
  connectToBoard(0);
}

// Transport interface
var Transport = {
  info: function() {
    return {
      name: "mtslink",
      version: "1.1.0",
      cookieDomain: "",
      mtu: 0,
      reliable: false,
      ordered: false,
      params: [
        { key: "url", label: "MTS-Link Board URL", type: "url", required: true }
      ]
    };
  },

  // setup is the wizard an app runs while a node is being made (before anything connects).
  setup: function(values) {
    startWizard(values || {});
  },

  open: function(cfg) {
    var p = (cfg && cfg.params) || {};
    boardURL = p.url || (cfg && cfg.url) || "";
    urlFromParams = !!boardURL;
    if (!boardURL) {
      // Nothing in the settings: what an earlier setup page left for this profile.
      boardURL = String((cookieJar.get() || {}).url || "").trim();
    }
    if (!boardURL) {
      if (p.exit === true || p.exit === "true") {
        // A node has no one to ask: it needs the link its setup gave it.
        setState("dead", "у ноды нет ссылки на доску: настройте ноду заново");
        return;
      }
      running = true;
      setState("connecting");
      startWizard({});
      return;
    }
    launch();
  },

  // cookiesApplied: the setup page's answer for this side. Settings the carrier was given outrank it.
  onEvent: function(kind) {
    if (kind !== "cookiesApplied" || !running || urlFromParams) return;
    var link = String((cookieJar.get() || {}).url || "").trim();
    if (!link || link === boardURL) return;
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
    if (sock) { try { sock.close(); } catch (e) {} sock = null; }
    baseInfo = null;
    connectedAt = null;
    sessionUID = "";
    reconnectAttempt = 0;
    boardURL = link;
    launch();
  },

  write: function(bytes) {
    if (!sock || !running) throw new Error("not connected");

    // Encode packet as base64 and send via fast/view. sessionUID may still be
    // empty if initResponse hasn't arrived yet - mirrors native (mtslink.go
    // sendFastView uses sess.sessionUID as-is, empty or not).
    var b64 = base64.encode(bytes);

    var msg = {
      type: "fast",
      subtype: "view",
      data: {
        sessionUID: sessionUID,
        name: "Guest",
        login: "",
        token: baseInfo.clientUID,
        cursorPosition: {
          x: b64,  // Send base64 string as X coordinate
          y: 350.0
        },
        viewPosition: {
          viewportStartX: 0,
          viewportStartY: 0,
          viewportWidth: 1920,
          viewportHeight: 1080
        }
      }
    };

    sock.send(JSON.stringify(msg));
  },

  close: function() {
    running = false;
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (pingTimer) {
      clearInterval(pingTimer);
      pingTimer = null;
    }
    if (sock) {
      try { sock.close(); } catch (e) {}
      sock = null;
    }
    connectedAt = null;
    sessionUID = "";
    if (wizardServer) {
      try { wizardServer.close(); } catch (e) {}
      wizardServer = null;
    }
  }
};
