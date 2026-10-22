// Bitrix24 Flipchart transport - smuggles packets through cursor position updates.
// Script-transport port of transport/bitrix/bitrix.go.
//
// Two peers open the same Bitrix24 board document and tunnel packets through
// awareness state updates in the collaborative editing protocol, encoded as
// base64 strings in cursor X coordinates.

var UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36";
var PING_INTERVAL_MS = 5000;
var READ_TIMEOUT_MS = 90000;
var MAX_RECONNECT_ATTEMPTS = 999999;

var wizard = require("../../lib/wizard.js");
var wizardServer = null;
var urlFromParams = false;

var boardURL = "";
var running = false;
var sock = null;
var info = null; // { token, wsHost, tabID, myUserID }
var reconnectAttempt = 0;
var reconnectTimer = null;
var pingTimer = null;
var connectedAt = null;

// Generate UUID v4
function generateUUID() {
  var hex = [];
  for (var i = 0; i < 16; i++) {
    hex.push(Math.floor(Math.random() * 256));
  }
  hex[6] = (hex[6] & 0x0f) | 0x40; // version 4
  hex[8] = (hex[8] & 0x3f) | 0x80; // variant 10

  var uuid = "";
  for (var i = 0; i < 16; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) uuid += "-";
    var byte = hex[i].toString(16);
    if (byte.length === 1) byte = "0" + byte;
    uuid += byte;
  }
  return uuid;
}

// Extract JWT payload
function decodeJWT(token) {
  var parts = token.split(".");
  if (parts.length < 2) throw new Error("invalid JWT format");

  var payload = parts[1];
  // Fix base64url padding
  payload = payload.replace(/-/g, "+").replace(/_/g, "/");
  while (payload.length % 4 !== 0) payload += "=";

  var decoded = base64.decode(payload);
  var json = text.decode(decoded);
  return JSON.parse(json);
}

// parseBoardPage pulls the board's token and app host out of the board page's HTML. Pure (sets no globals), so the
// setup check and the transport both read the page the same way.
function parseBoardPage(html) {
  // Extract token with regex
  var tokenMatch = html.match(/token:\s*['"]([^'"]+)['"]/);
  if (!tokenMatch) throw new Error("token not found in HTML");
  var token = tokenMatch[1];

  // Extract appUrl
  var appURLMatch = html.match(/appUrl:\s*['"]([^'"]+)['"]/);
  var appURL = "https://boards-ruv.i.bitrix24.ru";
  if (appURLMatch) {
    var parsed = url.parse(appURLMatch[1]);
    appURL = parsed.protocol + "//" + parsed.host;
  }

  // Decode JWT to get user info
  var payload = decodeJWT(token);

  // Extract WebSocket host
  var wsHost = "boards-ruv.i.bitrix24.ru";
  var parsedApp = url.parse(appURL);
  if (parsedApp.host) wsHost = parsedApp.host;

  return { token: token, payload: payload, appURL: appURL, wsHost: wsHost };
}

// buildWsURL is the flipchart WebSocket address, token in the query. Shared by the transport and the check.
function buildWsURL(info) {
  return "wss://" + info.wsHost + "/api/v1/flip/ws?token=" + encodeURIComponent(info.token);
}

// Authorization: fetch board page and extract token
async function authorize(boardURL) {
  var res = await http.fetch({
    url: boardURL,
    method: "GET",
    headers: {
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
      "User-Agent": UA,
      "Sec-Fetch-Dest": "document",
      "Sec-Fetch-Mode": "navigate",
      "Sec-Fetch-Site": "none",
      "Sec-GPC": "1"
    }
  });

  if (res.status !== 200) {
    throw new Error("board request: HTTP " + res.status);
  }

  var parsed = parseBoardPage(res.body);
  return {
    token: parsed.token,
    payload: parsed.payload,
    appURL: parsed.appURL,
    wsHost: parsed.wsHost,
    tabID: generateUUID(),
    myUserID: parsed.payload.user_id || ""
  };
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
    if (!info) {
      info = await authorize(boardURL);
    }

    var wsURL = buildWsURL(info);

    sock = await ws.open(wsURL, {
      "Origin": info.appURL,
      "Sec-GPC": "1",
      "Cache-Control": "no-cache",
      "Accept-Language": "en-US,en;q=0.9",
      "Pragma": "no-cache",
      "User-Agent": UA
    }, { readTimeoutMs: READ_TIMEOUT_MS });

    sock.onmessage = onMessage;
    sock.onclose = onClose;

    // Send initial Ping
    var initialPing = {
      type: "Ping",
      id: generateUUID(),
      tabId: info.tabID
    };
    sock.send(JSON.stringify(initialPing));

    connectedAt = Date.now();
    setState("connected");
    reconnectAttempt = 0;

    // Start ping loop
    startPingLoop();

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
      var pingMsg = {
        type: "Ping",
        id: generateUUID(),
        tabId: info.tabID
      };
      sock.send(JSON.stringify(pingMsg));
    } catch (e) {
      console.error("[BITRIX] ping error:", e);
    }
  }, PING_INTERVAL_MS);
}

function onMessage(data) {
  if (typeof data !== "string") return;

  try {
    var msg = JSON.parse(data);

    if (msg.type === "Ping") {
      // Respond with Pong
      var pongMsg = {
        type: "Pong",
        id: msg.id,
        tabId: info.tabID,
        payload: "pong"
      };
      sock.send(JSON.stringify(pongMsg));

    } else if (msg.type === "UserSync" || msg.type === "UpdateCursor") {
      // Extract tunnel data from cursor coordinates
      handleCursorUpdate(msg);
    }
  } catch (e) {
    console.error("[BITRIX] message parse error:", e);
  }
}

function handleCursorUpdate(msg) {
  // Skip our own messages
  if (msg.actor && msg.actor.userId === info.myUserID) return;

  if (!msg.payload) return;

  var payload = typeof msg.payload === "string" ? JSON.parse(msg.payload) : msg.payload;

  // Try to extract base64 from cursor X coordinate
  var x = null;
  if (payload.coordinates && payload.coordinates.x) {
    x = payload.coordinates.x;
  } else if (payload.cursorCoordinates && payload.cursorCoordinates.x) {
    x = payload.cursorCoordinates.x;
  }

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
  token: "Токен доски",
  socket: "Соединение с сервером"
};

var WIZARD_CFG = {
  service: "Bitrix24",
  title: "Подключение через Bitrix24",
  tagline: "Мастер настройки туннеля через доску-флипчарт",
  link: {
    title: "Шаг 1: Доска",
    desc: "Клиент и нода зайдут на одну доску-флипчарт Bitrix24.",
    label: "Ссылка на доску",
    placeholder: "https://…bitrix24.ru/…",
    check: "Проверить доску",
    checking: "Проверяю доску…",
    found: "Доска найдена",
    summary: "Доска",
    howTitle: "Где взять ссылку и что нужно знать",
    how: [
      "В Bitrix24 создайте доску-флипчарт и включите для неё общий доступ по ссылке.",
      "Скопируйте адрес открытой доски из адресной строки.",
      "Одна доска держит одну пару «клиент и нода». Для второй пары нужна вторая доска.",
      "Проверка заходит на доску так же, как туннель, и тут же уходит: на доске ничего не остаётся."
    ]
  },
  create: {
    tab: "Создать на сайте",
    have: "Есть ссылка",
    desc: "Откроется Bitrix24: войдите, создайте доску-флипчарт, включите доступ по ссылке и откройте её. Когда доска откроется, нажмите «Это она».",
    button: "Открыть Bitrix24",
    waiting: "Создайте доску и откройте её в окне…",
    url: "https://www.bitrix24.ru/",
    title: "Bitrix24",
    match: "^https://[^/]+\\.bitrix24\\.ru/.*(board|flip)",
    hint: "Откройте доску-флипчарт и нажмите «Это она»"
  },
  mode: {
    title: "Шаг 2: Режим",
    desc: "Как клиент и нода будут связываться через доску.",
    onlyClient: "Нода уже работает на этой доске: настроить только клиента",
    note: "На одной доске работает одна пара. Вторую ноду на ту же доску не ставьте: пакеты смешаются."
  },
  options: []
};

// checkBoard walks the board the way the transport does (the page, the token, one socket) and leaves again: it keeps
// nothing of the transport's own state, so it can run next to a live one. It reuses the transport's own pure builders
// (parseBoardPage, decodeJWT, buildWsURL) so a green check means the transport's own path is open.
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
      "Sec-Fetch-Site": "none",
      "Sec-GPC": "1"
    }
  });
  if (page.status !== 200) {
    throw new wizard.StepError("page", "board page HTTP " + page.status, page.status);
  }
  rep("page", true, "HTTP 200");

  var binfo;
  try {
    binfo = parseBoardPage(page.body);
  } catch (e) {
    throw new wizard.StepError("token", wizard.errText(e));
  }
  var exp = binfo.payload && binfo.payload.exp;
  if (exp && Number(exp) * 1000 < Date.now()) {
    throw new wizard.StepError("token", "the board token has expired");
  }
  rep("token", true, "board token ok");

  var s = await wizard.openSocket("socket", buildWsURL(binfo), {
    "Origin": binfo.appURL,
    "Sec-GPC": "1",
    "Cache-Control": "no-cache",
    "Accept-Language": "en-US,en;q=0.9",
    "Pragma": "no-cache",
    "User-Agent": UA
  }, { readTimeoutMs: 20000 });
  var answered = false;
  try {
    s.send(JSON.stringify({ type: "Ping", id: generateUUID(), tabId: generateUUID() }));
    var msg = await s.wait(function () { return true; }, 12000);
    answered = msg !== null;
  } finally {
    s.close();
  }
  rep("socket", true, binfo.wsHost + (answered ? " · ответил" : ""));
  return {
    title: WIZARD_CFG.link.found,
    lines: [
      "Сервер: " + binfo.wsHost,
      answered ? "Сервер ответил на соединение" : "Соединение открыто (сервер пока молчит — это не всегда ошибка)"
    ],
    url: link
  };
}

// explainFailure says in words what a failed step means for the board; "" leaves the kit's plain "refused at this step".
function explainFailure(e) {
  var st = e.status ? " (" + e.status + ")" : "";
  if (e.step === "link") return "Нужна ссылка на доску Bitrix24";
  if (e.step === "page") return "Страница доски не открывается" + st + ": доски нет или она закрыта";
  if (e.step === "token") return "На странице нет рабочего токена доски: ссылка устарела или это не доска-флипчарт Bitrix24";
  if (e.step === "socket") return "Не удалось подключиться к серверу доски Bitrix24" + st + ": проверьте, что доска открыта и доступна по ссылке";
  return "";
}

function startWizard(values) {
  if (wizardServer) return;
  wizardServer = wizard.serve({
    explain: explainFailure,
    tag: "[BITRIX]",
    service: "Bitrix24",
    labels: WIZARD_LABELS,
    reason: "Подключение через Bitrix24",
    cfg: WIZARD_CFG,
    values: { url: String((values && values.url) || "") },
    check: checkBoard
  });
}

// launch connects to the board the settings or the page left.
function launch() {
  running = true;
  info = null;
  connectToBoard(0);
}

// Transport interface
var Transport = {
  info: function() {
    return {
      name: "bitrix",
      version: "1.1.0",
      cookieDomain: "",
      mtu: 0,
      reliable: false,
      ordered: false,
      params: [
        { key: "url", label: "Bitrix24 Board URL", type: "url", required: true }
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
    connectedAt = null;
    reconnectAttempt = 0;
    boardURL = link;
    launch();
  },

  write: function(bytes) {
    if (!sock || !running) throw new Error("not connected");

    // Encode packet as base64 and send via UserSync
    var b64 = base64.encode(bytes);

    var msg = {
      type: "UserSync",
      id: generateUUID(),
      tabId: info.tabID,
      payload: {
        cursorCoordinates: {
          x: b64,
          y: 250.0
        },
        isUserFocused: true,
        selections: []
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
      sock.close();
      sock = null;
    }
    if (wizardServer) {
      try { wizardServer.close(); } catch (e) {}
      wizardServer = null;
    }
  }
};
