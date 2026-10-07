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

    var wsURL = baseInfo.wsDomain + "/ws/1?clientUID=" + baseInfo.clientUID + "&locale=en";
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

    // Send init message. Wire names mirror native initMessage struct tags in
    // mtslink.go: boardUID is `board_uid` (snake), everything else camelCase.
    // The server closes the socket (1006) if `board_uid` is wrong-case.
    var initMsg = {
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

// Transport interface
var Transport = {
  info: function() {
    return {
      name: "mtslink",
      version: "1.0.0",
      cookieDomain: "",
      mtu: 0,
      reliable: false,
      ordered: false,
      params: [
        { key: "url", label: "MTS-Link Board URL", type: "url", required: true }
      ]
    };
  },

  open: async function(cfg) {
    boardURL = cfg.url || cfg.params.url;
    if (!boardURL) {
      setState("dead", "no board URL provided");
      return;
    }

    running = true;
    connectToBoard(0);
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
  }
};
