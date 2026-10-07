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

  var html = res.body;

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

  return {
    token: token,
    payload: payload,
    appURL: appURL,
    wsHost: wsHost,
    tabID: generateUUID(),
    myUserID: payload.user_id || ""
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

    var wsURL = "wss://" + info.wsHost + "/api/v1/flip/ws?token=" + encodeURIComponent(info.token);

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

// Transport interface
var Transport = {
  info: function() {
    return {
      name: "bitrix",
      version: "1.0.0",
      cookieDomain: "",
      mtu: 0,
      reliable: false,
      ordered: false,
      params: [
        { key: "url", label: "Bitrix24 Board URL", type: "url", required: true }
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
  }
};
