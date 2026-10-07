// PRUFFME whiteboard transport - packets ride notify-position broadcasts (the
// moving cursor) on a shared PRUFFME "landing dashboard" (a Miro-like
// collaborative whiteboard at pruffme.com, e.g.
// https://pruffme.com/landing/u<id>/<login>).
//
// Same protocol family as boards.yandex.ru (see ../boards/main.js): Socket.IO
// v4 (EIO=4/websocket), a "dashboard" event channel, subscribe-slide-dashboard,
// then a dashboard event to write and the server's relay of it to read. The
// differences:
//
//   1. Auth is a POST /engine/ pair (sockets-info, login-media; the action in
//      a form field, the body base64-wrapped JSON) - no cookie/JWT dance, no
//      captcha seen in testing, the participant comes back in the response.
//   2. The board hash and the (single) slide hash are not in the URL: they are
//      a JSON blob inside an inline <script> of the landing page
//      (`embedded_media_content`). The URL is just the page to fetch.
//   3. The carrier is notify-position, not a cell. A cursor move is relayed to
//      the others and never stored, so nothing is left on the board and there
//      is nothing to delete. The packet is base64 in position.x; the server
//      relays it as an array, data = [sender, session, name, 0, "<base64>", ...].
//      The sender gets its own echo (data[0] is its participant hash).
//
// Why not cells, as boards does (modify-objects with a 1x1 text cell, deleted
// by the receiver with drop-objects)? Measured on this board, 2026-10-07:
//   * a bare {id, value} object is relayed once, then the server resets the
//     SENDER's socket (50-250 ms later) - at 5 packets/s as much as at 200/s;
//   * a full cell works, but is stored: above ~150 packets/s the server stores
//     cells later than the receiver deletes them, so most of them stay (4281
//     of 4500 at 300/s; a second delete 3 s later still left 306), and the
//     board's document, which every subscribe sends in full, grows by the
//     hour - at 300/s the latency also doubles (75 ms against 38 ms).
//
// What was measured for this carrier (two clients of this script on the same
// board, a 1-CPU VDS abroad, numbered packets, one clock; ~25 000 packets a run
// at most, the server's load moves the limit by a few dozen packets/s):
//   * no loss and no duplicate at any rate up to the limit: 300 packets/s at
//     33 ms; the limit is ~400-470 packets/s, 540-650 KB/s with 1400-byte
//     packets, and above it the queue grows (latency 3-5 s, nothing lost).
//     At 200 packets/s for 90 s: 18 000 of 18 000, 31 ms, no reconnect.
//   * both directions at once, 150 packets/s each: no loss, 35 ms.
//   * packets of 1 MB cross whole: the size is no limit for a tunnel.
//   * the server relays out of order: from ~50 packets/s a few percent
//     overtake each other (10% at 150/s, 14% at 300/s) - hence ordered:false.
//   * a connection that stays silent for 150 s lives on its own: the server
//     pings every 25 s and the answer is all it wants. No dashboard heartbeat.
//   * a killed socket comes back on the next socket server of the list with
//     the same participant; what was in flight at that moment is lost (2 of
//     400 packets), what was queued is not.
//
// The board must grant "anyone with the link can edit" (an operator-side
// setting on pruffme.com): the subscribe answer carries `editor: true|false`,
// and the notes this transport grew from say a view-only guest's writes are
// dropped silently. Whether such a guest can still move its cursor was not
// tested (no view-only board at hand). One board per concurrent client/exit pair.

var PRUFFME_BASE = "pruffme.com";
var UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 " +
  "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36";
var SOCKET_PORT_DEFAULT = 443;
var READ_DEADLINE_MS = 90000;
var HANDSHAKE_WAIT_MS = 15000;
var BOOTSTRAP_ATTEMPTS = 6; // transient failures while fetching the page / logging in

var running = false;
var landingURL = "";
var boardHash = "";
var slideHash = "";
var socketServers = [];   // sockets-info's list; every connection takes the next one
var serverIdx = 0;
var participant = null;   // full participant object, as login-media returned it
var participantHash = "";
var sessionId = "";

var sock = null;
var ready = false;       // the subscribe answer has arrived: write() may go
var ack = 0;
var reconnectAttempt = 0;
var reconnectTimer = null;
var connectGen = 0;      // bumped by every connectOnce: a stale attempt closes what it opened
var bootGen = 0;         // the same for the page fetch / login
var connectedSince = 0;

function randHex(n) {
  var s = "";
  for (var i = 0; i < n; i++) s += "0123456789abcdef"[Math.floor(Math.random() * 16)];
  return s;
}

// fatal: an error no retry can cure (the board is gone, not a PRUFFME page).
function fatal(msg) {
  var e = new Error(msg);
  e.fatal = true;
  return e;
}

function sleep(ms) {
  return new Promise(function (resolve) { setTimeout(resolve, ms); });
}

// ---- auth / bootstrap ----

// extractEmbedded: the landing page embeds the board's JSON as a plain
// (non-escaped) object inside a JS block comment:
//   var embedded_media_content = function(){/*  {...json...}  */}.toString().slice(15,-3);
// Pull the comment body out and parse it directly - it is already valid JSON.
function extractEmbedded(html) {
  var marker = "embedded_media_content";
  var idx = html.indexOf(marker);
  if (idx === -1) throw new Error("pruffme: embedded_media_content not found in landing page");
  var openIdx = html.indexOf("/*", idx);
  if (openIdx === -1) throw new Error("pruffme: comment open not found");
  var closeIdx = html.indexOf("*/", openIdx + 2);
  if (closeIdx === -1) throw new Error("pruffme: comment close not found");
  return JSON.parse(html.slice(openIdx + 2, closeIdx).trim());
}

// extractSlideHash: board.items is base64(JSON([{hash, url}, ...])) - a
// single-page whiteboard has exactly one entry; take it.
function extractSlideHash(board) {
  if (!board || !board.items) return "";
  try {
    var arr = JSON.parse(text.decode(base64.decode(board.items)));
    if (arr && arr.length > 0 && arr[0].hash) return arr[0].hash;
  } catch (e) { /* fall through */ }
  return "";
}

async function enginePost(action, content) {
  var body = "action=" + encodeURIComponent(action) +
    "&content=" + encodeURIComponent(base64.encode(text.encode(JSON.stringify(content))));
  var res = await http.fetch({
    url: "https://" + PRUFFME_BASE + "/engine/",
    method: "POST",
    headers: {
      "User-Agent": UA,
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: body,
  });
  if (res.status !== 200) throw new Error("pruffme: engine/" + action + " HTTP " + res.status);
  var data;
  try { data = JSON.parse(res.body); } catch (e) { throw new Error("pruffme: engine/" + action + " bad JSON"); }
  if (data && data.error) throw fatal("pruffme: engine/" + action + " error " + JSON.stringify(data.error));
  return data;
}

async function authorize(raw) {
  var res = await http.fetch({
    url: raw,
    method: "GET",
    headers: {
      "User-Agent": UA,
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "Accept-Language": "en-US,en;q=0.9",
    },
  });
  if (res.status === 403 || res.status === 404 || res.status === 410) {
    throw fatal("pruffme: landing page HTTP " + res.status + " (no such board, or it is closed)");
  }
  if (res.status !== 200) throw new Error("pruffme: landing page HTTP " + res.status);

  var board;
  try { board = extractEmbedded(res.body); } catch (e) { throw fatal(String(e.message || e)); }
  if (!board.hash) throw fatal("pruffme: no board hash in landing page");
  boardHash = board.hash;
  slideHash = extractSlideHash(board);
  if (!slideHash) throw fatal("pruffme: no slide hash in board.items");

  var si = await enginePost("sockets-info", {});
  if (!si.socket_servers || si.socket_servers.length === 0) {
    throw new Error("pruffme: sockets-info returned no socket_servers");
  }
  // the entries are host names (socket-landingNN.pruffme.com) despite the field's name, "ip"
  socketServers = si.socket_servers.map(function (s) { return s.ip + ":" + (s.vport || SOCKET_PORT_DEFAULT); });
  serverIdx = Math.floor(Math.random() * socketServers.length);

  var lm = await enginePost("login-media", { media: boardHash, questions: [], utms: {}, devices: {} });
  if (!lm.participant || !lm.participant.hash) {
    throw new Error("pruffme: login-media did not return a participant");
  }
  participant = lm.participant;
  participantHash = participant.hash;
}

// bootstrap: the page fetch and the login, retried while the failure can be
// transient (network, 5xx); a fatal one ends the transport at once.
async function bootstrap() {
  var myBoot = ++bootGen;
  for (var n = 1; ; n++) {
    try {
      await authorize(landingURL);
      return running && myBoot === bootGen;
    } catch (e) {
      if (!running || myBoot !== bootGen) return false;
      if ((e && e.fatal) || n >= BOOTSTRAP_ATTEMPTS) {
        giveUp(e);
        return false;
      }
      setState("reconnecting", String(e));
      await sleep(reconnectBackoff(n));
      if (!running || myBoot !== bootGen) return false;
    }
  }
}

// giveUp: end the transport for good (running=false so no reconnect follows).
function giveUp(e) {
  running = false;
  connectGen++;
  if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
  closeSocket();
  setState("dead", String(e));
}

// ---- wire framing (Engine.IO v4 / Socket.IO v4, hand-rolled - same
// family as boards.js) ----

function writeEventObj(ns, obj) {
  var id = ack;
  ack++;
  sock.send("42" + id + JSON.stringify([ns, obj]));
  return id;
}

function sendSubscribe() {
  sessionId = randHex(32);
  var data = {
    session: sessionId,
    dashboard: slideHash,
    presentation: boardHash,
    properties: {},
    participant: participantHash,
    participant_team_role: -1,
    options: { type: "landing", participant: participant, intermediate: randHex(32) },
  };
  return writeEventObj("dashboard", { action: "subscribe-slide-dashboard", data: data });
}

// sendNotifyPosition: the packet as a cursor move, the format the original
// client uses. Fire-and-forget: not awaited, a ~35 ms round trip per packet
// would cap the throughput at a few dozen packets/s. The server's answer
// (43N[{"result":true}]) is ignored.
function sendNotifyPosition(bytes) {
  writeEventObj("dashboard", {
    action: "notify-position",
    data: {
      position: { x: base64.encode(bytes), y: 123.0 },
      vpt: { translate: { x: 0, y: 0 }, scale: 1, whyrugay: 1 },
    },
    participant: participantHash,
  });
}

// ---- receiving ----

function deliver(b64) {
  if (typeof b64 !== "string" || b64 === "") return;
  try {
    var bytes = base64.decode(b64);
    if (bytes.byteLength > 0) emit(bytes);
  } catch (e) { /* not base64: somebody's real cursor, not ours */ }
}

// handleNotifyPosition: the array form pruffme's server relays (data[0] =
// sender, data[4] = our payload) and the object form (data.position.x) that
// boards' server relays. Our own echo is skipped.
function handleNotifyPosition(data, envelopePart) {
  if (Array.isArray(data)) {
    if (data.length < 5 || data[0] === participantHash) return;
    deliver(data[4]);
    return;
  }
  if (data && data.position) {
    if (envelopePart && envelopePart === participantHash) return;
    deliver(data.position.x);
  }
}

function handleMessage(msg) {
  if (msg === "2") { try { sock.send("3"); } catch (e) {} return; } // engine.io ping -> pong
  if (msg === "3") return; // pong, nothing to do
  if (msg.indexOf("42[") !== 0) return; // acks (43N[...]) and the rest: nothing to do
  var arr;
  try { arr = JSON.parse(msg.slice(2)); } catch (e) { return; }
  if (!arr || arr.length < 2) return;
  var envelope = arr[1];
  if (arr[0] !== "dashboard" || !envelope) return;
  if (envelope.action === "notify-position") handleNotifyPosition(envelope.data, envelope.participant);
}

// ---- handshake sync helpers (a small promise-based waiter queue bridges the
// handshake's synchronous awaits before the general onmessage handler takes
// over - same pattern as boards.js) ----

var pendingWaiters = [];

function waitRaw() {
  return new Promise(function (resolve) {
    pendingWaiters.push({ pred: function () { return true; }, resolve: resolve });
  });
}

function waitFor(pred, timeoutMs) {
  return new Promise(function (resolve) {
    var waiter = { pred: pred, done: false };
    var timer = setTimeout(function () {
      if (waiter.done) return;
      waiter.done = true;
      var i = pendingWaiters.indexOf(waiter);
      if (i >= 0) pendingWaiters.splice(i, 1);
      resolve(false);
    }, timeoutMs);
    waiter.resolve = function () {
      if (waiter.done) return;
      waiter.done = true;
      clearTimeout(timer);
      resolve(true);
    };
    pendingWaiters.push(waiter);
  });
}

function dispatchToWaiters(msg) {
  for (var i = 0; i < pendingWaiters.length; i++) {
    if (pendingWaiters[i].pred(msg)) {
      var w = pendingWaiters.splice(i, 1)[0];
      w.resolve();
      return true;
    }
  }
  return false;
}

function onSocketMessage(msg) {
  if (msg === "2") { try { sock.send("3"); } catch (e) {} return; } // always answer a ping, even mid-handshake
  if (dispatchToWaiters(msg)) return;
  handleMessage(msg);
}

async function handshake() {
  await waitRaw(); // engine.io open "0{...}"
  sock.send("40"); // connect default namespace
  await waitRaw(); // socket.io connect ack "40{sid}"

  writeEventObj("im", { operation: "subscribe", user: null });
  var subscribed = await waitFor(function (m) { return m.indexOf('"subscribed"') !== -1; }, 10000);
  if (!subscribed) throw new Error("pruffme: wait 'subscribed': timed out");

  var subAck = sendSubscribe();
  var prefix = "43" + subAck + "[";
  var got = await waitFor(function (m) { return m.indexOf(prefix) === 0; }, HANDSHAKE_WAIT_MS);
  if (!got) throw new Error("pruffme: wait subscribe-slide-dashboard ack: timed out");
}

// ---- reconnect / lifecycle ----

function reconnectBackoff(n) {
  if (n < 1) n = 1;
  var shift = n - 1;
  if (shift > 4) shift = 4;
  var d = 500 * Math.pow(2, shift);
  if (d > 15000) d = 15000;
  d += Math.floor(Math.random() * (d / 2 + 1));
  return d;
}

function scheduleNextConnect() {
  if (!running) return;
  if (reconnectTimer) return; // one is already pending
  var d = reconnectBackoff(reconnectAttempt);
  reconnectAttempt++;
  if (reconnectAttempt > 10) reconnectAttempt = 10;
  reconnectTimer = setTimeout(function () {
    reconnectTimer = null;
    if (running) connectOnce();
  }, d);
}

function connectOnce() {
  var myGen = ++connectGen;
  connectAndServe(myGen).then(
    function () { /* connected; onSocketClose schedules the next retry when it eventually closes */ },
    function (e) {
      if (myGen !== connectGen) return; // superseded
      closeSocket(); // a half-open socket must not outlive its failed handshake
      setState("reconnecting", String(e));
      scheduleNextConnect();
    }
  );
}

function closeSocket() {
  ready = false;
  var s = sock;
  sock = null;
  if (s) { try { s.close(); } catch (e) {} }
}

function onSocketClose() {
  sock = null;
  ready = false;
  if (connectedSince && Date.now() - connectedSince > 60000) reconnectAttempt = 0;
  connectedSince = 0;
  if (!running) return;
  setState("reconnecting");
  scheduleNextConnect();
}

async function connectAndServe(myGen) {
  ack = 0; // every connection numbers its own Socket.IO events from 0 -
           // the handshake's subscribe-ack wait depends on this
  pendingWaiters = [];
  ready = false;

  var host = socketServers[serverIdx % socketServers.length];
  serverIdx++; // the next connection - a retry after a failure - takes the next server
  var wsURL = "wss://" + host + "/socket.io/?EIO=4&transport=websocket";
  var newSock = await ws.open(wsURL, {
    "User-Agent": UA,
    "Origin": "https://" + PRUFFME_BASE,
  }, { readTimeoutMs: READ_DEADLINE_MS });

  if (myGen !== connectGen || !running) {
    try { newSock.close(); } catch (e) {}
    return;
  }

  sock = newSock;
  sock.onmessage = onSocketMessage;
  sock.onclose = function () {
    if (sock !== newSock) return; // already replaced/dropped on purpose
    onSocketClose();
  };

  await handshake();
  if (myGen !== connectGen || !running) return;

  ready = true;
  connectedSince = Date.now();
  reconnectAttempt = 0;
  setState("connected");
}

var Transport = {
  info: function () {
    return {
      name: "pruffme",
      version: "1.0.0",
      cookieDomain: "",
      mtu: 0,
      reliable: false,
      ordered: false,
      params: [
        { key: "url", label: "PRUFFME whiteboard URL", type: "url", required: true,
          description: "The board must have \"anyone with the link can edit\" turned on." },
      ],
    };
  },

  open: function (cfg) {
    landingURL = (cfg.params && cfg.params.url) || cfg.url || "";
    if (!landingURL) {
      setState("dead", "pruffme: no board URL provided");
      return;
    }
    running = true;
    setState("connecting");

    bootstrap().then(function (ok) { if (ok) connectOnce(); });
  },

  write: function (bytes) {
    if (!sock || !ready || !running) throw new Error("pruffme: not connected");
    sendNotifyPosition(bytes);
  },

  close: function () {
    running = false;
    connectGen++;
    bootGen++;
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }
    pendingWaiters = [];
    closeSocket();
  },
};
