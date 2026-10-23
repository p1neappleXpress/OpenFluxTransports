// MAX P2P DataChannel transport via TURN relay.
//
// 2026-10-18 PROVEN: P2P DC works through MAX TURN (155.212.x:19302,
// whitelisted on RU carriers). SFU is media-only — no SCTP/DC, and
// transmit-data does NOT forward between peers. Signaling goes through
// MSG_SEND (opcode 64) with emoji-encrypted SDP.
//
// Flow: login → call → TURN creds → gather relay candidates → SDP WITH the candidates via
// MSG_SEND → P2P DC. The host's createOffer()/createAnswer() hand back the SDP before ICE
// gathering, so the candidates are put into it (lib/sdp.js) before it goes into the chat:
// there is no trickle channel, one message per description.
//
// Two long-lived loops keep it up: the MAX signaling session (log in again when its socket
// dies, ping to notice that it did) and, on the client, the call (dial again when the data
// channel never opens or goes down). The node only waits: a new incoming call replaces the
// old one. Every step is traced to the console (ids, states, sizes; never secrets).
// Params: token, uid (callee userId), chatId, exit (receiver role).

var maxclient = require("../../lib/maxclient.js");
var sdputil = require("../../lib/sdp.js");
var SETUP_HTML = require("./setup_html.js");

// ── Timings ──
var KEEPALIVE_MS = 15000;           // ping the signaling socket this often ...
var KEEPALIVE_TIMEOUT_MS = 10000;   // ... and give up on a ping after this
var SIGNALING_READ_TIMEOUT_MS = 90000; // the server answers every ping: this much silence = a dead path
var CALL_READ_TIMEOUT_MS = 120000;  // the call socket is pinged by the server (we pong)
var GATHER_TIMEOUT_MS = 8000;       // relay candidates should be there well before this
var ACCEPT_WAIT_MS = 10000;         // the offer goes out on "accepted-call", or after this
var ACCEPT_FALLBACK_MS = 1500;      // the node accepts when the call socket told its participant id, or after this
var ACCEPT_DEADLINE_MS = 20000;     // the node accepts within a second; the client gives up on a call it does not
var CONNECT_DEADLINE_MS = 45000;    // a call with no open data channel by then is dropped
var OFFER_RESEND_MS = [7000, 16000];// the offer again, when no answer has come
var DISCONNECT_GRACE_MS = 8000;     // "disconnected" that long = lost
var STALE = { stale: true };        // thrown to leave a promise chain whose call/session is over

// ── The data channel's datagrams ──
//
// A message of the data channel is one datagram on the wire only if it fits one SCTP chunk. A longer one
// is cut into several datagrams sent back to back, and the relay in front of a mobile network keeps the
// first of such a burst and drops most of the rest: the whole message is lost with any one of them.
// So what the tunnel hands to write() is sized to one chunk.
//   SCTP_MTU      the SCTP packet; plus 69 bytes of DTLS, TURN, UDP and IP it is the datagram, 1429 here,
//                 which the phone's mobile network (1430) moves without fragmenting
//   a chunk       holds SCTP_MTU - 32 (the common header and an I-DATA header)
//   CARRIER_MTU   the IP packet the tunnel runs over this channel; the tunnel puts 115 bytes around it
//                 (78 envelope, 4 batch framing, 33 encryption): 1200 + 115 = 1315 <= 1328
//   BATCH_BYTES   how much the tunnel may coalesce into one message: a chunk less the 35 bytes of
//                 encryption and batch header, and a packet that does not fit starts the next message
var SCTP_MTU = 1360;
var CARRIER_MTU = 1200;
var BATCH_BYTES = SCTP_MTU - 32 - 35;

// ── The leg to the TURN server ──
//
// Each end reaches its relay over UDP or over TCP (a TURN server answers TCP on the port it answers UDP on).
// Measured both ways on the phone, Wi-Fi and mobile network alike: the speed is the same, so both use UDP.
// The table is there for a network that lets only TCP through.
var TURN_TRANSPORT = { caller: "udp", receiver: "udp" };
var NODE_DIRECT = true;

// ── Lanes ──
//
// One relay allocation moves about 10 KB/s here, whatever the access network and whatever the datagram
// size: the same STUN requests to the same server go through at 80 KB/s, so the limit is the relay's, per
// allocation. A call may hold more than one, but a user's allocations at the call's two TURN servers stop at
// about twenty (the next one is refused). The node, which is reached directly and needs no relay of its own,
// hands its own call credentials over, and the lanes beyond the twentieth allocate with those. Besides the
// call's own PeerConnection (lane 0) the client
// opens LANES - 1 more, each with a TURN allocation and a data channel of its own, and the tunnel's
// packets are dealt out over every channel that is open. A lane is a help only: one that fails is dropped
// and the call goes on with the rest.
var LANES = NODE_DIRECT ? 39 : 19;
var LANES_OWN = 19;                     // lanes below this allocate with the call's own credentials, the rest with the node's
var LANE_PEER_WAIT_MS = 10000;          // how long the client waits for the node to hand its credentials over
var LANE_OPEN_GAP_MS = 300;             // the client starts its lanes this far apart
var LANE_OFFER_RESEND_MS = [7000, 16000]; // a lane's offer again, when no answer has come
var LANE_CONNECT_DEADLINE_MS = 30000;   // a lane with no open channel by then is dropped
var LANE_RETRY_MS = 15000;              // and the client tries it again after this, ...
var LANE_TRIES = 3;                     // ... this many times in all

// ── Dealing the packets over the lanes ──
//
// A lane's relay moves about 10.4 KB/s (wire bytes, headers included) and drops what it is offered above that
// in bursts: the TCP inside the tunnel meets heavy loss, takes it for congestion and sends far less than the
// lanes could carry. So the tunnel's packets wait in ONE queue and leave for whichever lane has earned the
// bytes: each lane is let LANE_RATE a second, the queue is drained in order (no lane has a longer backlog than
// another, so nothing arrives out of order), and what the queue cannot hold is dropped, the signal the TCP
// inside reacts to. A packet as small as an ACK jumps the queue.
var LANE_RATE = 9500;             // wire bytes per second a lane is dealt (0 = no pacing: packets go out at once)
var laneRate = LANE_RATE;         // the node takes the client's value (a cfg message over the call's channel)
var WIRE_OVERHEAD = 100;          // what SCTP, DTLS, TURN, UDP and IP put around a record
var SMALL_RECORD = 200;           // a record this small (an ACK) jumps the queue
var QUEUE_MS = 400;               // the queue holds this much of the lanes' combined rate
var PUMP_MS = 4;
var CREDIT_MS = 150;              // a lane saves up this much credit: a packet, never a burst

var T0 = Date.now();
var running = false;
var role = "caller";
var calleeID = 0;
var chatID = 0;
var maxToken = "";

// The MAX signaling session (one socket, replaced when it dies).
var client = null;
var deviceId = "";                  // kept across logins: every new id is a new session in the account's list
var sessionGen = 0;
var sessionReady = false;
var sessionUpAt = 0;
var sessionFails = 0;
var sessionTimer = null;
var keepTimer = null;
var keepFails = 0;
var myUid = 0;
var loginChats = [];
var seenOps = {};

// The call (one at a time; callGen moves on with every reset, so a late callback of an old one is stale).
var callGen = 0;
var callNo = 0;                     // the number a call goes by in the log (callGen moves on at every reset)
var callTimer = null;
var callFails = 0;
var connectTimer = null;
var acceptDeadlineTimer = null;
var acceptWaitTimer = null;
var disconnectTimer = null;
var offerTimers = [];
var callConn = null;
var callSeq = 1;
var localID = 0;
var remoteID = 0;
var conversationID = "";
var callMsgsLogged = 0;
var callAccepted = false;
var acceptSent = false;
var offerSDP = "";
var offerSent = false;
var pendingOffer = null;            // node: an offer that came before its PeerConnection
var lastOfferSDP = "";
var lastAnswerSDP = "";
var answerResends = 0;
var replyChatID = 0;                // node: the chat the offer came in, where the answer goes
var haveRemoteDesc = false;
var turnCreds = null;
var gather = null;

var pc = null;
var dc = null;
var dcOpen = false;
var dcOpenAt = 0;
var lanes = [];                     // lanes[k], k >= 1: the extra PeerConnections (see Lanes)
var laneTimers = [];                // the client's pending lane starts and retries
var laneTries = [];
var sendDCs = [];                   // the open data channels (the call's own first): what write() deals over
var peerTurn = null;                // the node's TURN credentials, handed over once the call's channel is open (client)
var sendRR = 0;
var txq = [];                       // the records that wait for a lane, oldest first: {b, cost}
var txQueued = 0;                   // their wire bytes
var txDropped = 0;
var pumpTimer = null;
var pumpAt = 0;                     // when the lanes' credit was last topped up
var bytesIn = 0;
var bytesOut = 0;
var writeErrors = 0;
var statusTimer = null;

// ── Emoji SDP cipher ──
var B64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";
var EMOJI = "\u{1F300}\u{1F301}\u{1F302}\u{1F303}\u{1F304}\u{1F305}\u{1F306}\u{1F307}\u{1F308}\u{1F309}\u{1F30A}\u{1F30B}\u{1F30C}\u{1F30D}\u{1F30E}\u{1F30F}\u{1F310}\u{1F311}\u{1F312}\u{1F313}\u{1F314}\u{1F315}\u{1F316}\u{1F317}\u{1F318}\u{1F319}\u{1F31A}\u{1F31B}\u{1F31C}\u{1F31D}\u{1F31E}\u{1F31F}\u{1F320}\u{1F321}\u{1F322}\u{1F323}\u{1F324}\u{1F325}\u{1F326}\u{1F327}\u{1F328}\u{1F329}\u{1F32A}\u{1F32B}\u{1F32C}\u{1F32D}\u{1F32E}\u{1F32F}\u{1F330}\u{1F331}\u{1F332}\u{1F333}\u{1F334}\u{1F335}\u{1F336}\u{1F337}\u{1F338}\u{1F339}\u{1F33A}\u{1F33B}\u{1F33C}\u{1F33D}\u{1F33E}\u{1F33F}\u{1F340}";
var EMOJI_LIST = [];
for (var ci = 0; ci < EMOJI.length; ci++) {
  var cp = EMOJI.codePointAt(ci);
  EMOJI_LIST.push(String.fromCodePoint(cp));
  if (cp > 0xFFFF) ci++;
}
var C2E = {}, E2C = {};
for (var bi = 0; bi < B64.length; bi++) {
  C2E[B64[bi]] = EMOJI_LIST[bi];
  E2C[EMOJI_LIST[bi]] = B64[bi];
}
var SDP_PREFIX = "\u{1F511}"; // 🔑

function sdpKey() {
  return crypto.sha256(text.encode("max-ofx-sdp-key-2026"));
}

function xorBytes(data, key) {
  var keyBuf = new Uint8Array(key);
  var dataBuf = new Uint8Array(data);
  var out = new Uint8Array(dataBuf.length);
  for (var xi = 0; xi < dataBuf.length; xi++) {
    out[xi] = dataBuf[xi] ^ keyBuf[xi % keyBuf.length];
  }
  return out.buffer;
}

// convId names the call the description belongs to (c): the other end drops one meant for a call it is
// not in. A peer that does not know the field ignores it, and a message without it fits any call.
function encodeSDP(sdpType, sdp, convId) {
  var json = JSON.stringify({ t: sdpType, s: sdp, c: convId || "" });
  var compressed = gzip.compress(text.encode(json));
  var encrypted = xorBytes(compressed, sdpKey());
  var b64str = base64.encode(encrypted);
  var out = SDP_PREFIX;
  for (var ei = 0; ei < b64str.length; ei++) {
    out += C2E[b64str[ei]] || b64str[ei];
  }
  return out;
}

function decodeSDP(emojiText) {
  if (emojiText.indexOf(SDP_PREFIX) === 0) {
    emojiText = emojiText.substring(SDP_PREFIX.length);
  }
  var b64str = "";
  for (var di = 0; di < emojiText.length; di++) {
    var ch = emojiText.codePointAt(di);
    var sym = String.fromCodePoint(ch);
    b64str += E2C[sym] || sym;
    if (ch > 0xFFFF) di++;
  }
  var encrypted = base64.decode(b64str);
  var compressed = xorBytes(encrypted, sdpKey());
  var json = text.decode(gzip.decompress(compressed));
  var obj = JSON.parse(json);
  return { type: obj.t, sdp: obj.s, conv: obj.c || "" };
}


// ── Setup wizard backend ──

var wizardServer = null;

// startWizard serves the setup page and asks the app to show it. values are what is known: with the
// node's user id (values.uid) the wizard only signs this side in (a second client of an existing
// node); without, it signs both accounts in and hands back {client, node}.
function startWizard(values) {
  if (wizardServer) return;
  var init = { nodeUid: String((values && values.uid) || "") };
  var page = SETUP_HTML.replace(
    '<script id="init" type="application/json">{}</script>',
    '<script id="init" type="application/json">' + JSON.stringify(init).replace(/</g, "\\u003c") + "</script>"
  );
  wizardServer = httpserver.listen(function (req) {
    if (req.path === "/" || req.path === "") {
      return { status: 200, headers: { "Content-Type": "text/html; charset=utf-8" }, body: page };
    }
    if (req.path === "/api/verify" && req.method === "POST") {
      return verifyToken(req);
    }
    if (req.path === "/api/chat" && req.method === "POST") {
      return verifyChat(req);
    }
    return { status: 404, body: "not found" };
  });

  raise("needsSetup", {
    url: "http://" + wizardServer.addr + "/",
    reason: init.nodeUid ? "Вход в MAX" : "Подключение через MAX",
  });
}

function jsonResponse(obj) {
  return { status: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(obj) };
}

// accountOf reads who a LOGIN reply is about. The web client takes payload.profile.contact
// (id, phone, names[0].firstName / lastName); hintUid is the id the sign-in window saw.
function accountOf(payload, hintUid) {
  var p = payload || {};
  var profile = p.profile || {};
  var contact = profile.contact || p.contact || {};
  var n0 = (contact.names && contact.names[0]) || {};
  var name = ((n0.firstName || contact.firstName || n0.name || "") + " " + (n0.lastName || contact.lastName || "")).trim();
  var phone = contact.phone ? String(contact.phone) : "";
  if (phone && phone.charAt(0) !== "+") phone = "+" + phone;
  return {
    uid: contact.id || profile.id || hintUid || 0,
    name: name,
    phone: phone,
    // Key names only, no values: what to look at when a field comes back empty.
    shape: Object.keys(p).join(",") + " | profile: " + Object.keys(profile).join(",") + " | contact: " + Object.keys(contact).join(","),
  };
}

function verifyToken(req) {
  var body;
  try { body = JSON.parse(text.decode(req.body)); } catch (e) {
    return { status: 400, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "bad json" }) };
  }
  var token = body.token || "";
  if (!token) return jsonResponse({ ok: false, error: "пустой токен" });

  return maxclient.connect().then(function (c) {
    return maxclient.loginByToken(c, token).then(function (resp) {
      var a = accountOf(resp.payload, parseInt(body.viewerId, 10) || 0);
      try { c.sock.close(); } catch (e) {}
      return jsonResponse({ ok: true, uid: a.uid, name: a.name, phone: a.phone, shape: a.shape });
    });
  }).catch(function (e) {
    return jsonResponse({ ok: false, error: String(e) });
  });
}

// xorIds: a private chat's id is the two user ids XORed. The 32-bit ^ would cut the high bits off.
function xorIds(a, b) {
  var M = 4294967296;
  var hi = (Math.floor(a / M) ^ Math.floor(b / M)) >>> 0;
  var lo = ((a % M) ^ (b % M)) >>> 0;
  return hi * M + lo;
}

// dialogCandidates lists the chat ids a message to nodeUid may go to, best first: a dialog the
// LOGIN reply already lists with that user in it, the XOR id, the bare user id.
function dialogCandidates(chats, clientUid, nodeUid) {
  var out = [];
  function add(id) { if (id && out.indexOf(id) === -1) out.push(id); }
  for (var i = 0; i < chats.length; i++) {
    var c = chats[i] || {};
    var parts = c.participants || {};
    var ids = c.memberIds || [];
    if (c.type === "DIALOG" && (parts[nodeUid] !== undefined || ids.indexOf(nodeUid) !== -1)) add(c.id);
  }
  if (clientUid && nodeUid) add(xorIds(clientUid, nodeUid));
  add(nodeUid);
  return out;
}

// acceptedChat is the chat check without the other account's token (a second client of an existing
// node): the first chat id the server takes a message for. Whether it arrives cannot be seen from here.
function acceptedChat(c, cands) {
  var tried = [];
  function attempt(i) {
    if (i >= cands.length) {
      return Promise.resolve({ ok: false, error: "MAX не принял сообщение ни в один чат (" + tried.join("; ") + ")" });
    }
    var cid = Math.floor(Math.random() * 9007199254740991) + 1;
    return c.invoke(64, { chatId: cands[i], message: { text: "ofx_" + Date.now(), cid: cid } }).then(function (r) {
      if ((r || {}).cmd === 3) {
        tried.push(cands[i] + ": " + (((r.payload || {}).message) || ((r.payload || {}).error) || "ошибка"));
        return attempt(i + 1);
      }
      return { ok: true, chatId: (r.payload || {}).chatId || cands[i], unconfirmed: true };
    });
  }
  return attempt(0);
}

function verifyChat(req) {
  var body;
  try { body = JSON.parse(text.decode(req.body)); } catch (e) {
    return { status: 400, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "bad json" }) };
  }

  var clientTok = body.clientToken;
  var nodeTok = body.nodeToken;
  var clientUid = parseInt(body.clientUid, 10) || 0;
  var nodeUid = parseInt(body.nodeUid, 10) || 0;
  var cA = null, cB = null;
  function closeBoth() {
    try { if (cA) cA.sock.close(); } catch (e) {}
    try { if (cB) cB.sock.close(); } catch (e) {}
  }

  return maxclient.connect().then(function (a) {
    cA = a;
    return maxclient.loginByToken(cA, clientTok);
  }).then(function (respA) {
    var cands = dialogCandidates((respA.payload || {}).chats || [], clientUid, nodeUid);
    if (!nodeTok) return acceptedChat(cA, cands);
    return maxclient.connect().then(function (b) {
      cB = b;
      return maxclient.loginByToken(cB, nodeTok);
    }).then(function () {
      var msgRx = false;
      var testText = "ofx_" + Date.now();
      cB.setOnEvent(function (pkt) {
        if (pkt.opcode === 128) {
          var msg = (pkt.payload || {}).message || {};
          if (msg.text === testText) msgRx = true;
        }
      });

      // The first id the server takes a message for is the chat.
      var tried = [];
      function attempt(i) {
        if (i >= cands.length) {
          return Promise.resolve({ ok: false, error: "MSG_SEND не принят ни для одного чата (" + tried.join("; ") + ")" });
        }
        var cid = Math.floor(Math.random() * 9007199254740991) + 1;
        return cA.invoke(64, { chatId: cands[i], message: { text: testText, cid: cid } }).then(function (r) {
          if ((r || {}).cmd === 3) {
            tried.push(cands[i] + ": " + (((r.payload || {}).message) || ((r.payload || {}).error) || "ошибка"));
            return attempt(i + 1);
          }
          var serverChatId = (r.payload || {}).chatId || cands[i];
          return new Promise(function (resolve) {
            var checks = 0;
            var timer = setInterval(function () {
              checks++;
              if (msgRx || checks > 20) {
                clearInterval(timer);
                resolve(msgRx ? { ok: true, chatId: serverChatId } : { ok: false, error: "сообщение не доставлено за 10 секунд" });
              }
            }, 500);
          });
        });
      }
      return attempt(0);
    });
  }).then(function (res) {
    closeBoth();
    return jsonResponse(res);
  }).catch(function (e) {
    closeBoth();
    return jsonResponse({ ok: false, error: String(e) });
  });
}


// ── Tracing ──
//
// Lines go to the script's console (journald on a node, GoLog in logcat on Android). They carry
// ids, states and sizes only: never a token, an endpoint (its query holds the call's token),
// TURN credentials or SDP.

function trace(msg) {
  console.log("%s", "[oneme-webrtc " + (role === "receiver" ? "node" : "client") + " +" + ((Date.now() - T0) / 1000).toFixed(1) + "s] " + msg);
}

function short(id) {
  id = String(id || "");
  return id.length > 8 ? id.slice(0, 8) : id;
}

function hostOf(url) {
  var m = /^(wss?:\/\/[^\/?#]+)/.exec(String(url));
  return m ? m[1] : "?";
}

// shape names a message by its keys, never by what they hold.
function shape(data) {
  if (!data || typeof data !== "object") return String(data).slice(0, 20);
  var tag = data.notification || data.type || data.command || data.event || "";
  return (typeof tag === "string" ? tag : "") + "{" + Object.keys(data).join(",") + "}";
}

// ── Call + DC state ──

function clearOfferTimers() {
  for (var i = 0; i < offerTimers.length; i++) clearTimeout(offerTimers[i]);
  offerTimers = [];
}

function clearCallTimers() {
  if (connectTimer) { clearTimeout(connectTimer); connectTimer = null; }
  if (acceptDeadlineTimer) { clearTimeout(acceptDeadlineTimer); acceptDeadlineTimer = null; }
  if (acceptWaitTimer) { clearTimeout(acceptWaitTimer); acceptWaitTimer = null; }
  if (disconnectTimer) { clearTimeout(disconnectTimer); disconnectTimer = null; }
  clearOfferTimers();
}

// resetCallState drops everything a call holds. callGen moves on first, so whatever the old call's
// sockets and PeerConnection still deliver afterwards is recognised as stale and ignored.
function resetCallState() {
  callGen++;
  clearCallTimers();
  callSeq = 1;
  localID = 0;
  remoteID = 0;
  conversationID = "";
  callMsgsLogged = 0;
  callAccepted = false;
  acceptSent = false;
  offerSDP = "";
  offerSent = false;
  pendingOffer = null;
  lastOfferSDP = "";
  lastAnswerSDP = "";
  answerResends = 0;
  haveRemoteDesc = false;
  turnCreds = null;
  peerTurn = null;
  laneRate = LANE_RATE;
  gather = null;
  var oldPC = pc;
  var oldConn = callConn;
  pc = null;
  dc = null;
  dcOpen = false;
  dcOpenAt = 0;
  callConn = null;
  closeLanes();
  if (oldPC) { try { oldPC.close(); } catch (e) {} }
  if (oldConn) { try { oldConn.close(); } catch (e) {} }
}

// sendCallJSON writes one command to the call socket. plain leaves out the participant fields (the
// native client's accept-call carries none).
function sendCallJSON(obj, plain) {
  if (!callConn) return;
  obj.sequence = callSeq;
  if (!plain) {
    obj.participantId = localID;
    obj.participantType = "USER";
  }
  callSeq++;
  try {
    callConn.send(JSON.stringify(obj));
  } catch (e) {
    trace("call socket write failed: " + e);
  }
}

function sendAcceptCall() {
  if (acceptSent) return;
  acceptSent = true;
  trace("accept-call (participant " + localID + ")");
  sendCallJSON({
    command: "accept-call",
    mediaSettings: {
      isAudioEnabled: true, isVideoEnabled: false, isScreenSharingEnabled: false,
      isFastScreenSharingEnabled: false, isAudioSharingEnabled: false, isAnimojiEnabled: false,
    },
  }, true);
}

function findLocalID(data, wantCreator) {
  if (localID !== 0) return;
  var conv = data.conversation;
  if (!conv || !conv.participants) return;
  for (var i = 0; i < conv.participants.length; i++) {
    var p = conv.participants[i];
    var isCreator = (p.roles || []).indexOf("CREATOR") !== -1;
    if (isCreator === wantCreator) localID = p.id;
  }
}

// ── ICE gathering ──

// gatherCandidates resolves with the candidates found for a PeerConnection (the call's own, unless a
// lane's gathering record is given) once gathering is over (or, after GATHER_TIMEOUT_MS, with what there
// is); it fails when there are none, and then the TURN server cannot be reached.
function gatherCandidates(lane) {
  var g = lane || gather;
  return new Promise(function (resolve, reject) {
    if (!g) { reject(new Error("no PeerConnection")); return; }
    function finish(timedOut) {
      if (g.settled) return;
      g.settled = true;
      if (!g.cands.length) {
        reject(new Error(timedOut ? "ICE gathering timed out with no TURN candidate" : "ICE gathering ended with no candidate"));
        return;
      }
      if (timedOut) trace("ICE gathering still running after " + (GATHER_TIMEOUT_MS / 1000) + "s, going on with " + g.cands.length + " candidate(s)");
      resolve(g.cands.slice());
    }
    if (g.done) { finish(false); return; }
    g.waiters.push(function () { finish(false); });
    setTimeout(function () { finish(true); }, GATHER_TIMEOUT_MS);
  });
}

// turnKindOf names how a TURN url is reached: "tcp" for ?transport=tcp and for turns:, else "udp".
function turnKindOf(url) {
  url = String(url);
  return /^turns:/i.test(url) || /[?&]transport=tcp\b/i.test(url) ? "tcp" : "udp";
}

// turnURLsOf returns the urls reached the wanted way. A call lists UDP urls (and, on some calls, TCP ones
// as well): when it lists none of the wanted kind, the same servers are tried that way, which a TURN server
// answers on the port it answers UDP on.
function turnURLsOf(urls, want) {
  var out = [];
  for (var i = 0; i < urls.length; i++) {
    if (turnKindOf(urls[i]) === want) out.push(urls[i]);
  }
  if (!out.length && want === "tcp") {
    for (var j = 0; j < urls.length; j++) {
      var u = String(urls[j]).replace(/\?.*$/, "");
      if (/^turn:/i.test(u)) out.push(u + "?transport=tcp");
    }
  }
  return out.length ? out : urls;
}

// pcOptions is what every PeerConnection of a call is made with: the relay it allocates on, and what
// the core does about a path that drops in bursts (see tuneConnection in transport/script).
function pcOptions(urls, turn) {
  var direct = role === "receiver" && NODE_DIRECT;
  var o = {
    iceServers: direct ? [] : [{ urls: urls, username: turn.username, credential: turn.credential }],
    sctpMtu: SCTP_MTU,        // one packet of the tunnel is one datagram
    sctpRtoMaxMs: 3000,       // SCTP's retransmission timer backs off to 60 s by default: the channel is dead meanwhile
    sctpMinCwnd: 131072,      // the TCP inside has its own congestion control; SCTP's would only slow it a second time
    paceKBps: 400,            // datagrams leave one by one, not as the dozen SCTP releases on an acknowledgement
    paceQueue: 48,            // and what waits longer than that is dropped (the signal the TCP inside reacts to)
    dcBufKB: 96,              // as is what the channel itself holds
  };
  // The caller sits behind a whitelist and reaches the TURN servers only. A node with a public address need
  // not: its own address is its candidate, it sends straight to the caller's relayed address, and the data
  // crosses one allocation, not two. It allocates nothing then and lends its credentials (lendTurn).
  if (!direct) o.iceTransportPolicy = "relay";
  return o;
}

function createP2PPeerConnection(gen) {
  var turn = turnCreds && turnCreds.turn;
  var urls = turn && turn.urls;
  if (typeof urls === "string") urls = [urls];
  if (!urls || !urls.length) {
    callFailed(gen, "the call gave no TURN server (" + shape(turnCreds) + ")");
    return false;
  }
  var use = turnURLsOf(urls, TURN_TRANSPORT[role]);
  trace("call #" + callNo + ": " + urls.length + " TURN url(s) (" + urls.map(turnKindOf).join(", ") + "), using " + use.map(turnKindOf).join(", ") + ", first " + String(use[0]).replace(/\?.*$/, ""));

  var g = { cands: [], done: false, waiters: [], settled: false };
  gather = g;
  var p = webrtc.newPeerConnection(pcOptions(use, turn));
  pc = p;

  p.onicecandidate = function (c) {
    if (p !== pc) return;
    if (c && c.candidate) { g.cands.push(String(c.candidate)); return; }
    g.done = true;
    var waiting = g.waiters;
    g.waiters = [];
    for (var i = 0; i < waiting.length; i++) waiting[i]();
  };
  p.oniceconnectionstatechange = function (s) {
    if (p === pc) trace("call #" + callNo + ": ice " + s);
  };
  p.onconnectionstatechange = function (s) {
    if (p !== pc) return; // closing the old one ourselves is no news
    trace("call #" + callNo + ": peerconnection " + s);
    onPeerState(gen, s);
  };

  if (role === "caller") {
    // Unordered: a lost datagram must not hold the ones behind it (the TCP inside puts them in order itself).
    dc = p.createDataChannel("x", { ordered: false, maxRetransmits: 0 });
    wireDataChannel(gen, dc);
  } else {
    p.ondatachannel = function (remote) {
      if (p !== pc) return;
      dc = remote;
      wireDataChannel(gen, remote);
    };
  }
  return true;
}

function wireDataChannel(gen, d) {
  d.onopen = function () {
    if (d !== dc || gen !== callGen) return;
    dcOpen = true;
    dcOpenAt = Date.now();
    bytesIn = 0;
    bytesOut = 0;
    writeErrors = 0;
    if (connectTimer) { clearTimeout(connectTimer); connectTimer = null; }
    callFails = 0;
    trace("call #" + callNo + ": data channel open, connected");
    setState("connected");
    rebuildSendDCs();
    lendTurn(gen);
    sendCfg(gen);
    startLanes(gen);
  };
  d.onclose = function () {
    if (d !== dc || gen !== callGen) return;
    dcOpen = false;
    rebuildSendDCs();
    callFailed(gen, "data channel closed");
  };
  d.onmessage = function (bytes, isString) {
    if (d !== dc) return;
    if (isString) { onLaneSignal(gen, bytes); return; }
    bytesIn += bytes.byteLength || 0;
    emit(bytes);
  };
}

function onPeerState(gen, s) {
  if (gen !== callGen) return;
  if (s === "connected") {
    if (disconnectTimer) { clearTimeout(disconnectTimer); disconnectTimer = null; }
  } else if (s === "disconnected") {
    if (!disconnectTimer) {
      disconnectTimer = setTimeout(function () {
        disconnectTimer = null;
        callFailed(gen, "peerconnection disconnected for " + (DISCONNECT_GRACE_MS / 1000) + "s");
      }, DISCONNECT_GRACE_MS);
    }
  } else if (s === "failed" || s === "closed") {
    callFailed(gen, "peerconnection " + s);
  }
}

// ── MSG_SEND signaling (replaces transmit-data) ──

// sdpChatIDs lists the chats a description may go to, best first: the node answers where the offer
// came in; the client uses the chat the wizard checked, else the dialogs its login reply knows.
function sdpChatIDs() {
  var out = [];
  function add(id) { id = Number(id) || 0; if (id && out.indexOf(id) === -1) out.push(id); }
  if (role === "receiver") add(replyChatID);
  add(chatID);
  if (role === "caller") {
    var cands = dialogCandidates(loginChats, myUid, calleeID);
    for (var i = 0; i < cands.length; i++) add(cands[i]);
  }
  return out;
}

// sendSDPviaMsg puts a description into the chat; resolves true when MAX took it.
function sendSDPviaMsg(sdpType, sdp) {
  var c = client;
  if (!c || !sessionReady) {
    trace("sdp " + sdpType + " not sent: no MAX session");
    return Promise.resolve(false);
  }
  var ids = sdpChatIDs();
  if (!ids.length) {
    trace("sdp " + sdpType + " not sent: no chat to send it to");
    return Promise.resolve(false);
  }
  var emoji = encodeSDP(sdpType, sdp, conversationID);
  function attempt(i) {
    if (i >= ids.length) {
      trace("sdp " + sdpType + ": MAX took the message in none of " + ids.length + " chat(s)");
      return Promise.resolve(false);
    }
    var cid = Math.floor(Math.random() * 9007199254740991) + 1;
    return c.invoke(64, { chatId: ids[i], message: { text: emoji, cid: cid } }).then(function (r) {
      var pl = (r && r.payload) || {};
      if (r && r.cmd === 3) {
        trace("sdp " + sdpType + ": chat " + ids[i] + " refused it: " + (pl.localizedMessage || pl.message || pl.error || "error"));
        return attempt(i + 1);
      }
      trace("sdp " + sdpType + " sent to chat " + ids[i] + " (" + emoji.length + " chars)");
      if (role === "caller") chatID = ids[i];
      return true;
    }, function (e) {
      trace("sdp " + sdpType + " send failed: " + e);
      return false;
    });
  }
  return attempt(0);
}

function handleSDPPush(packet) {
  if (packet.opcode !== 128) return;
  var pl = packet.payload || {};
  var msg = pl.message || {};
  var txt = msg.text || "";
  if (typeof txt !== "string" || txt.indexOf(SDP_PREFIX) !== 0) return;

  var parsed;
  try { parsed = decodeSDP(txt); } catch (e) { trace("sdp message did not decode: " + e); return; }
  trace("sdp " + parsed.type + " came in chat " + pl.chatId + " (call " + (short(parsed.conv) || "?") + ", " + candidateCountOf(parsed.sdp) + " candidate(s))");

  if (role === "receiver" && parsed.type === "offer") onOffer(parsed, pl.chatId);
  else if (role === "caller" && parsed.type === "answer") onAnswer(parsed);
  else trace("sdp " + parsed.type + " is not for a " + role + ", ignored");
}

function candidateCountOf(sdp) {
  return sdputil.candidateCount(sdp);
}

// ── Node: the offer and the answer ──

function onOffer(parsed, chatId) {
  if (chatId) replyChatID = chatId;
  if (parsed.conv && conversationID && parsed.conv !== conversationID) {
    trace("offer for another call (" + short(parsed.conv) + ", ours " + short(conversationID) + "), ignored");
    return;
  }
  if (parsed.sdp === lastOfferSDP) {
    // The caller offering again: it has not seen our answer. Say it once more, a few times at most.
    if (lastAnswerSDP && answerResends < 3) {
      answerResends++;
      trace("the same offer again, repeating the answer (" + answerResends + ")");
      sendSDPviaMsg("answer", lastAnswerSDP);
    } else {
      trace("the same offer again, ignored");
    }
    return;
  }
  lastOfferSDP = parsed.sdp;
  if (!pc) {
    pendingOffer = parsed;
    trace("offer queued: no PeerConnection yet");
    return;
  }
  answerOffer(callGen, parsed);
}

function answerOffer(gen, parsed) {
  var p = pc;
  if (!p) return;
  p.setRemoteDescription("offer", parsed.sdp).then(function () {
    haveRemoteDesc = true;
    trace("call #" + callNo + ": offer applied");
    return p.createAnswer();
  }).then(function (answer) {
    if (gen !== callGen || p !== pc) throw STALE;
    return p.setLocalDescription("answer", answer).then(function () {
      return gatherCandidates();
    }).then(function (cands) {
      if (gen !== callGen || p !== pc) throw STALE;
      lastAnswerSDP = sdputil.withCandidates(answer, cands);
      trace("call #" + callNo + ": answer ready, " + cands.length + " candidate(s), " + lastAnswerSDP.length + " bytes");
      return sendSDPviaMsg("answer", lastAnswerSDP);
    });
  }).catch(function (e) {
    if (e === STALE || gen !== callGen) return;
    callFailed(gen, "answer: " + e);
  });
}

// ── Client: the offer and the answer ──

function sendOfferNow(gen) {
  if (gen !== callGen || offerSent || !offerSDP) return;
  offerSent = true;
  if (acceptWaitTimer) { clearTimeout(acceptWaitTimer); acceptWaitTimer = null; }
  trace("call #" + callNo + ": sending the offer" + (callAccepted ? "" : " (no accept seen, going on anyway)"));
  sendSDPviaMsg("offer", offerSDP);
  // MAX keeps no history for us and the node may not have been listening yet: offer again, with a
  // new message each time (the same cid would be taken for a duplicate), until the answer is in.
  for (var i = 0; i < OFFER_RESEND_MS.length; i++) {
    offerTimers.push(setTimeout(function () {
      if (gen !== callGen || haveRemoteDesc) return;
      trace("call #" + callNo + ": no answer yet, offering again");
      sendSDPviaMsg("offer", offerSDP);
    }, OFFER_RESEND_MS[i]));
  }
}

function onAnswer(parsed) {
  if (parsed.conv && conversationID && parsed.conv !== conversationID) {
    trace("answer for another call (" + short(parsed.conv) + ", ours " + short(conversationID) + "), ignored");
    return;
  }
  if (haveRemoteDesc) { trace("a second answer, ignored"); return; }
  var p = pc;
  if (!p) { trace("an answer with no PeerConnection, ignored"); return; }
  var gen = callGen;
  haveRemoteDesc = true; // before applying: a duplicate arriving meanwhile must not apply it again
  callAccepted = true;   // an answer is the proof the node is in the call, whatever the socket said
  if (acceptDeadlineTimer) { clearTimeout(acceptDeadlineTimer); acceptDeadlineTimer = null; }
  clearOfferTimers();
  p.setRemoteDescription("answer", parsed.sdp).then(function () {
    trace("call #" + callNo + ": answer applied");
  }).catch(function (e) {
    if (gen !== callGen) return;
    callFailed(gen, "answer: " + e);
  });
}

// startPeer runs when the call socket handed over the TURN credentials.
function startPeer(gen) {
  if (!createP2PPeerConnection(gen)) return;
  if (role === "caller") {
    var p = pc;
    p.createOffer().then(function (offer) {
      if (gen !== callGen || p !== pc) throw STALE;
      return p.setLocalDescription("offer", offer).then(function () {
        return gatherCandidates();
      }).then(function (cands) {
        if (gen !== callGen || p !== pc) throw STALE;
        offerSDP = sdputil.withCandidates(offer, cands);
        trace("call #" + callNo + ": offer ready, " + cands.length + " candidate(s), " + offerSDP.length + " bytes");
        if (callAccepted) {
          sendOfferNow(gen);
        } else {
          acceptWaitTimer = setTimeout(function () {
            acceptWaitTimer = null;
            sendOfferNow(gen);
          }, ACCEPT_WAIT_MS);
        }
      });
    }).catch(function (e) {
      if (e === STALE || gen !== callGen) return;
      callFailed(gen, "offer: " + e);
    });
  } else if (pendingOffer) {
    var queued = pendingOffer;
    pendingOffer = null;
    trace("call #" + callNo + ": answering the offer that came early");
    answerOffer(gen, queued);
  }
}

// ── Call signaling socket handling ──

function handleCallMessage(gen, text_, wantCreator) {
  var data;
  try { data = JSON.parse(text_); } catch (e) { return; }
  if (!data || typeof data !== "object") return;
  if (callMsgsLogged < 12) {
    callMsgsLogged++;
    trace("call socket message: " + shape(data));
  }
  if (data.type === "error") {
    trace("call socket error: " + String(data.message || "").slice(0, 160));
    return;
  }
  findLocalID(data, wantCreator);
  if (typeof data.participantId === "number") remoteID = data.participantId;
  if (role === "receiver" && !acceptSent && localID) sendAcceptCall();

  if (data.conversationParams) {
    if (pc) { trace("a second conversationParams, ignored"); return; }
    turnCreds = data.conversationParams;
    startPeer(gen);
  }
}

function onCallSocketMessage(gen, msg, wantCreator) {
  if (gen !== callGen) return;
  var t = typeof msg === "string" ? msg : text.decode(msg);
  if (t === "ping") {
    try { callConn.send("pong"); } catch (e) {}
    return;
  }
  if (t.indexOf("accepted-call") !== -1 && !callAccepted) {
    callAccepted = true;
    if (acceptDeadlineTimer) { clearTimeout(acceptDeadlineTimer); acceptDeadlineTimer = null; }
    trace("call #" + callNo + ": the callee accepted");
    if (role === "caller") sendOfferNow(gen);
  }
  if (t.length < 10) return;
  handleCallMessage(gen, t, wantCreator);
}

function onCallSocketClose(gen, reason) {
  if (gen !== callGen) return;
  callConn = null;
  if (dcOpen) {
    trace("call #" + callNo + ": call socket closed (" + reason + "), the data channel carries on");
    return;
  }
  callFailed(gen, "call socket closed: " + reason);
}

function armConnectDeadline(gen) {
  if (role === "caller") {
    acceptDeadlineTimer = setTimeout(function () {
      acceptDeadlineTimer = null;
      if (gen === callGen && !callAccepted) callFailed(gen, "the node did not answer within " + (ACCEPT_DEADLINE_MS / 1000) + "s");
    }, ACCEPT_DEADLINE_MS);
  }
  connectTimer = setTimeout(function () {
    connectTimer = null;
    if (gen === callGen && !dcOpen) callFailed(gen, "no data channel within " + (CONNECT_DEADLINE_MS / 1000) + "s");
  }, CONNECT_DEADLINE_MS);
}

// callFailed: this call is over (it never came up, or went down). The client dials again after a
// pause; the node just waits for the next call.
function callFailed(gen, why) {
  if (gen !== callGen || !running) return;
  trace("call #" + callNo + " over: " + why);
  resetCallState();
  setState("reconnecting", why);
  if (role === "caller") scheduleCall(why);
}

function scheduleCall(why) {
  if (!running || role !== "caller" || callTimer) return;
  callFails++;
  var delay = Math.min(120000, 2000 * Math.pow(2, Math.min(callFails - 1, 6)));
  delay = Math.round(delay * (0.75 + Math.random() * 0.5));
  trace("redial in " + (delay / 1000).toFixed(1) + "s (" + why + ", fail #" + callFails + ")");
  callTimer = setTimeout(function () {
    callTimer = null;
    startCall("retry");
  }, delay);
}

// startCall (client): CALL_START to the node's account, then the call socket for what the call hands out.
function startCall(why) {
  if (!running || role !== "caller") return;
  if (callTimer) { clearTimeout(callTimer); callTimer = null; }
  if (!sessionReady || !client) { trace("call postponed: the MAX session is not up"); return; }
  if (!calleeID) { setState("dead", "не указан аккаунт ноды (uid)"); return; }
  resetCallState();
  var gen = callGen;
  callNo++;
  var c = client;
  conversationID = maxclient.genUUID();
  trace("call #" + callNo + ": start (" + why + "), call " + short(conversationID) + ", callee " + calleeID);
  setState("connecting", "calling the node");
  armConnectDeadline(gen);

  c.invoke(78, {
    conversationId: conversationID,
    calleeIds: [calleeID],
    internalParams: JSON.stringify({
      deviceId: c.deviceId, sdkVersion: "2.8.9", clientAppKey: "CNHIJPLGDIHBABABA",
      platform: "WEB", protocolVersion: 5, domainId: "", capabilities: "2A03F",
    }),
    isVideo: false,
  }).then(function (resp) {
    if (gen !== callGen) throw STALE;
    var payload = resp.payload || {};
    if (resp.cmd === 3 || payload.error) {
      throw new Error("CALL_START refused: " + (payload.localizedMessage || payload.message || payload.error || "error"));
    }
    var params;
    try { params = JSON.parse(payload.internalCallerParams || "{}"); } catch (e) { params = {}; }
    if (!params.endpoint) throw new Error("CALL_START gave no endpoint (" + Object.keys(payload).join(",") + ")");
    trace("call #" + callNo + ": CALL_START accepted, dialing " + hostOf(params.endpoint));
    var endpoint = params.endpoint + "&platform=WEB&appVersion=1.1&version=5&device=browser&capabilities=2A03F&clientType=ONE_ME&tgt=start";
    return ws.open(endpoint, {}, { readTimeoutMs: CALL_READ_TIMEOUT_MS });
  }).then(function (sock) {
    if (gen !== callGen || !running) { try { sock.close(); } catch (e) {} throw STALE; }
    callConn = sock;
    sock.onmessage = function (msg) { onCallSocketMessage(gen, msg, false); };
    sock.onclose = function (reason) { onCallSocketClose(gen, reason); };
    trace("call #" + callNo + ": call socket open");
  }).catch(function (e) {
    if (e === STALE || gen !== callGen) return;
    callFailed(gen, String(e));
  });
}

// onIncomingCall (node): the push says a call is coming; join its socket and accept.
function onIncomingCall(payload) {
  var convId = String(payload.conversationId || "");
  trace("incoming call " + short(convId) + " {" + Object.keys(payload).join(",") + "}");
  if (convId && convId === conversationID && pc) { trace("already in this call"); return; }
  var details;
  var endpoint;
  try {
    details = maxclient.decodeCallDetails(payload.vcp || "");
    endpoint = maxclient.craftEndpoint(convId, details);
  } catch (e) {
    trace("the incoming call did not decode: " + e);
    return;
  }

  var early = pendingOffer; // an offer for this call may have beaten the push
  resetCallState();
  var gen = callGen;
  callNo++;
  conversationID = convId;
  if (early && (!early.conv || early.conv === convId)) pendingOffer = early;
  trace("call #" + callNo + ": joining " + hostOf(endpoint));
  setState("connecting", "incoming call");
  armConnectDeadline(gen);

  ws.open(endpoint, {}, { readTimeoutMs: CALL_READ_TIMEOUT_MS }).then(function (sock) {
    if (gen !== callGen || !running) { try { sock.close(); } catch (e) {} return; }
    callConn = sock;
    sock.onmessage = function (msg) { onCallSocketMessage(gen, msg, true); };
    sock.onclose = function (reason) { onCallSocketClose(gen, reason); };
    trace("call #" + callNo + ": call socket open");
    setTimeout(function () {
      if (gen === callGen && !acceptSent) {
        trace("call #" + callNo + ": no participant id yet, accepting anyway");
        sendAcceptCall();
      }
    }, ACCEPT_FALLBACK_MS);
  }).catch(function (e) {
    if (gen !== callGen) return;
    callFailed(gen, "join: " + e);
  });
}

// ── Lanes (see the constants) ──

// rebuildSendDCs lists the channels that are open, the call's own first.
function rebuildSendDCs() {
  var out = [];
  if (dc && dcOpen) out.push(dc);
  for (var k = 1; k < lanes.length; k++) {
    var l = lanes[k];
    if (l && l.open && l.d) out.push(l.d);
  }
  sendDCs = out;
}

function sendRecord(d, b) {
  try {
    d.send(b);
    bytesOut += b.byteLength || 0;
  } catch (e) {
    // The channel is going down: the PeerConnection's own state change says so.
    writeErrors++;
    if (writeErrors <= 3) trace("data channel write failed (" + writeErrors + "): " + e);
  }
}

function startPump() {
  if (pumpTimer) return;
  if (!pumpAt) pumpAt = Date.now();
  pumpTimer = setInterval(pumpLanes, PUMP_MS);
}

function stopPump() {
  if (pumpTimer) { clearInterval(pumpTimer); pumpTimer = null; }
}

// pumpLanes tops up every lane's credit for the time that has passed and hands the queue's oldest records to
// the lanes that have credit; a lane may go a record into debt, which the next top-ups pay off.
function pumpLanes() {
  var n = sendDCs.length;
  var now = Date.now();
  var dt = Math.max(0, now - pumpAt);
  pumpAt = now;
  if (!n) { txq = []; txQueued = 0; stopPump(); return; }
  var add = laneRate * dt / 1000;
  var cap = laneRate * CREDIT_MS / 1000;
  for (var i = 0; i < n; i++) {
    var c = sendDCs[i];
    c.__credit = Math.min((c.__credit || 0) + add, cap);
  }
  var progress = true;
  while (txq.length && progress) {
    progress = false;
    for (var k = 0; k < n && txq.length; k++) {
      sendRR = (sendRR + 1) % n;
      var d = sendDCs[sendRR];
      if (d.__credit > 0) {
        var rec = txq.shift();
        txQueued -= rec.cost;
        d.__credit -= rec.cost;
        sendRecord(d, rec.b);
        progress = true;
      }
    }
  }
  if (!txq.length) stopPump();
}

function closeLane(l) {
  if (!l) return;
  l.closed = true;
  for (var i = 0; i < l.timers.length; i++) clearTimeout(l.timers[i]);
  l.timers = [];
  var p = l.p;
  l.p = null;
  l.d = null;
  l.open = false;
  if (p) { try { p.close(); } catch (e) {} }
}

function closeLanes() {
  for (var i = 0; i < laneTimers.length; i++) clearTimeout(laneTimers[i]);
  laneTimers = [];
  laneTries = [];
  for (var k = 1; k < lanes.length; k++) closeLane(lanes[k]);
  lanes = [];
  sendDCs = [];
  txq = [];
  txQueued = 0;
  stopPump();
}

// laneTurn is whose credentials lane k allocates with: the call's own, or (the client's lanes from LANES_OWN
// on) the node's, null while they have not come.
function laneTurn(k) {
  if (role === "caller" && k >= LANES_OWN) return peerTurn;
  return turnCreds && turnCreds.turn;
}

// laneURLs is the relay a lane allocates on: one of the credentials' servers, the lanes spread over them.
function laneURLs(k, turn) {
  var urls = turn && turn.urls;
  if (typeof urls === "string") urls = [urls];
  if (!urls || !urls.length) return null;
  var use = turnURLsOf(urls, TURN_TRANSPORT[role]);
  return [use[k % use.length]];
}

// newLane makes lane k's PeerConnection; its events are ignored once the lane is closed or replaced.
function newLane(gen, k) {
  var turn = laneTurn(k);
  var urls = laneURLs(k, turn);
  if (!turn || !urls) return null;
  var l = {
    k: k, p: null, d: null, open: false, closed: false, timers: [],
    g: { cands: [], done: false, waiters: [], settled: false },
    offerSDP: "", lastOfferSDP: "", answerSDP: "", answerResends: 0, haveRemote: false,
  };
  var p = webrtc.newPeerConnection(pcOptions(urls, turn));
  l.p = p;
  p.onicecandidate = function (c) {
    if (p !== l.p) return;
    if (c && c.candidate) { l.g.cands.push(String(c.candidate)); return; }
    l.g.done = true;
    var waiting = l.g.waiters;
    l.g.waiters = [];
    for (var i = 0; i < waiting.length; i++) waiting[i]();
  };
  p.oniceconnectionstatechange = function (s) {
    if (p === l.p) trace("lane " + k + ": ice " + s);
  };
  p.onconnectionstatechange = function (s) {
    if (p !== l.p) return;
    trace("lane " + k + ": peerconnection " + s);
    if (s === "failed" || s === "closed" || s === "disconnected") laneFailed(gen, l, "peerconnection " + s);
  };
  l.timers.push(setTimeout(function () {
    if (!l.closed && !l.open) laneFailed(gen, l, "no data channel within " + (LANE_CONNECT_DEADLINE_MS / 1000) + "s");
  }, LANE_CONNECT_DEADLINE_MS));
  return l;
}

function wireLaneChannel(gen, l, d) {
  d.onopen = function () {
    if (l.closed || d !== l.d || gen !== callGen) return;
    l.open = true;
    rebuildSendDCs();
    trace("lane " + l.k + ": data channel open (" + sendDCs.length + " channel(s) in all)");
  };
  d.onclose = function () {
    if (d !== l.d) return;
    laneFailed(gen, l, "data channel closed");
  };
  d.onmessage = function (bytes) {
    if (d !== l.d) return;
    bytesIn += bytes.byteLength || 0;
    emit(bytes);
  };
}

// laneFailed drops a lane. The client opens it again a little later, a few times.
function laneFailed(gen, l, why) {
  if (gen !== callGen || l.closed) return;
  trace("lane " + l.k + " over: " + why);
  closeLane(l);
  if (lanes[l.k] === l) lanes[l.k] = null;
  rebuildSendDCs();
  if (role === "caller" && dcOpen) scheduleLane(gen, l.k, LANE_RETRY_MS);
}

// startLanes: the client opens its lanes once the call's own channel is up.
function startLanes(gen) {
  if (role !== "caller" || LANES < 2) return;
  for (var k = 1; k < LANES; k++) scheduleLane(gen, k, LANE_OPEN_GAP_MS * k);
}

function scheduleLane(gen, k, delay) {
  laneTries[k] = laneTries[k] || 0;
  if (laneTries[k] >= LANE_TRIES) { trace("lane " + k + ": given up after " + LANE_TRIES + " tries"); return; }
  laneTimers.push(setTimeout(function () {
    if (gen !== callGen || !dcOpen) return;
    if (k >= LANES_OWN && !peerTurn) {
      if (Date.now() - dcOpenAt < LANE_PEER_WAIT_MS) { scheduleLane(gen, k, 1000); return; }
      if (k === LANES_OWN) trace("the node handed no TURN credentials over: lanes " + LANES_OWN + " and up not opened");
      return;
    }
    openLane(gen, k);
  }, delay));
}

// openLane (client): a PeerConnection of its own with a data channel, and its offer through the chat.
function openLane(gen, k) {
  if (lanes[k]) return;
  laneTries[k] = (laneTries[k] || 0) + 1;
  var l = newLane(gen, k);
  if (!l) return;
  lanes[k] = l;
  var p = l.p;
  l.d = p.createDataChannel("x", { ordered: false, maxRetransmits: 0 });
  wireLaneChannel(gen, l, l.d);
  p.createOffer().then(function (offer) {
    if (l.closed || p !== l.p) throw STALE;
    return p.setLocalDescription("offer", offer).then(function () {
      return gatherCandidates(l.g);
    }).then(function (cands) {
      if (l.closed || p !== l.p) throw STALE;
      l.offerSDP = sdputil.withCandidates(offer, cands);
      trace("lane " + k + ": offer ready, " + cands.length + " candidate(s)");
      sendLaneOffer(gen, l);
    });
  }).catch(function (e) {
    if (e === STALE || l.closed) return;
    laneFailed(gen, l, "offer: " + e);
  });
}

// sendLaneOffer puts the offer into the chat, and again (as a new message each time) until the answer is in.
function sendLaneOffer(gen, l) {
  sendLaneSignal("offer", l.k, l.offerSDP);
  for (var i = 0; i < LANE_OFFER_RESEND_MS.length; i++) {
    l.timers.push(setTimeout(function () {
      if (gen !== callGen || l.closed || l.haveRemote) return;
      trace("lane " + l.k + ": no answer yet, offering again");
      sendLaneSignal("offer", l.k, l.offerSDP);
    }, LANE_OFFER_RESEND_MS[i]));
  }
}

// The lanes' descriptions travel inside the call's own data channel, as text (the tunnel's packets are binary),
// so a dozen lanes do not put two dozen messages into the account's chat.
function sendLaneSignal(sdpType, k, sdp) {
  var d = dc;
  if (!d || !dcOpen) return false;
  try {
    d.send(JSON.stringify({ lane: k, type: sdpType, sdp: sdp }));
    return true;
  } catch (e) {
    trace("lane " + k + ": " + sdpType + " not sent: " + e);
    return false;
  }
}

// lendTurn (node): the call's credentials go to the client over the call's own channel. The node is reached
// directly and needs none, and a user's allocations at a TURN server are limited in number.
function lendTurn(gen) {
  if (gen !== callGen || role !== "receiver" || !NODE_DIRECT || !dcOpen) return;
  var turn = turnCreds && turnCreds.turn;
  if (!turn || !turn.urls || !turn.username || !turn.credential) return;
  try {
    dc.send(JSON.stringify({ type: "turn", turn: { urls: turn.urls, username: turn.username, credential: turn.credential } }));
    trace("TURN credentials handed to the client");
  } catch (e) {
    trace("TURN credentials not sent: " + e);
  }
}

// sendCfg (client): what the node deals out per lane is the client's to say, so a change of the number needs
// no new node.
function sendCfg(gen) {
  if (gen !== callGen || role !== "caller" || !dcOpen || !LANE_RATE) return;
  try {
    dc.send(JSON.stringify({ type: "cfg", rate: LANE_RATE }));
  } catch (e) {
    trace("cfg not sent: " + e);
  }
}

function onLaneSignal(gen, bytes) {
  if (gen !== callGen) return;
  var m;
  try { m = JSON.parse(text.decode(bytes)); } catch (e) { trace("a text on the data channel did not parse: " + e); return; }
  if (m && m.type === "cfg") {
    var r = Number(m.rate);
    if (role === "receiver" && r >= 1000 && r <= 30000) {
      laneRate = r;
      trace("lane rate " + r + " B/s, as the client says");
    }
    return;
  }
  if (m && m.type === "turn") {
    var t = m.turn;
    if (role === "caller" && t && t.urls && t.username && t.credential) {
      peerTurn = t;
      trace("the node handed its TURN credentials over");
    }
    return;
  }
  var k = Number(m && m.lane) || 0;
  if (!m || k < 1 || typeof m.sdp !== "string") return;
  if (role === "receiver" && m.type === "offer") onLaneOffer(m.sdp, k);
  else if (role === "caller" && m.type === "answer") onLaneAnswer(m.sdp, k);
  else trace("lane " + k + ": " + m.type + " is not for a " + role + ", ignored");
}

function onLaneAnswer(sdp, k) {
  var l = lanes[k];
  if (!l || l.closed || l.haveRemote || !l.p) { trace("lane " + k + ": an answer with no use, ignored"); return; }
  var gen = callGen;
  var p = l.p;
  l.haveRemote = true;
  p.setRemoteDescription("answer", sdp).then(function () {
    trace("lane " + k + ": answer applied");
  }).catch(function (e) {
    laneFailed(gen, l, "answer: " + e);
  });
}

// onLaneOffer (node): the client's lane k. The offer again (it has not seen the answer) gets the
// answer again; a different one means the client started the lane over.
function onLaneOffer(sdp, k) {
  if (!pc || !turnCreds) { trace("lane " + k + ": offer before the call is up, ignored (the client offers again)"); return; }
  var gen = callGen;
  var l = lanes[k];
  if (l && !l.closed && l.lastOfferSDP === sdp) {
    if (l.answerSDP && l.answerResends < 3) {
      l.answerResends++;
      trace("lane " + k + ": the same offer again, repeating the answer (" + l.answerResends + ")");
      sendLaneSignal("answer", k, l.answerSDP);
    }
    return;
  }
  if (l) { closeLane(l); lanes[k] = null; }
  l = newLane(gen, k);
  if (!l) return;
  lanes[k] = l;
  l.lastOfferSDP = sdp;
  var p = l.p;
  p.ondatachannel = function (remote) {
    if (p !== l.p || l.closed) return;
    l.d = remote;
    wireLaneChannel(gen, l, remote);
  };
  p.setRemoteDescription("offer", sdp).then(function () {
    return p.createAnswer();
  }).then(function (answer) {
    if (l.closed || p !== l.p) throw STALE;
    return p.setLocalDescription("answer", answer).then(function () {
      return gatherCandidates(l.g);
    }).then(function (cands) {
      if (l.closed || p !== l.p) throw STALE;
      l.answerSDP = sdputil.withCandidates(answer, cands);
      trace("lane " + k + ": answer ready, " + cands.length + " candidate(s)");
      sendLaneSignal("answer", k, l.answerSDP);
    });
  }).catch(function (e) {
    if (e === STALE || l.closed) return;
    laneFailed(gen, l, "answer: " + e);
  });
}

// ── Status ──

// A line a minute with the state of everything, for working out afterwards what a drop was about.
function traceStatus() {
  var call = "no call";
  if (pc) {
    call = "call #" + callNo + (dcOpen ? " up " + Math.round((Date.now() - dcOpenAt) / 1000) + "s, " + sendDCs.length + " channel(s), in " + bytesIn + "B out " + bytesOut + "B, queue " + txQueued + "B, dropped " + txDropped : " setting up");
  } else if (callTimer) {
    call = "redial pending (fail #" + callFails + ")";
  }
  trace("status: MAX session " + (sessionReady ? "up " + Math.round((Date.now() - sessionUpAt) / 1000) + "s" : "down") + ", " + call);
}

// ── MAX signaling session ──

function stopKeepalive() {
  if (keepTimer) { clearInterval(keepTimer); keepTimer = null; }
}

function startKeepalive(gen) {
  stopKeepalive();
  keepFails = 0;
  keepTimer = setInterval(function () {
    if (gen !== sessionGen || !client) return;
    client.invoke(1, { interactive: false }, KEEPALIVE_TIMEOUT_MS).then(function () {
      keepFails = 0;
    }).catch(function (e) {
      if (gen !== sessionGen) return;
      keepFails++;
      trace("MAX ping failed (" + keepFails + "): " + e);
      if (keepFails >= 2 || /signaling (send|closed)/.test(String(e))) sessionLost(gen, "no answer to the ping");
    });
  }, KEEPALIVE_MS);
}

// dropClient closes the session's socket (its late close event is stale by then).
function dropClient() {
  var c = client;
  client = null;
  sessionReady = false;
  stopKeepalive();
  if (c && c.sock) { try { c.sock.close(); } catch (e) {} }
}

// scheduleSession logs in again after a pause: short while the network is simply away (nothing is
// sent to MAX then), long after MAX itself turned the login down (each login counts against the token).
function scheduleSession(afterRefusal) {
  if (!running || sessionTimer) return;
  sessionFails++;
  var cap = afterRefusal ? 300000 : 20000;
  var delay = Math.min(cap, 1000 * Math.pow(2, Math.min(sessionFails - 1, 9)));
  delay = Math.round(delay * (0.8 + Math.random() * 0.4));
  trace("MAX session: next try in " + (delay / 1000).toFixed(1) + "s (fail #" + sessionFails + ")");
  sessionTimer = setTimeout(function () {
    sessionTimer = null;
    startSession("retry");
  }, delay);
}

// sessionLost: the signaling socket is gone or no longer answers. A call that is up carries on without it.
function sessionLost(gen, why) {
  if (gen !== sessionGen || !running || sessionTimer) return;
  trace("MAX session #" + gen + " lost: " + why);
  if (sessionUpAt && Date.now() - sessionUpAt > 60000) sessionFails = 0; // it was a good one
  sessionUpAt = 0;
  dropClient();
  if (!dcOpen) setState("reconnecting", "MAX: " + why);
  scheduleSession(false);
}

function onSessionFailed(gen, phase, e) {
  var msg = String(e);
  trace("MAX session #" + gen + " " + phase + " failed: " + msg);
  dropClient();
  if (msg.indexOf("MAX не принял токен") !== -1) {
    setState("dead", msg);
    if (sessionFails < 9) sessionFails = 9;
    scheduleSession(true);
    return;
  }
  if (!dcOpen) setState("reconnecting", "MAX: " + msg);
  scheduleSession(false);
}

function noteOpcode(pkt) {
  var op = pkt.opcode;
  seenOps[op] = (seenOps[op] || 0) + 1;
  if (seenOps[op] === 1 || op === 137) {
    trace("push opcode " + op + " (cmd " + pkt.cmd + ") " + shape(pkt.payload));
  }
}

function onSessionEvent(gen, pkt) {
  if (gen !== sessionGen || !running) return;
  if (pkt.__closed) { sessionLost(gen, "socket closed"); return; }
  noteOpcode(pkt);
  if (pkt.opcode === 137 && role === "receiver") { onIncomingCall(pkt.payload || {}); return; }
  handleSDPPush(pkt);
}

function onSessionReady(gen, resp) {
  var payload = resp.payload || {};
  var me = accountOf(payload, 0);
  if (me.uid) myUid = me.uid;
  loginChats = payload.chats || [];
  sessionReady = true;
  sessionUpAt = Date.now();
  trace("MAX session #" + gen + ": logged in as " + myUid + ", " + loginChats.length + " chat(s)");
  startKeepalive(gen);
  if (role === "receiver") {
    if (!pc) setState("connecting", "waiting for a call");
  } else if (!pc && !callTimer) {
    startCall("session ready");
  }
}

function startSession(why) {
  if (!running) return;
  var gen = ++sessionGen;
  dropClient();
  if (!deviceId) deviceId = maxclient.genUUID();
  var phase = "dial";
  trace("MAX session #" + gen + ": connecting (" + why + ")");
  maxclient.connect({ deviceId: deviceId, readTimeoutMs: SIGNALING_READ_TIMEOUT_MS }).then(function (c) {
    if (gen !== sessionGen || !running) { try { c.sock.close(); } catch (e) {} throw STALE; }
    client = c;
    c.setOnEvent(function (pkt) { onSessionEvent(gen, pkt); });
    phase = "login";
    return maxclient.loginByToken(c, maxToken);
  }).then(function (resp) {
    if (gen !== sessionGen || !running) throw STALE;
    onSessionReady(gen, resp);
  }).catch(function (e) {
    if (e === STALE || gen !== sessionGen || !running) return;
    onSessionFailed(gen, phase, e);
  });
}

// ── Transport interface ──

// readParams takes the settings the carrier was given (the core adds exit itself: it is the node).
function readParams(p) {
  maxToken = p.token || "";
  calleeID = parseInt(p.uid, 10) || 0;
  chatID = parseInt(p.chatId, 10) || 0;
  role = (p.exit === true || p.exit === "true") ? "receiver" : "caller";
}

// begin signs in with the token and runs the role: the node waits for the call, the client makes it.
function begin() {
  if (!statusTimer) statusTimer = setInterval(traceStatus, 60000);
  startSession("begin");
}

// stopAll drops what a running transport holds, for a restart with other settings or for good.
function stopAll() {
  if (statusTimer) { clearInterval(statusTimer); statusTimer = null; }
  if (sessionTimer) { clearTimeout(sessionTimer); sessionTimer = null; }
  if (callTimer) { clearTimeout(callTimer); callTimer = null; }
  sessionGen++; // the old session's late events are stale now
  dropClient();
  resetCallState();
}

var tokenFromParams = false;

var Transport = {
  info: function () {
    return {
      name: "oneme-webrtc",
      version: "3.8.2",
      // What a setup page hands over lands in the cookie jar of this address (cookiesApplied).
      cookieDomain: "https://web.max.ru/",
      mtu: 0,
      carrierMtu: CARRIER_MTU,
      batchBytes: BATCH_BYTES,
      reliable: false,
      ordered: false,
      // All of them are settings, none is the profile's one input: a profile's value field would otherwise be
      // taken for the token, and wiped when the wizard filled the setting instead. Which end this is the core
      // says itself (cfg.params.exit), so it is no setting.
      params: [
        { key: "token", label: "MAX web token", type: "secret", required: true, scope: "settings" },
        { key: "uid", label: "Callee userId", type: "text", required: false, scope: "settings" },
        { key: "chatId", label: "Chat ID for signaling", type: "text", required: false, scope: "settings" },
      ],
    };
  },

  // setup is the wizard an app runs while a node is being made (before anything connects): it
  // signs the accounts in and hands back {client: {...}, node: {...}} through openfluxSubmit.
  setup: function (values) {
    startWizard(values || {});
  },

  open: function (cfg) {
    readParams((cfg && cfg.params) || {});
    tokenFromParams = !!maxToken;
    running = true;
    T0 = Date.now();
    sessionFails = 0;
    callFails = 0;
    seenOps = {};
    trace("open 3.8.2: " + role + (calleeID ? ", callee " + calleeID : "") + (chatID ? ", chat " + chatID : ", no chat id") + (maxToken ? "" : ", no token"));

    if (!maxToken) {
      if (role === "receiver") {
        // A node has no one to sign in for it: it needs the token its setup gave it.
        setState("dead", "у ноды нет токена аккаунта MAX: настройте ноду заново");
        return;
      }
      // Nothing saved yet: ask for the account (or both), the answer comes back as cookiesApplied.
      startWizard({ uid: calleeID ? String(calleeID) : "" });
      return;
    }
    begin();
  },

  // cookiesApplied: the setup page's answer for this side (token, uid, chatId), or what the app
  // saved from an earlier one. Settings the carrier was given outrank it.
  onEvent: function (kind) {
    if (kind !== "cookiesApplied" || !running || tokenFromParams) return;
    var c = cookieJar.get();
    if (!c.token || c.token === maxToken) return;
    stopAll();
    maxToken = c.token;
    calleeID = parseInt(c.uid, 10) || calleeID;
    chatID = parseInt(c.chatId, 10) || chatID;
    begin();
  },

  write: function (bytes) {
    var n = sendDCs.length;
    if (!n) return;
    if (!laneRate) {
      sendRR = (sendRR + 1) % n;
      sendRecord(sendDCs[sendRR], bytes);
      return;
    }
    var cost = (bytes.byteLength || 0) + WIRE_OVERHEAD;
    if (cost <= SMALL_RECORD + WIRE_OVERHEAD) {
      // An ACK does not wait, but the lane it takes is charged for it.
      var best = sendDCs[0];
      for (var i = 1; i < n; i++) {
        if ((sendDCs[i].__credit || 0) > (best.__credit || 0)) best = sendDCs[i];
      }
      best.__credit = (best.__credit || 0) - cost;
      sendRecord(best, bytes);
      return;
    }
    if (txQueued + cost > n * laneRate * QUEUE_MS / 1000) { txDropped++; return; }
    txq.push({ b: bytes, cost: cost });
    txQueued += cost;
    startPump();
  },

  close: function () {
    running = false;
    if (wizardServer) { try { wizardServer.close(); } catch (e) {} wizardServer = null; }
    stopAll();
  },

};
