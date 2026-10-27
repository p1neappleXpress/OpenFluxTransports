// OpenFlux script transport for VK Teams calls (WebRTC DataChannel over VK Calls room).
//
// Establishes a high-speed, low-latency P2P/TURN WebRTC DataChannel between peers
// using the VK Teams calls signaling WebSocket (custom-data command).
//
// Params:
//   url:     VK Teams call join link (e.g. https://calls.myteam.mail.ru/join/...)
//   relay:   true to use the call's TURN server only
//   account: the sign-in the wizard took (a secret); empty: join as a guest
//   exit:    set by the core: true on the node (listener), false on the client (initiator)
//
// Written by trader52. OpenFlux's changes on top of 1.0.0 (the wire is untouched: ends of both versions talk):
//   - the guest is called "Гость" and has an ordinary device id (the service shows both to the other participants);
//   - the settings are declared as settings, none is a profile field (the wizard fills them, both ends alike);
//   - Transport.setup: a wizard page that checks the call link and hands back {client, node};
//   - the log carries no signaling address and no candidate text;
//   - an account instead of a guest: the wizard's sign-in window hands over the cookies of the VK WorkSpace
//     superapp, the script gets a messenger session from them the way the superapp does (a silent token, then
//     withSession), asks for the calls token with that session and goes on as a guest does. That lets the wizard
//     create a call and lets an end join a call that is closed to guests;
//   - a failed attempt is tried again until the transport is closed (1.0.0 tried a second time and then stopped);
//   - the data channel is unordered and unreliable, paced, and one tunnel packet is one datagram (see "The data
//     channel's datagrams"). The wire is the same, but the end that creates the channel decides how it behaves.

var BASE_URL = "https://u.myteam.vmailru.net";
var API_VERSION = "v149";
var GUEST_NAME = "Гость";
var UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
var OK_API = "https://calls.okcdn.ru";
var OK_APP_KEY = "CIAPPDKGDIHBABABA";
var CALL_HOST = "https://calls.myteam.mail.ru";

// Signing in as an account (see "Sign-in chains"): the VK WorkSpace superapp and the messenger behind it.
var WS_ORIGIN = "https://app.workspace.vk.ru";
var WS_CLIENT_ID = "9oxgKFbOIviE1bXPWf1YfbgkAxNLRopf";
var SESSION_API_VERSION = "v145";
// The Mail.ru SSO that turns a calls-scoped silent token into the access token the calls API takes, and the calls app
// those requests claim to come from. A signed-in account creates a call through these; everyone (the account included)
// then joins the call as a guest.
var SSO_ORIGIN = "https://corsapi.imgsmail.ru";
var CALLS_ORIGIN = "https://calls-app.myteam.mail.ru";
var SSO_RESOURCE = "https://e.mail.ru";
var MESSENGER_HOSTS = [BASE_URL]; // u.myteam.mail.ru is dead (no DNS); vmailru.net only

// ── The data channel's datagrams ──
//
// The tunnel inside needs no help from the channel under it: its packets are IP packets, its TCP orders and
// retransmits for itself. So the channel is unordered and has no retransmissions (a reliable one stalled a
// second at every loss, and the TCP inside retransmitted what it had already re-sent). What is left is to keep
// one tunnel packet in one datagram: a message longer than one SCTP chunk is cut into several datagrams sent
// back to back, and the relay in front of a mobile network keeps the first of such a burst and drops most of
// the rest.
//   SCTP_MTU      the SCTP packet; plus 69 bytes of DTLS, TURN, UDP and IP it is the datagram, 1429 here
//   a chunk       holds SCTP_MTU - 32 (the common header and an I-DATA header)
//   CARRIER_MTU   the IP packet the tunnel runs over this channel; the tunnel puts 115 bytes around it
//                 (78 envelope, 4 batch framing, 33 encryption): 1200 + 115 = 1315 <= 1328
//   BATCH_BYTES   how much the tunnel may coalesce into one message: a chunk less the 35 bytes of
//                 encryption and batch header
var SCTP_MTU = 1360;
var CARRIER_MTU = 1200;
var BATCH_BYTES = SCTP_MTU - 32 - 35;

var running = false;
var isExit = false;
var joinLink = "";
var callConfig = null;
var account = null; // {cookie, ua, name} of the account this end joins as; null: as a guest
var accountText = ""; // the setting it was read from, to see a changed one
var attempting = false;
var retryTimer = null;
var retryCount = 0;

// Network state
var wsConn = null;
var wsSeq = 1;
var pc = null;
var dc = null;
var offerTimer = null;
var reconnTimer = null; // a grace timer for an ICE "disconnected" blip before a full reconnect
var lastReconnectAt = 0; // when the last forced reconnect fired, to back off if the link is flapping
var pendingCandidates = [];
var haveRemoteDesc = false;
var offerEpoch = 0;
var localAnswer = null;
var forceRelay = false;
var myPeerId = "";
var targetPeerId = "";

function nextSeq() {
  return wsSeq++;
}

// newDeviceID is an ordinary random id of a browser's shape (8-4-4-4-12 hex).
function newDeviceID() {
  var h = "";
  for (var i = 0; i < 32; i++) h += Math.floor(Math.random() * 16).toString(16);
  return h.slice(0, 8) + "-" + h.slice(8, 12) + "-4" + h.slice(13, 16) + "-" +
    "89ab".charAt(Math.floor(Math.random() * 4)) + h.slice(17, 20) + "-" + h.slice(20, 32);
}

// hostOf names an address by its scheme and host, never by its query (a signaling address carries the call's token).
function hostOf(url) {
  var m = /^(wss?:\/\/[^\/?#]+)/.exec(String(url));
  return m ? m[1] : "?";
}

function extractJoinLink(url) {
  if (!url) return "";
  var u = String(url).trim();
  var idx = u.indexOf("/join/");
  if (idx !== -1) {
    var sub = u.substring(idx + 6);
    var q = sub.indexOf("?");
    if (q !== -1) sub = sub.substring(0, q);
    var sl = sub.indexOf("/");
    if (sl !== -1) sub = sub.substring(0, sl);
    return sub;
  }
  if (u.indexOf("/") !== -1) {
    var parts = u.split("/");
    return parts[parts.length - 1];
  }
  return u;
}

function resetCallState() {
  haveRemoteDesc = false;
  pendingCandidates = [];
  localAnswer = null;
  if (offerTimer) {
    clearInterval(offerTimer);
    offerTimer = null;
  }
  if (dc) {
    try {
      dc.onclose = null;
      dc.close();
    } catch (e) {}
    dc = null;
  }
  if (pc) {
    try {
      pc.onconnectionstatechange = null;
      pc.close();
    } catch (e) {}
    pc = null;
  }
}

function clearReconnTimer() {
  if (reconnTimer) {
    clearTimeout(reconnTimer);
    reconnTimer = null;
  }
}

// forceReconnect tears the live attempt down and starts a clean one. The peer connection breaking (a Wi-Fi<->LTE jump,
// say) is not caught by the ws.onclose path on its own: the signalling socket can stay up for a while, so waiting for it
// would hang, and re-sending the old offer only carries ICE candidates for a network that is gone. A fresh attempt
// signs in again and gathers candidates for the network the device is on now. A quick flap is backed off to every 5 s.
function forceReconnect(reason) {
  clearReconnTimer();
  if (!running) return;
  console.log("[VKTEAMS] forcing reconnect: " + reason);
  setState("reconnecting", reason);
  stopSocket();        // drop the (maybe still-open) signalling socket; its onclose will not start a second retry
  resetCallState();    // tear down pc/dc and the offer timer
  var now = Date.now();
  var delay = (now - lastReconnectAt < 5000) ? 5000 : 1000;
  lastReconnectAt = now;
  retryLater(delay);
}

// scheduleReconnect waits a moment first, so a brief ICE "disconnected" that recovers on its own is not churned; a
// return to "connected" clears the timer.
function scheduleReconnect(reason, ms) {
  if (!running || reconnTimer) return;
  reconnTimer = setTimeout(function () {
    reconnTimer = null;
    forceReconnect(reason);
  }, ms);
}

// exitDropConnection is the node's answer to its peer connection breaking: it cannot dial, so it drops the dead peer and
// waits for the client's next (fresh) offer, which handleRemoteOffer then rebuilds from.
function exitDropConnection(reason) {
  setState("reconnecting", reason);
  resetCallState();
  targetPeerId = "";
}

function sendCustomData(payload) {
  if (!wsConn) return;
  var msg = {
    command: "custom-data",
    sequence: nextSeq(),
    data: payload
  };
  try {
    wsConn.send(JSON.stringify(msg));
  } catch (e) {
    // ignore send error
  }
}

// ===== Sign-in chains =====
//
// Whoever joins gets a token of the calls service (rapi okcalls/getAuthToken), logs in to the calls API with it and
// joins the call by its link. The chains differ in whose token that is: a guest's, which the service makes up on the
// spot (all that 1.0.0 knew), or an account's, from the cookies the setup wizard's sign-in window took.

// StepError is a chain failing at a step: which one, what status the service answered (0: none), whether the service
// refused the account's sign-in itself (trying again changes nothing: the person signs in again), whether there was
// no way to the service at all and whether the session in hand ran out (a new one is worth a try).
function StepError(step, message, status, auth, network) {
  this.name = "StepError";
  this.message = message;
  this.step = step;
  this.status = status || 0;
  this.auth = !!auth;
  this.network = !!network;
  this.expired = false;
  this.code = 0;
}
StepError.prototype = Object.create(Error.prototype);
StepError.prototype.constructor = StepError;

var STEP_LABELS = {
  guest: "Гостевой вход",
  userinfo: "Вход в аккаунт",
  silent: "Токен звонков",
  session: "Авторизация звонков",
  calls: "Токен звонков",
  login: "Вход в сервис звонков",
  join: "Вход в звонок",
  create: "Создание звонка"
};

// newReport is how a chain tells each step: the log keeps it, the wizard shows it. A note is a status, a host or the
// names of the keys of an answer, never a token, a cookie or a name.
function newReport() {
  var steps = [];
  return {
    steps: steps,
    rep: function (step, ok, note) {
      steps.push({ id: step, label: STEP_LABELS[step] || step, ok: ok, note: note || "" });
      console.log("[VKTEAMS] " + step + (ok ? " ok" : " FAILED") + (note ? " (" + note + ")" : ""));
    }
  };
}

function parse(body) {
  try { return JSON.parse(body); } catch (e) { return null; }
}

// shape names the keys of a JSON answer, depth levels down, and not what they hold.
function shape(v, depth) {
  if (v === null || v === undefined) return "empty";
  if (typeof v !== "object") return typeof v;
  if (Array.isArray(v)) return "[" + (v.length && depth > 0 ? shape(v[0], depth - 1) : v.length) + "]";
  var keys = Object.keys(v);
  var out = [];
  for (var i = 0; i < keys.length && i < 16; i++) {
    var x = v[keys[i]];
    out.push(keys[i] + (depth > 0 && x !== null && typeof x === "object" ? shape(x, depth - 1) : ""));
  }
  return "{" + out.join(",") + (keys.length > 16 ? ",…" : "") + "}";
}

// hostName names an address by its host, never by its path or query.
function hostName(url) {
  var m = /^https?:\/\/([^\/?#]+)/.exec(String(url));
  return m ? m[1] : "?";
}

// errText is an error's text, cut short for a note.
function errText(e) {
  return String(e && e.message || e).slice(0, 160);
}

// send is http.fetch with no answer at all told as a StepError.
async function send(step, req) {
  try {
    return await http.fetch(req);
  } catch (e) {
    throw new StepError(step, "no connection: " + errText(e), 0, false, true);
  }
}

// msgHeaders are the headers the messenger's web client sends (the guest wire of 1.0.0, the calls API).
function msgHeaders(extra) {
  var h = { "User-Agent": UA, "Origin": "https://myteam.mail.ru", "Referer": "https://myteam.mail.ru/" };
  for (var k in extra) h[k] = extra[k];
  return h;
}

// okApi calls the calls API (fb.do) the way its web SDK does: a form, an object in it as JSON, the session key when
// there is one. An answer with error_msg is a refusal whatever the HTTP status says.
async function okApi(step, method, params, sessionKey, base) {
  var all = { method: method, format: "JSON", application_key: OK_APP_KEY };
  for (var k in params) all[k] = params[k];
  if (sessionKey) all.session_key = sessionKey;
  var form = [];
  for (var key in all) {
    var v = all[key];
    if (v === undefined || v === null) continue;
    form.push(key + "=" + encodeURIComponent(typeof v === "object" ? JSON.stringify(v) : String(v)));
  }
  var r = await send(step, {
    url: (base || OK_API) + "/fb.do",
    method: "POST",
    headers: msgHeaders({ "Content-Type": "application/x-www-form-urlencoded" }),
    body: form.join("&")
  });
  var j = parse(r.body);
  if (r.status !== 200 || !j || j.error_msg || j.error_code) {
    var code = j && j.error_code ? Number(j.error_code) : 0;
    var why = j && j.error_msg ? " " + String(j.error_msg).slice(0, 120) : "";
    var e = new StepError(step, method + " failed: " + r.status + (code ? " code " + code : "") + why, r.status);
    e.code = code;
    e.expired = (method === "auth.anonymLogin" && code === 401);
    throw e;
  }
  return j;
}

// ---- guest ----

async function guestCredentials(link, rep) {
  var ts = Math.floor(Date.now() / 1000);

  // 1. Optional config fetch (ignore errors)
  try {
    await http.fetch({
      url: BASE_URL + "/myteam-config.json?r=" + ts + "&domain=biz.mail.ru&email=guest@biz.mail.ru",
      headers: { "User-Agent": UA }
    });
  } catch (e) {}

  // 2. Auth as guest
  var guestName = GUEST_NAME;
  var devId = newDeviceID();

  var authResp = await send("guest", {
    url: BASE_URL + "/api/" + API_VERSION + "/rapi/auth/guest",
    method: "POST",
    headers: msgHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({
      reqId: "78200-" + ts,
      params: {
        name: guestName,
        scopes: ["calls/" + link]
      }
    })
  });
  if (authResp.status !== 200) throw new StepError("guest", "auth guest failed: " + authResp.status + " " + authResp.body, authResp.status);
  var guestData = parse(authResp.body);
  if (!guestData || !guestData.results || !guestData.results.token) {
    throw new StepError("guest", "auth guest: no token in the answer " + shape(guestData, 2), 200);
  }
  var sn = guestData.results.sn;
  var token = guestData.results.token;

  // 3. Start WIM session
  var sessionParams = "ts=" + ts +
    "&a=" + encodeURIComponent(token) +
    "&userSn=" + encodeURIComponent(sn) +
    "&k=ic1zmlWFTdkiTnkL&view=online&clientName=webVKTeams&language=en-US&deviceId=" + encodeURIComponent(devId) + "&sessionTimeout=2592000" +
    "&assertCaps=094613584C7F11D18222444553540000,0946135C4C7F11D18222444553540000" +
    "&events=myInfo,presence,webrtcMsg";

  var sessResp = await send("guest", {
    url: BASE_URL + "/api/" + API_VERSION + "/wim/aim/startSession",
    method: "POST",
    headers: msgHeaders({ "Content-Type": "application/x-www-form-urlencoded" }),
    body: sessionParams
  });
  if (sessResp.status !== 200) throw new StepError("guest", "start session failed: " + sessResp.status, sessResp.status);
  var sessData = parse(sessResp.body);
  var aimsid = sessData && sessData.response && sessData.response.data && sessData.response.data.aimsid;
  if (!aimsid) throw new StepError("guest", "start session: no aimsid in the answer " + shape(sessData, 2), 200);

  // 4. Get OK Calls auth token
  var tokenResp = await send("guest", {
    url: BASE_URL + "/api/" + API_VERSION + "/rapi/okcalls/getAuthToken",
    method: "POST",
    headers: msgHeaders({ "Content-Type": "application/json", "X-Teams-Aimsid": aimsid }),
    body: JSON.stringify({ reqId: "10711-" + ts, aimsid: aimsid })
  });
  if (tokenResp.status !== 200) throw new StepError("guest", "get auth token failed: " + tokenResp.status, tokenResp.status);
  var tokenData = parse(tokenResp.body);
  if (!tokenData || !tokenData.results || !tokenData.results.token) {
    throw new StepError("guest", "get auth token: no token in the answer " + shape(tokenData, 2), 200);
  }
  rep("guest", true, "");

  return { token: tokenData.results.token, devId: devId, apiBase: OK_API, anonym: true };
}

// ---- account ----

// The cookies of app.workspace.vk.ru, the browser's user agent and the account's name, packed into the one setting
// the wizard hands the ends.
function parseAccount(textValue) {
  if (!textValue) return null;
  var a = parse(String(textValue));
  if (!a || typeof a.c !== "string" || !a.c) return null;
  return { cookie: a.c, ua: typeof a.u === "string" ? a.u : "", name: typeof a.n === "string" ? a.n : "", email: "" };
}

function packAccount(acct) {
  return JSON.stringify({ c: acct.cookie, u: acct.ua, n: acct.name });
}

// whoName is what the superapp's "who am I" calls the person, for the wizard to show: they see their own name.
function whoName(info) {
  var s = "";
  if (typeof info.name === "string") {
    s = info.name;
  } else if (info.name && typeof info.name === "object") {
    s = [info.name.first, info.name.last].filter(Boolean).join(" ");
  }
  if (!s) s = [info.first_name, info.last_name].filter(Boolean).join(" ");
  if (!s) s = String(info.nick || info.display_name || info.displayName || "");
  return s.trim().slice(0, 80);
}

var acctAuth = null; // the signed-in account's calls access token (+ email, name), kept between attempts
var acctDevice = ""; // its device id: one per run, like a browser's

function accountDevice() {
  if (!acctDevice) acctDevice = newDeviceID();
  return acctDevice;
}

// sessionContext is what the superapp's messenger sends along with the silent token to start its session.
function sessionContext(deviceId, ts) {
  return {
    k: "webVKTeams",
    view: "online",
    clientName: "webVKTeams",
    language: "ru",
    deviceId: deviceId,
    sessionTimeout: 2592000,
    assertCaps: "094613584C7F11D18222444553540000,0946135C4C7F11D18222444553540000,0946135b4c7f11d18222444553540000," +
      "0946135E4C7F11D18222444553540000,AABC2A1AF270424598B36993C6231952,1f99494e76cbc880215d6aeab8e42268," +
      "A20C362CD4944B6EA3D1E77642201FD8,B5ED3E51C7AC4137B5926BC686E7A60D",
    interestCaps: "",
    subscriptions: "status,ownReactions",
    events: "myInfo,presence,buddylist,typing,hiddenChat,hist,mchat,sentIM,imState,dataIM,offlineIM," +
      "userAddedToBuddyList,service,lifestream,apps,permitDeny,diff,webrtcMsg",
    includePresenceFields: "aimId,displayId,friendly,friendlyName,state,userType,statusMsg,statusTime,lastseen,ssl," +
      "mute,counterEnabled,abContactName,abPhoneNumber,abPhones,official,quiet,autoAddition,largeIconId,nick,userState",
    ts: ts
  };
}

// accountAuth signs the account's cookies in the way the superapp's calls page does: "who am I" (is the sign-in alive,
// and what is the account's email), a calls-scoped silent token, then the Mail.ru SSO exchange that turns it into the
// access token calls/create takes. No messenger/WIM session is needed: a signed-in account joins calls as a guest; the
// account is used only to create them.
async function accountAuth(acct, rep) {
  if (acctAuth && acctAuth.cookie === acct.cookie && acctAuth.expiresAt - 60000 > Date.now()) {
    acct.email = acctAuth.email;
    if (!acct.name) acct.name = acctAuth.name;
    return acctAuth;
  }
  acctAuth = null;
  var agent = acct.ua || UA;
  var wsHeaders = function (post) {
    var h = {
      "User-Agent": agent,
      "Cookie": acct.cookie,
      "Accept": "application/json, text/plain, */*",
      "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
      "Referer": WS_ORIGIN + "/calls",
      "Cache-Control": "no-cache, no-store"
    };
    if (post) {
      h["Origin"] = WS_ORIGIN;
      h["Content-Type"] = "application/json";
    }
    return h;
  };

  // 1. Is the sign-in alive? (also gives the account's email, which calls/create needs.)
  var r = await send("userinfo", { url: WS_ORIGIN + "/api/v2/userinfo", headers: wsHeaders(false) });
  var info = parse(r.body);
  if (r.status !== 200 || !info || typeof info.email !== "string") {
    throw new StepError("userinfo", "userinfo answered " + r.status + (info ? " " + shape(info, 1) : " with a page, not an account"),
      r.status, r.status === 200 || r.status === 401 || r.status === 403 || r.status === 510);
  }
  acct.email = info.email;
  if (!acct.name) acct.name = whoName(info);
  rep("userinfo", true, "");

  // 2. A calls-scoped silent token from the superapp (targetID "calls", not "messenger").
  r = await send("silent", {
    url: WS_ORIGIN + "/api/v1/silent_token",
    method: "POST",
    headers: wsHeaders(true),
    body: JSON.stringify({ clientID: WS_CLIENT_ID, targetID: "calls", grantType: "ws_app_silent_token" })
  });
  var st = parse(r.body);
  if (r.status !== 200 || !st || typeof st.silent_token !== "string" || !st.silent_token) {
    throw new StepError("silent", "silent_token answered " + r.status + " " + shape(st, 1), r.status, r.status === 401);
  }
  rep("silent", true, "");

  // 3. Exchange it at the Mail.ru SSO for the calls access token (the one calls/create takes).
  var sr = await send("session", {
    url: SSO_ORIGIN + "/auth/api/v1/silent_auth/sso?silent_token=" + encodeURIComponent(st.silent_token) +
      "&resource=" + encodeURIComponent(SSO_RESOURCE),
    headers: { "User-Agent": agent, "Accept": "*/*", "Origin": CALLS_ORIGIN, "Referer": CALLS_ORIGIN + "/" }
  });
  var sj = parse(sr.body);
  var at = sj && sj.body && sj.body.access_token;
  if (sr.status !== 200 || !sj || sj.status !== 200 || typeof at !== "string" || !at) {
    throw new StepError("session", "silent_auth answered " + sr.status + " " + (sj ? shape(sj, 2) : "no body"),
      sr.status, sr.status === 401 || sr.status === 403);
  }
  var life = sj.body && typeof sj.body.expires_in === "number" && sj.body.expires_in > 0 ? sj.body.expires_in * 1000 : 12 * 60 * 60 * 1000;
  acctAuth = { cookie: acct.cookie, accessToken: at, email: acct.email, name: acct.name, expiresAt: Date.now() + life };
  rep("session", true, "");
  return acctAuth;
}

// sessionHeaders are the two headers a session goes in: set 0 both, set 1 the aimsid only, set 2 the bearer only.
function sessionHeaders(sess, set) {
  var h = msgHeaders({ "Content-Type": "application/json", "Accept": "application/json" });
  if (set !== 2) h["X-Teams-Aimsid"] = sess.aimsid;
  if (set !== 1) h["Authorization"] = "Bearer " + sess.bearer;
  return h;
}

var SESSION_REFUSED = [401, 403, 460, 40100, 40107, 40200, 40300];

// rapi calls the messenger backend's REST layer the way its web clients do: {reqId, aimsid, params} and the session in
// headers. The first set of headers the service takes is kept for the calls after it.
async function rapi(step, sess, endpoint, params) {
  var sets = sess.hdr >= 0 ? [sess.hdr] : [0, 1, 2];
  var last = null;
  var refused = false;
  for (var i = 0; i < sets.length; i++) {
    var body = { reqId: newDeviceID(), aimsid: sess.aimsid };
    if (params) body.params = params;
    var r = await send(step, {
      url: sess.host + "/api/" + API_VERSION + "/rapi/" + endpoint,
      method: "POST",
      headers: sessionHeaders(sess, sets[i]),
      body: JSON.stringify(body)
    });
    var j = parse(r.body);
    var full = j && j.status && typeof j.status.code === "number" ? j.status.code : r.status;
    if (r.status === 200 && j && (full === 200 || Math.floor(full / 100) === 200)) {
      sess.hdr = sets[i];
      return j.results || {};
    }
    if (SESSION_REFUSED.indexOf(full) !== -1 || SESSION_REFUSED.indexOf(r.status) !== -1) refused = true;
    last = new StepError(step, endpoint + " answered " + r.status + " code " + (j && j.status ? j.status.code : "-") + " " + shape(j && j.results, 1), r.status);
  }
  last.expired = refused;
  throw last;
}

// apiBaseOf is the calls API's address the messenger names for this account, or the usual one.
function apiBaseOf(server) {
  var m = /^(https:\/\/[A-Za-z0-9.-]+(?::\d+)?)(?:\/|$)/.exec(String(server || ""));
  return m ? m[1] : OK_API;
}

// (An account no longer gets its own calls token: it creates a call via the SSO — see createCall — then joins as a
// guest, the same chain guestCredentials runs. The messenger-session helpers above it are kept but unused.)

// ---- joining and creating ----

// callsLogin logs in to the calls API with the token: the session key every call after it carries.
async function callsLogin(cred, rep) {
  var login = await okApi("login", "auth.anonymLogin", {
    session_data: { version: 3, device_id: cred.devId, client_version: 1.1, client_type: "SDK_JS", auth_token: cred.token }
  }, "", cred.apiBase);
  if (!login.session_key) throw new StepError("login", "auth.anonymLogin: no session_key in the answer " + shape(login, 1), 200);
  rep("login", true, cred.apiBase === OK_API ? "" : hostName(cred.apiBase));
  return login.session_key;
}

// joinCall joins the call by its link and returns what the service says about it (the signaling address, the servers).
async function joinCall(link, cred, rep) {
  var sessionKey = await callsLogin(cred, rep);
  var jp = { joinLink: link, isVideo: false, protocolVersion: 6, capabilities: "253F" };
  if (cred.anonym) jp.anonymToken = cred.token;
  var conv;
  try {
    conv = await okApi("join", "vchat.joinConversationByLink", jp, sessionKey, cred.apiBase);
  } catch (e) {
    if (cred.anonym || !(e instanceof StepError) || e.network) throw e;
    // An account is let in by its session; if the service wants its token as well, give it.
    rep("join", false, errText(e));
    jp.anonymToken = cred.token;
    conv = await okApi("join", "vchat.joinConversationByLink", jp, sessionKey, cred.apiBase);
  }
  rep("join", true, "");
  return conv;
}

// authenticateAndJoin is a chain from nothing to a join. The calls layer is guest for everyone — a signed-in account
// joins its own call as a guest too — so joining is always the guest chain; the account matters only for createCall.
// (acct is accepted for call-site compatibility and is not used here.)
async function authenticateAndJoin(link, rep, acct) {
  rep = rep || newReport().rep;
  return joinCall(link, await guestCredentials(link, rep), rep);
}

// createCall makes a call as the signed-in account: a calls access token via the SSO (accountAuth), then the Mail.ru
// calls API's create. It returns the join link code; the other end — and this one — join it as guests. The call this
// path makes is open to guests, so `closed` is not honoured here (kept for call-site compatibility).
async function createCall(acct, closed, rep) {
  var auth;
  for (var tries = 0; ; tries++) {
    try {
      auth = await accountAuth(acct, rep);
      break;
    } catch (e) {
      if (tries > 0 || !(e instanceof StepError) || !e.expired) throw e;
      acctAuth = null;
    }
  }
  var cr = await send("create", {
    url: SSO_ORIGIN + "/api/v1/calls/create",
    method: "POST",
    headers: {
      "User-Agent": acct.ua || UA,
      "Accept": "*/*",
      "Content-Type": "application/json",
      "Origin": CALLS_ORIGIN,
      "Referer": CALLS_ORIGIN + "/"
    },
    body: JSON.stringify({ access_token: auth.accessToken, email: acct.email })
  });
  var cj = parse(cr.body);
  var code = cj && cj.body && cj.body.call_id;
  if (cr.status !== 200 || !cj || cj.status !== 200 || typeof code !== "string" || !/^[\w-]{4,128}$/.test(code)) {
    throw new StepError("create", "calls/create answered " + cr.status + " " + (cj ? shape(cj, 2) : "no body"), cr.status);
  }
  rep("create", true, "");
  return { code: code };
}

function buildIceServers(convData) {
  var iceServers = [];
  if (convData.turn_server && convData.turn_server.urls && convData.turn_server.urls.length > 0) {
    iceServers.push({
      urls: convData.turn_server.urls,
      username: convData.turn_server.username,
      credential: convData.turn_server.credential
    });
  }
  if (!forceRelay && convData.stun_server && convData.stun_server.urls && convData.stun_server.urls.length > 0) {
    iceServers.push({
      urls: convData.stun_server.urls
    });
  }
  return iceServers;
}

// pcOptions is what both ends' PeerConnections are made with: the call's relay, and what the core does about a
// path that drops in bursts (see tuneConnection in transport/script).
function pcOptions(convData) {
  var o = {
    iceServers: buildIceServers(convData),
    sctpMtu: SCTP_MTU,        // one packet of the tunnel is one datagram
    sctpRtoMaxMs: 3000,       // SCTP's retransmission timer backs off to 60 s by default: the channel is dead meanwhile
    sctpMinCwnd: 524288,      // the TCP inside has its own congestion control; SCTP's would only slow it a second time
    paceKBps: 3000,           // datagrams leave one by one, not as the dozen SCTP releases on an acknowledgement
    paceQueue: 64,            // and what waits longer than that is dropped (the signal the TCP inside reacts to)
    dcBufKB: 512              // as is what the channel itself holds
  };
  if (forceRelay) o.iceTransportPolicy = "relay";
  return o;
}

function flushPendingCandidates() {
  if (!pc || !haveRemoteDesc) return;
  for (var i = 0; i < pendingCandidates.length; i++) {
    try {
      pc.addIceCandidate(pendingCandidates[i]);
    } catch (e) {}
  }
  pendingCandidates = [];
}

async function initiateClientOffer() {
  if (!running || !callConfig) return;
  console.log("[VKTEAMS] client creating offer...");
  resetCallState();

  myPeerId = "cli-" + Math.floor(Math.random() * 1000000000);
  targetPeerId = "";

  pc = webrtc.newPeerConnection(pcOptions(callConfig));
  offerEpoch = Date.now();
  var epoch = offerEpoch;

  pc.onicecandidate = function (c) {
    if (c) {
      if (forceRelay && c.candidate && c.candidate.indexOf("typ relay") === -1) {
        return;
      }
      sendCustomData({
        type: "ice-candidate",
        from: "client",
        clientId: myPeerId,
        targetExitId: targetPeerId,
        candidate: JSON.stringify(c),
        epoch: epoch
      });
    }
  };

  pc.oniceconnectionstatechange = function (s) {
    console.log("[VKTEAMS] client iceConnectionState: " + s);
  };

  pc.onconnectionstatechange = function (s) {
    console.log("[VKTEAMS] client pc connectionState: " + s);
    if (s === "connected") {
      clearReconnTimer();
      setState("connected");
    } else if (s === "failed" || s === "closed") {
      forceReconnect("pc " + s);
    } else if (s === "disconnected") {
      setState("reconnecting", "pc " + s);
      scheduleReconnect("pc disconnected", 4000);
    }
  };

  dc = pc.createDataChannel("flux-tunnel", { ordered: false, maxRetransmits: 0 });
  dc.onopen = function () {
    console.log("[VKTEAMS] client dc opened!");
    setState("connected");
    if (offerTimer) {
      clearInterval(offerTimer);
      offerTimer = null;
    }
  };
  dc.onclose = function () {
    console.log("[VKTEAMS] client dc closed!");
    forceReconnect("datachannel closed");
  };
  dc.onmessage = function (bytes) {
    emit(bytes);
  };

  var offer = await pc.createOffer();
  await pc.setLocalDescription("offer", offer);

  console.log("[VKTEAMS] client sending sdp-offer, clientId=" + myPeerId);
  sendCustomData({
    type: "sdp-offer",
    from: "client",
    clientId: myPeerId,
    sdp: offer,
    epoch: epoch
  });

  if (!offerTimer) {
    offerTimer = setInterval(function () {
      if (!running || (dc && dc.readyState === "open")) {
        if (offerTimer) {
          clearInterval(offerTimer);
          offerTimer = null;
        }
        return;
      }
      console.log("[VKTEAMS] client re-sending sdp-offer");
      sendCustomData({
        type: "sdp-offer",
        from: "client",
        clientId: myPeerId,
        sdp: offer,
        epoch: epoch
      });
    }, 2000);
  }
}

async function handleRemoteOffer(d, epoch) {
  if (!running || !callConfig) return;
  var offerSDP = d.sdp;
  var fromClientId = d.clientId;
  if (!fromClientId) return;

  // A different client while one is already set. A reconnected client (e.g. after a Wi-Fi<->LTE jump) comes back with a
  // fresh peer id AND a newer epoch — let it take over. Only a genuinely concurrent second client (epoch not newer) is
  // turned away, so its offer can't knock the established peer off.
  if (targetPeerId && targetPeerId !== fromClientId && pc && epoch <= offerEpoch) {
    console.log("[VKTEAMS] exit ignoring a second client " + fromClientId + " (epoch " + epoch + " <= " + offerEpoch + ")");
    return;
  }

  targetPeerId = fromClientId;
  if (!myPeerId) {
    myPeerId = "exit-" + Math.floor(Math.random() * 1000000000);
  }

  console.log("[VKTEAMS] exit handling remote offer from " + fromClientId + ", epoch=" + epoch);
  if (pc && epoch === offerEpoch) {
    if (localAnswer) {
      console.log("[VKTEAMS] exit re-sending sdp-answer");
      sendCustomData({
        type: "sdp-answer",
        from: "exit",
        clientId: targetPeerId,
        exitId: myPeerId,
        sdp: localAnswer,
        epoch: epoch
      });
    }
    return;
  }
  if (epoch < offerEpoch) return;

  resetCallState();
  offerEpoch = epoch;

  pc = webrtc.newPeerConnection(pcOptions(callConfig));

  pc.onicecandidate = function (c) {
    if (c) {
      if (forceRelay && c.candidate && c.candidate.indexOf("typ relay") === -1) {
        return;
      }
      sendCustomData({
        type: "ice-candidate",
        from: "exit",
        clientId: targetPeerId,
        exitId: myPeerId,
        candidate: JSON.stringify(c),
        epoch: epoch
      });
    }
  };

  pc.oniceconnectionstatechange = function (s) {
    console.log("[VKTEAMS] exit iceConnectionState: " + s);
  };

  pc.onconnectionstatechange = function (s) {
    console.log("[VKTEAMS] exit pc connectionState: " + s);
    if (s === "connected") {
      setState("connected");
    } else if (s === "failed" || s === "closed") {
      exitDropConnection("pc " + s);
    }
  };

  pc.ondatachannel = function (remoteDC) {
    console.log("[VKTEAMS] exit ondatachannel received!");
    dc = remoteDC;
    dc.onopen = function () {
      console.log("[VKTEAMS] exit dc opened!");
      setState("connected");
    };
    dc.onclose = function () {
      console.log("[VKTEAMS] exit dc closed!");
      exitDropConnection("datachannel closed");
    };
    dc.onmessage = function (bytes) {
      emit(bytes);
    };
    if (dc.readyState === "open") {
      console.log("[VKTEAMS] exit dc readyState is open immediately!");
      setState("connected");
    }
  };

  await pc.setRemoteDescription("offer", offerSDP);
  haveRemoteDesc = true;
  flushPendingCandidates();

  var answer = await pc.createAnswer();
  await pc.setLocalDescription("answer", answer);
  localAnswer = answer;

  var ufragExit = (answer.match(/a=ice-ufrag:(\S+)/) || [])[1];
  console.log("[VKTEAMS] exit sending sdp-answer to " + targetPeerId + ", ufrag=" + ufragExit);
  sendCustomData({
    type: "sdp-answer",
    from: "exit",
    clientId: targetPeerId,
    exitId: myPeerId,
    sdp: answer,
    epoch: epoch
  });
}

async function handleRemoteAnswer(answerSDP, epoch) {
  if (!pc || haveRemoteDesc) return;
  var ufragAns = (answerSDP.match(/a=ice-ufrag:(\S+)/) || [])[1];
  console.log("[VKTEAMS] client handling remote answer, ufrag=" + ufragAns);
  haveRemoteDesc = true;
  if (offerTimer) {
    clearInterval(offerTimer);
    offerTimer = null;
  }
  try {
    await pc.setRemoteDescription("answer", answerSDP);
    flushPendingCandidates();
  } catch (e) {
    console.log("[VKTEAMS] client setRemoteDescription answer error: " + String(e));
    setState("degraded", "answer error: " + String(e));
  }
}

function handleRemoteCandidate(candData, epoch) {
  try {
    if (epoch && epoch !== offerEpoch) return;
    var c = typeof candData === "string" ? JSON.parse(candData) : candData;
    if (!c || !c.candidate) return;
    if (forceRelay && c.candidate.indexOf("typ relay") === -1) {
      return;
    }
    console.log("[VKTEAMS] " + (isExit ? "exit" : "client") + " applying remote candidate (" + ((/ typ (\w+)/.exec(c.candidate) || [])[1] || "?") + ")");
    if (pc && haveRemoteDesc) {
      pc.addIceCandidate(c);
    } else {
      pendingCandidates.push(c);
    }
  } catch (e) {}
}

function handleCustomData(d) {
  if (!d) return;
  var typ = d.type;
  var from = d.from;
  var epoch = d.epoch || 0;
  console.log("[VKTEAMS] handleCustomData: typ=" + typ + " from=" + from + " isExit=" + isExit);

  if (isExit) {
    if (from !== "client") return;
    if (typ === "sdp-offer") {
      if (offerEpoch !== 0 && epoch < offerEpoch) {
        console.log("[VKTEAMS] exit dropping stale offer (epoch " + epoch + " < " + offerEpoch + ")");
        return;
      }
      handleRemoteOffer(d, epoch).catch(function (e) {
        console.log("[VKTEAMS] handleRemoteOffer error: " + String(e));
        setState("degraded", "handle offer: " + String(e));
      });
    } else if (typ === "ice-candidate") {
      if (targetPeerId && d.clientId !== targetPeerId) return;
      handleRemoteCandidate(d.candidate, epoch);
    }
  } else {
    if (from !== "exit") return;
    if (d.clientId !== myPeerId) return;
    if (typ === "sdp-answer") {
      targetPeerId = d.exitId;
      handleRemoteAnswer(d.sdp, epoch).catch(function (e) {
        console.log("[VKTEAMS] handleRemoteAnswer error: " + String(e));
        setState("degraded", "handle answer: " + String(e));
      });
    } else if (typ === "ice-candidate") {
      if (targetPeerId && d.exitId !== targetPeerId) return;
      handleRemoteCandidate(d.candidate, epoch);
    }
  }
}

function handleSocketMessage(msg) {
  var t = typeof msg === "string" ? msg : text.decode(msg);
  if (t === "ping") {
    try { wsConn.send("pong"); } catch (e) {}
    return;
  }
  var data;
  try {
    data = JSON.parse(t);
  } catch (e) {
    return;
  }
  if (data.type === "notification" && data.notification === "custom-data") {
    handleCustomData(data.data);
  }
}

async function connectWebSocket(endpoint) {
  var wsURL = endpoint + "&platform=WEB&appVersion=1.1&version=6&device=browser&capabilities=253F&clientType=VK_TEAMS&tgt=join";
  console.log("[VKTEAMS] opening ws: " + hostOf(wsURL));
  var sock = await ws.open(wsURL, {
    "Origin": "https://myteam.mail.ru",
    "User-Agent": UA
  }, { readTimeoutMs: 0 });

  wsConn = sock;
  console.log("[VKTEAMS] ws connected!");
  sock.onmessage = handleSocketMessage;
  sock.onclose = function () {
    console.log("[VKTEAMS] ws closed!");
    wsConn = null;
    if (!running) return;
    setState("reconnecting", "websocket closed");
    retryLater(2000);
  };
}

async function connectOnce() {
  console.log("[VKTEAMS] connectOnce: authenticateAndJoin starting...");
  callConfig = await authenticateAndJoin(joinLink, newReport().rep, account);
  console.log("[VKTEAMS] connectOnce: authenticateAndJoin success!");
  await connectWebSocket(callConfig.endpoint);

  if (!isExit) {
    setTimeout(function () {
      if (running) {
        initiateClientOffer().catch(function (e) {
          console.log("[VKTEAMS] initiateClientOffer error: " + String(e));
          setState("degraded", "offer error: " + String(e));
        });
      }
    }, 1000);
  }
}

// ===== Setup wizard =====
//
// Transport.setup (an app runs it while a node is being made) and open() without a link both serve the wizard page
// on a loopback port and ask the app to show it. The page checks the call here (/api/check) and hands back
// {client, node} through window.openfluxSubmit; the answer for this side returns as cookiesApplied.

var SETUP_HTML = require("./setup_html.js");
var wizardServer = null;

function jsonResponse(obj) {
  return { status: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(obj) };
}

// The account the person signed in with in the wizard, for its later calls (the call check, "create a call").
var wizardAccount = null;

function readBody(req) {
  return parse(text.decode(req.body)) || {};
}

// failureAnswer turns a chain's failure into the wizard's answer: the words for the person, what kind of failure it
// was and the steps so far. The words say the step and the status, never what the service said.
function failureAnswer(e, report, asAccount) {
  var out = { ok: false, steps: report.steps };
  if (!(e instanceof StepError)) {
    out.error = "Ошибка в скрипте: " + errText(e);
    report.rep("script", false, errText(e));
    return out;
  }
  report.rep(e.step, false, e.message.slice(0, 160));
  var st = e.status ? " (" + e.status + ")" : "";
  if (e.network) {
    out.network = true;
    out.error = "Нет связи с VK Teams: " + e.message.replace(/^no connection: /, "").slice(0, 160);
  } else if (e.auth) {
    out.auth = true;
    out.error = "Вход в аккаунт не подтвердился" + st + ": войдите заново";
  } else if (e.step === "guest") {
    out.error = "VK Teams отказал в гостевом входе" + st + ". Если звонки для гостей выключены, войдите аккаунтом";
  } else if (e.step === "join") {
    if (e.code === 300 || e.status === 404) {
      out.error = "Звонок не найден или уже закончился" + (e.code ? " (код " + e.code + ")" : st);
    } else if (asAccount) {
      out.error = "VK Teams не пустил этот аккаунт в звонок" + (e.code ? " (код " + e.code + ")" : st);
    } else {
      out.error = "VK Teams не пустил гостя в звонок" + (e.code ? " (код " + e.code + ")" : st) + ". Если он закрыт для гостей, войдите аккаунтом";
    }
  } else {
    out.error = "VK Teams отказал на шаге «" + (STEP_LABELS[e.step] || e.step) + "»" + (e.code ? " (код " + e.code + ")" : st);
  }
  return out;
}

// checkCall answers the wizard's "does this call exist": the sign-in chain the transport starts with (the guest's, or the
// account's the wizard has), up to joining the conversation by its link. No signaling socket is opened, so the check
// leaves nothing in the call.
function checkCall(req) {
  var body = readBody(req);
  var link = extractJoinLink(String(body.url || "").trim());
  if (!/^[\w-]{4,128}$/.test(link)) {
    return jsonResponse({ ok: false, error: "В ссылке нет кода звонка: нужна ссылка вида https://calls.myteam.mail.ru/join/…" });
  }
  var acct = body.mode === "account" ? wizardAccount : null;
  if (body.mode === "account" && !acct) {
    return jsonResponse({ ok: false, auth: true, error: "Сначала войдите в аккаунт" });
  }
  var report = newReport();
  return authenticateAndJoin(link, report.rep, acct).then(function (conv) {
    if (!conv || !conv.endpoint) {
      return jsonResponse({ ok: false, steps: report.steps, error: "VK Teams не вернул адрес звонка: он закончился?" });
    }
    var turn = !!(conv.turn_server && conv.turn_server.urls && conv.turn_server.urls.length);
    var stun = !!(conv.stun_server && conv.stun_server.urls && conv.stun_server.urls.length);
    return jsonResponse({ ok: true, turn: turn, stun: stun, code: link.slice(0, 4) + "…", as: acct ? "account" : "guest", steps: report.steps });
  }).catch(function (e) {
    return jsonResponse(failureAnswer(e, report, !!acct));
  });
}

// takeAccount answers the wizard's "here is what the sign-in window found": it runs the account's chain up to the calls
// token and, when that works, keeps the account for the wizard's later calls and hands it back packed, for the settings.
function takeAccount(req) {
  var body = readBody(req);
  var cap = parse(String(body.capture || ""));
  if (!cap || typeof cap.cookie !== "string" || !cap.cookie) {
    return jsonResponse({ ok: false, error: "Окно входа ничего не вернуло: войдите ещё раз" });
  }
  var acct = { cookie: cap.cookie, ua: typeof cap.ua === "string" ? cap.ua : "", name: "", email: "" };
  var probe = parse(String(cap.probe || ""));
  if (probe) acct.name = whoName(probe);
  var report = newReport();
  wizardAccount = null;
  acctAuth = null;
  return accountAuth(acct, report.rep).then(function () {
    wizardAccount = acct;
    return jsonResponse({ ok: true, name: acct.name, email: acct.email, account: packAccount(acct), steps: report.steps });
  }).catch(function (e) {
    return jsonResponse(failureAnswer(e, report, true));
  });
}

// makeCall answers the wizard's "create a call": a call with a join link, made as the account.
function makeCall(req) {
  var body = readBody(req);
  if (!wizardAccount) return jsonResponse({ ok: false, auth: true, error: "Сначала войдите в аккаунт" });
  var report = newReport();
  return createCall(wizardAccount, !!body.closed, report.rep).then(function (made) {
    return jsonResponse({ ok: true, url: CALL_HOST + "/join/" + made.code, steps: report.steps });
  }).catch(function (e) {
    return jsonResponse(failureAnswer(e, report, true));
  });
}

// startWizard serves the setup page and asks the app to show it. values are what is known (values.url prefills the
// link, values.relay the option).
function startWizard(values) {
  if (wizardServer) return;
  var v = values || {};
  var init = { url: String(v.url || ""), relay: v.relay === "true" || v.relay === true };
  var page = SETUP_HTML.replace(
    '<script id="init" type="application/json">{}</script>',
    '<script id="init" type="application/json">' + JSON.stringify(init).replace(/</g, "\\u003c") + "</script>"
  );
  wizardServer = httpserver.listen(function (req) {
    if (req.path === "/" || req.path === "") {
      return { status: 200, headers: { "Content-Type": "text/html; charset=utf-8" }, body: page };
    }
    if (req.method === "POST") {
      if (req.path === "/api/check") return checkCall(req);
      if (req.path === "/api/account") return takeAccount(req);
      if (req.path === "/api/create") return makeCall(req);
    }
    return { status: 404, body: "not found" };
  });
  raise("needsSetup", {
    url: "http://" + wizardServer.addr + "/",
    reason: "Подключение через VK Teams"
  });
}

// ===== Transport Interface =====

var urlFromParams = false;

// readSettings takes the settings the transport runs with, from cfg.params or from what a setup page left in the
// cookie jar. It returns the call's join code.
function readSettings(p, link) {
  p = p || {};
  joinLink = extractJoinLink(String(link || p.url || "").trim());
  forceRelay = (p.relay === "true" || p.relay === true);
  accountText = String(p.account || "");
  account = parseAccount(accountText);
  if (accountText && !account) console.log("[VKTEAMS] the account setting is damaged: joining as a guest");
  return joinLink;
}

// attempt signs in and joins once; a failure is told to the core and the attempt is made again after a while, for as
// long as the transport runs (1.0.0 made a second one and stopped).
function attempt() {
  if (!running) return;
  if (attempting) {
    retryLater(1000);
    return;
  }
  attempting = true;
  connectOnce().then(function () {
    attempting = false;
    retryCount = 0;
  }, function (e) {
    attempting = false;
    if (!running) return;
    console.log("[VKTEAMS] attempt failed" + (e && e.step ? " at " + e.step : "") + ": " + errText(e));
    if (e && e.auth) {
      // The account's sign-in is gone: trying again changes nothing, the person has to sign in again.
      setState("dead", "вход в аккаунт VK Teams больше не действует: пройдите мастер заново");
      return;
    }
    setState("reconnecting", String(e));
    retryCount++;
    retryLater(Math.min(3000 * retryCount, 30000));
  });
}

function retryLater(ms) {
  if (retryTimer || !running) return;
  retryTimer = setTimeout(function () {
    retryTimer = null;
    attempt();
  }, ms);
}

// launch signs in and joins.
function launch() {
  running = true;
  retryCount = 0;
  setState("connecting");
  attempt();
}

// stopSocket lets go of the signaling socket without the reconnect its close would start.
function stopSocket() {
  if (!wsConn) return;
  var sock = wsConn;
  wsConn = null;
  sock.onclose = null;
  sock.onmessage = null;
  try { sock.close(); } catch (e) {}
}

var Transport = {
  info: function () {
    return {
      name: "vkteams",
      version: "1.3.0",
      mtu: 0,
      carrierMtu: CARRIER_MTU,
      batchBytes: BATCH_BYTES,
      reliable: false,
      ordered: false,
      // All three are settings, none is the profile's one input: the wizard hands each end its values, and a profile
      // field would be wiped by it. Which end this is the core says itself (cfg.params.exit).
      params: [
        { key: "url", label: "Ссылка на звонок VK Teams", type: "url", required: true, scope: "settings",
          placeholder: "https://calls.myteam.mail.ru/join/…",
          description: "Клиент и нода заходят в этот звонок." },
        { key: "relay", label: "Только через TURN", type: "boolean", scope: "settings", advanced: true, default: "false",
          description: "Весь трафик пойдёт через сервер VK: медленнее, но проходит там, где прямой путь закрыт." },
        { key: "account", label: "Вход аккаунтом VK Teams", type: "secret", scope: "settings", advanced: true,
          description: "Его записывает мастер после входа. Пусто: в звонок заходят гостем." }
      ]
    };
  },

  // setup is the wizard an app runs while a node is being made (before anything connects): it checks the call and
  // hands back {client: {...}, node: {...}} through openfluxSubmit.
  setup: function (values) {
    startWizard(values || {});
  },

  open: function (cfg) {
    var p = (cfg && cfg.params) || {};
    isExit = (p.exit === "true" || p.exit === true || p.role === "exit");
    urlFromParams = !!readSettings(p, cfg && cfg.url);
    if (!joinLink) {
      // Nothing in the settings: what an earlier setup page left for this profile.
      var saved = cookieJar.get() || {};
      if (saved.url) readSettings(saved, "");
    }
    if (!joinLink) {
      if (isExit) {
        // A node has no one to ask: it needs the link its setup gave it.
        setState("dead", "у ноды нет ссылки на звонок: настройте ноду заново");
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
  onEvent: function (kind) {
    if (kind !== "cookiesApplied" || !running || urlFromParams) return;
    var c = cookieJar.get() || {};
    var link = extractJoinLink(String(c.url || "").trim());
    if (!link || (link === joinLink && String(c.account || "") === accountText)) return;
    resetCallState();
    stopSocket();
    callConfig = null;
    myPeerId = "";
    targetPeerId = "";
    offerEpoch = 0;
    readSettings(c, "");
    launch();
  },

  write: function (bytes) {
    if (!dc) {
      throw new Error("data channel not ready");
    }
    dc.send(bytes);
  },

  close: function () {
    running = false;
    setState("dead");
    if (retryTimer) {
      clearTimeout(retryTimer);
      retryTimer = null;
    }
    attempting = false;
    if (wizardServer) {
      try { wizardServer.close(); } catch (e) {}
      wizardServer = null;
    }
    wizardAccount = null;
    resetCallState();
    stopSocket();
  }
};
