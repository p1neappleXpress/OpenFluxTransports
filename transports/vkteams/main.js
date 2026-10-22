// OpenFlux script transport for VK Teams calls (WebRTC DataChannel over VK Calls room).
//
// Establishes a high-speed, low-latency P2P/TURN WebRTC DataChannel between peers
// using the VK Teams calls signaling WebSocket (custom-data command).
//
// Params:
//   url:   VK Teams call join link (e.g. https://calls.myteam.mail.ru/join/...)
//   relay: true to use the call's TURN server only
//   exit:  set by the core: true on the node (listener), false on the client (initiator)
//
// Written by trader52. OpenFlux's changes on top of 1.0.0 (the wire is untouched: ends of both versions talk):
//   - the guest is called "Гость" and has an ordinary device id (the service shows both to the other participants);
//   - the settings are declared as settings, none is a profile field (the wizard fills them, both ends alike);
//   - Transport.setup: a wizard page that checks the call link and hands back {client, node};
//   - the log carries no signaling address and no candidate text.

var BASE_URL = "https://u.myteam.vmailru.net";
var API_VERSION = "v149";
var GUEST_NAME = "Гость";
var UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";

var running = false;
var isExit = false;
var joinLink = "";
var callConfig = null;

// Network state
var wsConn = null;
var wsSeq = 1;
var pc = null;
var dc = null;
var offerTimer = null;
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

async function authenticateAndJoin(link) {
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

  var authResp = await http.fetch({
    url: BASE_URL + "/api/" + API_VERSION + "/rapi/auth/guest",
    method: "POST",
    headers: {
      "User-Agent": UA,
      "Content-Type": "application/json",
      "Origin": "https://myteam.mail.ru",
      "Referer": "https://myteam.mail.ru/"
    },
    body: JSON.stringify({
      reqId: "78200-" + ts,
      params: {
        name: guestName,
        scopes: ["calls/" + link]
      }
    })
  });
  if (authResp.status !== 200) throw new Error("auth guest failed: " + authResp.status + " " + authResp.body);
  var guestData = JSON.parse(authResp.body);
  var sn = guestData.results.sn;
  var token = guestData.results.token;

  // 3. Start WIM session
  var sessionParams = "ts=" + ts +
    "&a=" + encodeURIComponent(token) +
    "&userSn=" + encodeURIComponent(sn) +
    "&k=ic1zmlWFTdkiTnkL&view=online&clientName=webVKTeams&language=en-US&deviceId=" + encodeURIComponent(devId) + "&sessionTimeout=2592000" +
    "&assertCaps=094613584C7F11D18222444553540000,0946135C4C7F11D18222444553540000" +
    "&events=myInfo,presence,webrtcMsg";

  var sessResp = await http.fetch({
    url: BASE_URL + "/api/" + API_VERSION + "/wim/aim/startSession",
    method: "POST",
    headers: {
      "User-Agent": UA,
      "Content-Type": "application/x-www-form-urlencoded",
      "Origin": "https://myteam.mail.ru",
      "Referer": "https://myteam.mail.ru/"
    },
    body: sessionParams
  });
  if (sessResp.status !== 200) throw new Error("start session failed: " + sessResp.status);
  var sessData = JSON.parse(sessResp.body);
  var aimsid = sessData.response.data.aimsid;

  // 4. Get OK Calls auth token
  var tokenResp = await http.fetch({
    url: BASE_URL + "/api/" + API_VERSION + "/rapi/okcalls/getAuthToken",
    method: "POST",
    headers: {
      "User-Agent": UA,
      "Content-Type": "application/json",
      "X-Teams-Aimsid": aimsid,
      "Origin": "https://myteam.mail.ru",
      "Referer": "https://myteam.mail.ru/"
    },
    body: JSON.stringify({ reqId: "10711-" + ts, aimsid: aimsid })
  });
  if (tokenResp.status !== 200) throw new Error("get auth token failed: " + tokenResp.status);
  var tokenData = JSON.parse(tokenResp.body);
  var callsApiToken = tokenData.results.token;

  // 5. Calls anonym login
  var sessionDataJSON = JSON.stringify({
    version: 3,
    device_id: devId,
    client_version: 1.1,
    client_type: "SDK_JS",
    auth_token: callsApiToken
  });

  var loginParams = "session_data=" + encodeURIComponent(sessionDataJSON) +
    "&method=auth.anonymLogin&format=JSON&application_key=CIAPPDKGDIHBABABA";

  var loginResp = await http.fetch({
    url: "https://calls.okcdn.ru/fb.do",
    method: "POST",
    headers: {
      "User-Agent": UA,
      "Content-Type": "application/x-www-form-urlencoded",
      "Origin": "https://myteam.mail.ru",
      "Referer": "https://myteam.mail.ru/"
    },
    body: loginParams
  });
  if (loginResp.status !== 200) throw new Error("calls anonymLogin failed: " + loginResp.status);
  var loginData = JSON.parse(loginResp.body);

  // 6. Join conversation by link
  var joinParams = "joinLink=" + encodeURIComponent(link) +
    "&isVideo=false&protocolVersion=6&capabilities=253F" +
    "&anonymToken=" + encodeURIComponent(callsApiToken) +
    "&method=vchat.joinConversationByLink&format=JSON&application_key=CIAPPDKGDIHBABABA" +
    "&session_key=" + encodeURIComponent(loginData.session_key);

  var joinResp = await http.fetch({
    url: "https://calls.okcdn.ru/fb.do",
    method: "POST",
    headers: {
      "User-Agent": UA,
      "Content-Type": "application/x-www-form-urlencoded",
      "Origin": "https://myteam.mail.ru",
      "Referer": "https://myteam.mail.ru/"
    },
    body: joinParams
  });
  if (joinResp.status !== 200) throw new Error("join conversation failed: " + joinResp.status);
  var convData = JSON.parse(joinResp.body);

  return convData;
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

  var iceServers = buildIceServers(callConfig);
  var pcOpts = { iceServers: iceServers };
  if (forceRelay) pcOpts.iceTransportPolicy = "relay";
  pc = webrtc.newPeerConnection(pcOpts);
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
      setState("connected");
    } else if (s === "failed" || s === "closed") {
      setState("reconnecting", "pc state " + s);
    }
  };

  dc = pc.createDataChannel("flux-tunnel", { ordered: true });
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
    setState("reconnecting", "datachannel closed");
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

  if (targetPeerId && targetPeerId !== fromClientId && pc) {
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

  var iceServers = buildIceServers(callConfig);
  var pcOpts = { iceServers: iceServers };
  if (forceRelay) pcOpts.iceTransportPolicy = "relay";
  pc = webrtc.newPeerConnection(pcOpts);

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
      setState("reconnecting", "pc state " + s);
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
      setState("reconnecting", "datachannel closed");
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
    setTimeout(function () {
      if (running) connectOnce().catch(function () {});
    }, 2000);
  };
}

async function connectOnce() {
  console.log("[VKTEAMS] connectOnce: authenticateAndJoin starting...");
  callConfig = await authenticateAndJoin(joinLink);
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

// checkCall answers the wizard's "does this call exist": the guest sign-in chain the transport starts with, up to
// joining the conversation by its link. No signaling socket is opened, so the check leaves nothing in the call.
function checkCall(req) {
  var body;
  try { body = JSON.parse(text.decode(req.body)); } catch (e) {
    return { status: 400, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ok: false, error: "bad json" }) };
  }
  var link = extractJoinLink(String(body.url || "").trim());
  if (!/^[\w-]{4,128}$/.test(link)) {
    return jsonResponse({ ok: false, error: "В ссылке нет кода звонка: нужна ссылка вида https://calls.myteam.mail.ru/join/…" });
  }
  return authenticateAndJoin(link).then(function (conv) {
    if (!conv || !conv.endpoint) return jsonResponse({ ok: false, error: "VK Teams не вернул адрес звонка: он закончился?" });
    var turn = !!(conv.turn_server && conv.turn_server.urls && conv.turn_server.urls.length);
    var stun = !!(conv.stun_server && conv.stun_server.urls && conv.stun_server.urls.length);
    return jsonResponse({ ok: true, turn: turn, stun: stun, code: link.slice(0, 4) + "…" });
  }).catch(function (e) {
    var msg = String(e && e.message || e);
    // An answer the service gave (a status) is about the call; anything else is the way to it.
    var st = /failed: (\d+)/.exec(msg);
    if (st) {
      return jsonResponse({ ok: false, error: /join conversation failed/.test(msg)
        ? "Звонок не найден или уже закончился (" + st[1] + ")"
        : "VK Teams отказал в гостевом входе (" + st[1] + ")" });
    }
    return jsonResponse({ ok: false, network: true, error: "Нет связи с VK Teams: " + msg.slice(0, 160) });
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
    if (req.path === "/api/check" && req.method === "POST") {
      return checkCall(req);
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
  return joinLink;
}

// launch signs in and joins; a failed attempt is tried again every few seconds.
function launch() {
  running = true;
  setState("connecting");
  connectOnce().catch(function (e) {
    setState("reconnecting", String(e));
    setTimeout(function () {
      if (running) connectOnce().catch(function () {});
    }, 3000);
  });
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
      version: "1.1.0",
      mtu: 0,
      reliable: true,
      ordered: true,
      // Both are settings, none is the profile's one input: the wizard hands each end its values, and a profile
      // field would be wiped by it. Which end this is the core says itself (cfg.params.exit).
      params: [
        { key: "url", label: "Ссылка на звонок VK Teams", type: "url", required: true, scope: "settings",
          placeholder: "https://calls.myteam.mail.ru/join/…",
          description: "Клиент и нода заходят в этот звонок гостями." },
        { key: "relay", label: "Только через TURN", type: "boolean", scope: "settings", advanced: true, default: "false",
          description: "Весь трафик пойдёт через сервер VK: медленнее, но проходит там, где прямой путь закрыт." }
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
    if (!link || link === joinLink) return;
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
    if (wizardServer) {
      try { wizardServer.close(); } catch (e) {}
      wizardServer = null;
    }
    resetCallState();
    stopSocket();
  }
};
