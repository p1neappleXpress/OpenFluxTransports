// MTS-Link WebRTC transport - genuine P2P WebRTC DataChannel tunnel through MTS-Link meetings.
//
// Joins an MTS-Link video conference as a participant, exchanges SDP offer/answer
// and ICE candidates over Comet WebSocket/Chat signaling, and tunnels data directly
// through high-speed WebRTC DataChannels with automatic Chat fallback.

var UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36";
var APP_VERSION = "3.9.207";
var PING_INTERVAL_MS = 10000;

var meetingURL = "";
var userName = "OpenFlux-Client";
var isExit = false;
var running = false;

// HTTP client state
var deviceID = "";
var publicIP = "";

// Session state
var eventID = "";
var eventSessionID = "";
var sessionID = "";
var userId = 0;
var participationID = "";
var wsHost = "";
var runID = "";
var peerRunID = "";
var peerUserID = 0;

// Comet WebSocket
var cometWS = null;
var cometPingTimer = null;

// P2P WebRTC
var peerPC = null;
var dataChannel = null;
var isDCOpen = false;
var isChatFallback = false;
var fallbackTimer = null;
var haveRemoteDesc = false;
var pendingCandidates = [];
var localCandidates = [];
var candTimer = null;

// Signaling state
var offerTimer = null;
var offerStopFlag = false;

// Reconnect
var reconnectAttempt = 0;
var connectedAt = null;

// STUN servers
var defaultStunServers = [
  "stun:stun.cloudflare.com:3478",
  "stun:stun.l.google.com:19302",
  "stun:stun1.l.google.com:19302",
  "stun:stun.webinar.ru:3478",
  "stun:stun.mts.ru:3478"
];
var activeStunServers = [];
var customTurn = "";
var customTurnUser = "";
var customTurnPass = "";

// Fallback chat queue (used if WebRTC connection fails or pending)
var chatQueue = [];
var chatFlushTimer = null;

function generateDeviceID() {
  var bytes = new Uint8Array(8);
  for (var i = 0; i < 8; i++) bytes[i] = Math.floor(Math.random() * 256);
  var hex = "";
  for (var i = 0; i < bytes.length; i++) {
    var h = bytes[i].toString(16);
    if (h.length === 1) h = "0" + h;
    hex += h;
  }
  return hex;
}

function generateRunID() {
  var bytes = new Uint8Array(4);
  for (var i = 0; i < 4; i++) bytes[i] = Math.floor(Math.random() * 256);
  var hex = "";
  for (var i = 0; i < bytes.length; i++) {
    var h = bytes[i].toString(16);
    if (h.length === 1) h = "0" + h;
    hex += h;
  }
  return hex;
}

function reconnectBackoff(n) {
  if (n < 1) n = 1;
  var shift = n - 1;
  if (shift > 4) shift = 4;
  var d = 1500 * Math.pow(2, shift);
  if (d > 30000) d = 30000;
  d += Math.floor(Math.random() * (d / 2 + 1));
  return d;
}

function getICEServers() {
  var servers = [];
  for (var i = 0; i < activeStunServers.length; i++) {
    servers.push({ urls: [activeStunServers[i]] });
  }
  if (customTurn) {
    var turnObj = { urls: [customTurn] };
    if (customTurnUser) turnObj.username = customTurnUser;
    if (customTurnPass) turnObj.credential = customTurnPass;
    servers.push(turnObj);
  }
  return servers;
}

// ===== HTTP API Methods =====

async function detectPublicIP() {
  if (publicIP) return;
  try {
    var res = await http.fetch({
      url: "https://api.ipify.org",
      method: "GET",
      headers: { "User-Agent": UA }
    });
    if (res.status === 200 && res.body) {
      publicIP = res.body.trim();
      console.log("[MTS-WebRTC] Detected public IP:", publicIP);
    }
  } catch (e) {
    try {
      var res2 = await http.fetch({
        url: "https://ifconfig.me/ip",
        method: "GET",
        headers: { "User-Agent": UA }
      });
      if (res2.status === 200 && res2.body) {
        publicIP = res2.body.trim();
        console.log("[MTS-WebRTC] Detected public IP:", publicIP);
      }
    } catch (e2) {}
  }
}

async function getEventInfo() {
  var re1 = /\/j\/(\d+)\/(\d+)\/session\/(\d+)/;
  var matches = meetingURL.match(re1);
  if (matches && matches.length === 4) {
    eventID = matches[1];
    eventSessionID = matches[2];
  } else {
    var re2 = /\/j\/(\d+)\/(\d+)/;
    matches = meetingURL.match(re2);
    if (!matches || matches.length !== 3) {
      throw new Error("invalid meeting URL format: " + meetingURL);
    }
    eventID = matches[2];
  }

  var apiURL = "https://gw.mts-link.ru/api/event/" + eventID;
  var res = await http.fetch({
    url: apiURL,
    method: "GET",
    headers: {
      "User-Agent": UA,
      "X-Device-Id": deviceID,
      "X-Device": "Chrome 153.0.0.0",
      "X-Os": "Linux",
      "X-App-Version": APP_VERSION,
      "X-Platform": "Web",
      "Accept": "*/*"
    }
  });

  if (res.status !== 200) {
    throw new Error("fetch event info failed: " + res.status);
  }

  var eventResp = JSON.parse(res.body);
  if (!eventResp.eventSessions || eventResp.eventSessions.length === 0) {
    throw new Error("no event sessions found");
  }

  eventSessionID = String(eventResp.eventSessions[0].id);
  wsHost = eventResp.comet && eventResp.comet.server && eventResp.comet.server.host;
  if (!wsHost) wsHost = "msg-edge-5.webinar.ru";
}

async function loadInitialPage() {
  await http.fetch({
    url: meetingURL,
    method: "GET",
    headers: {
      "User-Agent": UA,
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
  });
}

async function guestLogin() {
  var apiURL = "https://gw.mts-link.ru/api/eventsessions/" + eventSessionID + "/guestlogin";
  var formData = "nickname=" + encodeURIComponent(userName) + "&name=&secondName=&phone=";

  var res = await http.fetch({
    url: apiURL,
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
      "Origin": "https://my.mts-link.ru",
      "Referer": "https://my.mts-link.ru/",
      "User-Agent": UA,
      "X-Device-Id": deviceID,
      "X-Device": "Chrome 153.0.0.0",
      "X-Os": "Linux",
      "X-App-Version": APP_VERSION,
      "X-Platform": "Web"
    },
    body: formData
  });

  if (res.status === 404) throw new Error("meeting not found (404)");
  if (res.status !== 200) throw new Error("guest login failed: " + res.status);

  var loginResp = JSON.parse(res.body);
  userId = loginResp.participation.user.id;
  sessionID = loginResp.user.sessionID;
  participationID = String(loginResp.participation.id);
}

async function registerConnection() {
  var apiURL = "https://gw.mts-link.ru/api/eventsessions/" + eventSessionID + "/connections";

  try {
    var res = await http.fetch({
      url: apiURL,
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
        "X-User-Id": String(userId),
        "Origin": "https://my.mts-link.ru",
        "Referer": "https://my.mts-link.ru/",
        "User-Agent": UA,
        "X-Device-Id": deviceID
      }
    });

    if (res.status === 200) {
      var connResp = JSON.parse(res.body);
      participationID = String(connResp.id);
    }
  } catch (e) {
    // Non-critical
  }
}

async function getUserChannels() {
  var apiURL = "https://gw.mts-link.ru/api/eventsession/" + eventSessionID + "/userchannels";

  var res = await http.fetch({
    url: apiURL,
    method: "GET",
    headers: {
      "User-Agent": UA,
      "X-Device-Id": deviceID
    }
  });

  if (res.status !== 200) throw new Error("get userchannels failed: " + res.status);

  var uc = JSON.parse(res.body);
  var channels = [];
  if (uc.cometUserId) channels.push(uc.cometUserId);
  if (uc.cometUserInEventSessionId) channels.push(uc.cometUserInEventSessionId);
  if (uc.cometEventId) channels.push(uc.cometEventId);
  if (uc.sessionCometId) channels.push(uc.sessionCometId);
  if (uc.cometEventSessionId) channels.push(uc.cometEventSessionId);
  if (uc.cometParticipantSessionInEventSessionId) channels.push(uc.cometParticipantSessionInEventSessionId);

  return channels;
}

async function sendChatMessage(text) {
  var apiURL = "https://gw.mts-link.ru/api/eventsessions/" + eventSessionID + "/chat";

  var res = await http.fetch({
    url: apiURL,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Origin": "https://my.mts-link.ru",
      "Referer": "https://my.mts-link.ru/",
      "User-Agent": UA,
      "X-Device-Id": deviceID
    },
    body: JSON.stringify({ text: text })
  });

  if (res.status !== 200 && res.status !== 201) {
    throw new Error("chat send failed: " + res.status);
  }
}

// ===== Comet WebSocket =====

async function connectCometWebSocket(channels) {
  var wsURL = "wss://" + wsHost + "/engine.io/?EIO=3&transport=websocket";

  cometWS = await ws.open(wsURL, {
    "Origin": "https://my.mts-link.ru",
    "User-Agent": UA
  }, { readTimeoutMs: 90000 });

  cometWS.onmessage = handleCometMessage;
  cometWS.onclose = handleCometClose;

  var subscribeData = {
    name: "subscribe",
    data: {
      version: "0.3",
      channels: channels,
      cursor: null
    }
  };
  var eioMsg = "4" + JSON.stringify(subscribeData);
  cometWS.send(eioMsg);

  console.log("[MTS-WebRTC] ✓ Comet WebSocket subscribed to " + channels.length + " channels");
  startCometPing();
}

function startCometPing() {
  if (cometPingTimer) clearInterval(cometPingTimer);
  cometPingTimer = setInterval(function() {
    if (cometWS) {
      try { cometWS.send("2"); } catch (e) {}
    }
  }, PING_INTERVAL_MS);
}

function handleCometMessage(data) {
  var msg = typeof data === "string" ? data : text.decode(data);

  if (msg === "2") {
    if (cometWS) cometWS.send("3");
    return;
  }
  if (msg === "3") return;

  if (msg.indexOf("4{") === 0) {
    var jsonData = msg.substring(1);
    try {
      var ev = JSON.parse(jsonData);
      if (ev.name === "message" && ev.data && ev.data.messages) {
        for (var i = 0; i < ev.data.messages.length; i++) {
          var item = JSON.parse(ev.data.messages[i]);
          if (item.key === "message.add" && item.data) {
            handleIncomingChat(item.data.authorId, item.data.text);
          }
        }
      }
    } catch (e) {
      console.error("[MTS-WebRTC] Comet message parse error:", e);
    }
  }
}

function handleCometClose(reason) {
  console.log("[MTS-WebRTC] Comet WebSocket closed:", reason);
  if (cometPingTimer) {
    clearInterval(cometPingTimer);
    cometPingTimer = null;
  }
  cometWS = null;
}

// ===== SDP Codec =====

function encodeSDP(sdp) {
  try {
    var compressed = gzip.compress(text.encode(sdp));
    return base64.encode(compressed);
  } catch (e) {
    return base64.encode(text.encode(sdp));
  }
}

function decodeSDP(b64) {
  var data = base64.decode(b64);
  var arr = new Uint8Array(data);
  if (arr.length >= 2 && arr[0] === 0x1f && arr[1] === 0x8b) {
    try {
      var uncompressed = gzip.decompress(data);
      return text.decode(uncompressed);
    } catch (e) {
      console.error("[MTS-WebRTC] gzip decompress error:", e);
    }
  }
  return text.decode(data);
}

// ===== Signaling Message Dispatcher =====

function handleIncomingChat(authorID, messageText) {
  if (authorID === userId) return;

  // OF1:OFFER:<runID>:<clientUID>:<timeMs>:<b64SDP>
  if (messageText.indexOf("OF1:OFFER:") === 0) {
    var parts = messageText.split(":");
    if (parts.length >= 5) {
      var offerRunID = parts[2];
      var clientUID = parseInt(parts[3]);
      var timeMs = parts.length >= 6 ? parseInt(parts[4]) : 0;
      var b64Offer = parts.length >= 6 ? parts[5] : parts[4];
      if (!isNaN(clientUID)) {
        handleOffer(offerRunID, clientUID, timeMs, b64Offer);
      }
    }
    return;
  }

  // OF1:ANSWER:<runID>:<clientUID>:<exitUID>:<timeMs>:<b64SDP>
  if (messageText.indexOf("OF1:ANSWER:") === 0) {
    var parts = messageText.split(":");
    if (parts.length >= 6) {
      var answerRunID = parts[2];
      var clientUID = parseInt(parts[3]);
      var exitUID = parseInt(parts[4]);
      var timeMs = parts.length >= 7 ? parseInt(parts[5]) : 0;
      var b64Answer = parts.length >= 7 ? parts[6] : parts[5];
      if (!isNaN(clientUID) && !isNaN(exitUID)) {
        handleAnswer(answerRunID, clientUID, exitUID, timeMs, b64Answer);
      }
    }
    return;
  }

  // OF1:CANDS:<runID>:<destUID>:<srcUID>:<timeMs>:<b64JSON>
  if (messageText.indexOf("OF1:CANDS:") === 0) {
    var parts = messageText.split(":");
    if (parts.length >= 6) {
      var candRunID = parts[2];
      var destUID = parseInt(parts[3]);
      var srcUID = parseInt(parts[4]);
      var timeMs = parts.length >= 7 ? parseInt(parts[5]) : 0;
      var b64JSON = parts.length >= 7 ? parts[6] : parts[5];
      if (destUID === userId) {
        handleRemoteCandidates(candRunID, srcUID, timeMs, b64JSON);
      }
    }
    return;
  }

  // OF1:DATA:<destUID>:<srcUID>:<b64Data> (chat fallback)
  if (messageText.indexOf("OF1:DATA:") === 0) {
    var parts = messageText.split(":");
    if (parts.length === 5) {
      var destUID = parseInt(parts[2]);
      if (destUID === userId) {
        try {
          var payload = base64.decode(parts[4]);
          if (payload.byteLength > 0) {
            emit(payload);
          }
        } catch (e) {
          console.error("[MTS-WebRTC] Data decode error:", e);
        }
      }
    }
  }
}

// ===== Candidate Batching and Exchange =====

function addCandidateWithPublicIP(c) {
  if (!c || !c.candidate) return;
  localCandidates.push(c);

  // If host candidate has a private IP (e.g. Docker/VPS 172.x, 10.x, 192.168.x),
  // synthesize a candidate with the detected public IP so remote peers (e.g. mobile phones)
  // can route UDP packets to this node directly.
  if (publicIP) {
    var candStr = c.candidate;
    var parts = candStr.split(" ");
    if (parts.length >= 8 && parts[7] === "host") {
      var ip = parts[4];
      if (ip !== publicIP && (ip.indexOf("172.") === 0 || ip.indexOf("10.") === 0 || ip.indexOf("192.168.") === 0 || ip === "127.0.0.1")) {
        var pubCandStr = candStr.replace(" " + ip + " ", " " + publicIP + " ");
        var pubCandidate = {
          candidate: pubCandStr,
          sdpMid: c.sdpMid,
          sdpMLineIndex: c.sdpMLineIndex
        };
        console.log("[MTS-WebRTC] Adding public IP candidate:", pubCandStr.substring(0, 48) + "...");
        localCandidates.push(pubCandidate);
      }
    }
  }
}

function flushPendingCandidates() {
  if (!peerPC || !haveRemoteDesc) return;
  for (var i = 0; i < pendingCandidates.length; i++) {
    peerPC.addIceCandidate(pendingCandidates[i]).catch(function(e) {
      console.error("[MTS-WebRTC] flush candidate error:", e);
    });
  }
  pendingCandidates = [];
}

function handleRemoteCandidates(candRunID, srcUID, timeMs, b64JSON) {
  var age = timeMs > 0 ? (Date.now() - timeMs) : 0;
  if (age > 30000 || age < -5000) return;

  var currentRun = isExit ? peerRunID : runID;
  if (candRunID !== currentRun) return;

  try {
    var raw = text.decode(base64.decode(b64JSON));
    var cands = JSON.parse(raw);
    console.log("[MTS-WebRTC] Received " + cands.length + " remote candidates from UID=" + srcUID);
    for (var i = 0; i < cands.length; i++) {
      var c = cands[i];
      if (peerPC && haveRemoteDesc) {
        peerPC.addIceCandidate(c).catch(function(e) {
          console.error("[MTS-WebRTC] addIceCandidate error:", e);
        });
      } else {
        pendingCandidates.push(c);
      }
    }
  } catch (e) {
    console.error("[MTS-WebRTC] parse candidates error:", e);
  }
}

function sendBufferedCandidates() {
  if (localCandidates.length === 0 || peerUserID === 0) return;
  var batch = localCandidates.slice();
  localCandidates = [];
  var currentRun = isExit ? peerRunID : runID;
  var b64 = base64.encode(text.encode(JSON.stringify(batch)));
  var msg = "OF1:CANDS:" + currentRun + ":" + peerUserID + ":" + userId + ":" + Date.now() + ":" + b64;
  console.log("[MTS-WebRTC] Sending " + batch.length + " local candidates to peer UID=" + peerUserID);
  sendChatMessage(msg).catch(function(e) {
    console.error("[MTS-WebRTC] Send candidates error:", e);
  });
}

// ===== WebRTC P2P Signaling =====

async function startWebRTCSignaling() {
  if (isExit) {
    console.log("[MTS-WebRTC] [Exit] Ready, listening for Client offers...");
    return;
  }

  console.log("[MTS-WebRTC] [Client] Creating P2P PeerConnection...");
  peerPC = webrtc.newPeerConnection({
    iceServers: getICEServers(),
    iceTransportPolicy: "all"
  });

  haveRemoteDesc = false;
  pendingCandidates = [];
  localCandidates = [];

  setupPCHandlers(peerPC, "Client");

  dataChannel = peerPC.createDataChannel("openflux", {
    ordered: false,
    maxRetransmits: 0
  });
  setupDataChannelHandlers(dataChannel, "Client");

  peerPC.onicecandidate = function(c) {
    if (c) {
      console.log("[MTS-WebRTC] [Client] ICE candidate: " + c.candidate.substring(0, 35) + "...");
      addCandidateWithPublicIP(c);
      if (peerUserID !== 0) {
        if (candTimer) clearTimeout(candTimer);
        candTimer = setTimeout(sendBufferedCandidates, 150);
      }
    } else {
      console.log("[MTS-WebRTC] [Client] ICE gathering complete");
      if (peerUserID !== 0) sendBufferedCandidates();
    }
  };

  var offerSdp = await peerPC.createOffer();
  await peerPC.setLocalDescription("offer", offerSdp);

  var b64Offer = encodeSDP(offerSdp);
  var offerMsg = "OF1:OFFER:" + runID + ":" + userId + ":" + Date.now() + ":" + b64Offer;

  offerStopFlag = false;
  sendOfferPeriodically(offerMsg);

  // Arm fallback timer: if WebRTC DataChannel doesn't connect within 8 seconds, activate Chat fallback
  if (fallbackTimer) clearTimeout(fallbackTimer);
  fallbackTimer = setTimeout(function() {
    if (!isDCOpen && running) {
      console.log("[MTS-WebRTC] [Client] WebRTC connect timeout (8s), activating Chat fallback");
      isChatFallback = true;
      setState("connected");
    }
  }, 8000);
}

function sendOfferPeriodically(offerMsg) {
  sendChatMessage(offerMsg).catch(function(e) {
    console.error("[MTS-WebRTC] Offer send error:", e);
  });

  if (offerTimer) clearTimeout(offerTimer);
  offerTimer = setTimeout(function() {
    if (!offerStopFlag && running && !isDCOpen) {
      console.log("[MTS-WebRTC] [Client] Re-sending OFFER...");
      sendOfferPeriodically(offerMsg);
    }
  }, 3000);
}

function handleOffer(clientRunID, clientUID, timeMs, b64Offer) {
  if (!isExit) return;

  var age = timeMs > 0 ? (Date.now() - timeMs) : 0;
  if (age > 20000 || age < -5000) {
    console.log("[MTS-WebRTC] [Exit] Ignoring stale offer (age=" + age + "ms)");
    return;
  }

  if (peerPC) {
    if (peerRunID === clientRunID && peerUserID === clientUID) {
      return; // Already handling this run
    }
    console.log("[MTS-WebRTC] [Exit] Replacing old connection with new offer");
    cleanupPeerConnection();
  }

  console.log("[MTS-WebRTC] [Exit] Handling OFFER from clientUID=" + clientUID + " (runID=" + clientRunID + ")");
  peerRunID = clientRunID;
  peerUserID = clientUID;
  haveRemoteDesc = false;
  pendingCandidates = [];
  localCandidates = [];

  peerPC = webrtc.newPeerConnection({
    iceServers: getICEServers(),
    iceTransportPolicy: "all"
  });

  setupPCHandlers(peerPC, "Exit");

  peerPC.ondatachannel = function(channel) {
    console.log("[MTS-WebRTC] [Exit] ondatachannel label=" + channel.label);
    dataChannel = channel;
    setupDataChannelHandlers(channel, "Exit");
  };

  peerPC.onicecandidate = function(c) {
    if (c) {
      console.log("[MTS-WebRTC] [Exit] ICE candidate: " + c.candidate.substring(0, 35) + "...");
      addCandidateWithPublicIP(c);
      if (candTimer) clearTimeout(candTimer);
      candTimer = setTimeout(sendBufferedCandidates, 150);
    } else {
      console.log("[MTS-WebRTC] [Exit] ICE gathering complete");
      sendBufferedCandidates();
    }
  };

  var sdpStr = decodeSDP(b64Offer);
  peerPC.setRemoteDescription("offer", sdpStr).then(function() {
    haveRemoteDesc = true;
    flushPendingCandidates();
    return peerPC.createAnswer();
  }).then(function(answerSdp) {
    return peerPC.setLocalDescription("answer", answerSdp).then(function() {
      return answerSdp;
    });
  }).then(function(answerSdp) {
    console.log("[MTS-WebRTC] [Exit] Sending ANSWER to clientUID=" + clientUID);
    var b64Answer = encodeSDP(answerSdp);
    var ansMsg = "OF1:ANSWER:" + peerRunID + ":" + clientUID + ":" + userId + ":" + Date.now() + ":" + b64Answer;
    return sendChatMessage(ansMsg);
  }).then(function() {
    sendBufferedCandidates();
  }).catch(function(e) {
    console.error("[MTS-WebRTC] [Exit] Offer handling error:", e);
  });

  // Arm fallback timer on Exit side
  if (fallbackTimer) clearTimeout(fallbackTimer);
  fallbackTimer = setTimeout(function() {
    if (!isDCOpen && running) {
      console.log("[MTS-WebRTC] [Exit] WebRTC connect timeout (8s), activating Chat fallback");
      isChatFallback = true;
      setState("connected");
    }
  }, 8000);
}

function handleAnswer(answerRunID, clientUID, exitUID, timeMs, b64Answer) {
  if (isExit) return;
  if (answerRunID !== runID || clientUID !== userId) return;
  if (haveRemoteDesc) return; // Prevent duplicate answer handling

  var age = timeMs > 0 ? (Date.now() - timeMs) : 0;
  if (age > 20000 || age < -5000) return;

  console.log("[MTS-WebRTC] [Client] Received ANSWER from exitUID=" + exitUID);

  offerStopFlag = true;
  if (offerTimer) {
    clearTimeout(offerTimer);
    offerTimer = null;
  }

  peerUserID = exitUID;

  var sdpStr = decodeSDP(b64Answer);
  peerPC.setRemoteDescription("answer", sdpStr).then(function() {
    console.log("[MTS-WebRTC] [Client] Remote answer set!");
    haveRemoteDesc = true;
    flushPendingCandidates();
    sendBufferedCandidates();
  }).catch(function(e) {
    console.error("[MTS-WebRTC] [Client] SetRemoteDescription error:", e);
  });
}

function setupPCHandlers(pc, role) {
  pc.onconnectionstatechange = function(state) {
    console.log("[MTS-WebRTC] [" + role + "] PeerConnection state:", state);
    if (state === "failed") {
      if (!isDCOpen && running) {
        console.log("[MTS-WebRTC] [" + role + "] PeerConnection failed, activating Chat fallback");
        isChatFallback = true;
        setState("connected");
      }
    } else if (state === "closed") {
      if (isDCOpen) {
        isDCOpen = false;
        if (!isChatFallback && running) {
          isChatFallback = true;
          setState("connected");
        } else {
          setState("reconnecting", "peerconnection closed");
        }
      }
    }
  };

  pc.oniceconnectionstatechange = function(state) {
    console.log("[MTS-WebRTC] [" + role + "] ICE state:", state);
  };
}

function setupDataChannelHandlers(dc, role) {
  dc.onopen = function() {
    console.log("[MTS-WebRTC] [" + role + "] ✓ P2P DataChannel OPEN!");
    isDCOpen = true;
    isChatFallback = false;
    if (fallbackTimer) {
      clearTimeout(fallbackTimer);
      fallbackTimer = null;
    }
    connectedAt = Date.now();
    reconnectAttempt = 0;
    setState("connected");
  };

  dc.onclose = function() {
    console.log("[MTS-WebRTC] [" + role + "] DataChannel closed");
    isDCOpen = false;
    if (!isChatFallback && running) {
      console.log("[MTS-WebRTC] [" + role + "] Switching to Chat fallback on DC close");
      isChatFallback = true;
      setState("connected");
    }
  };

  dc.onerror = function(err) {
    console.error("[MTS-WebRTC] [" + role + "] DataChannel error:", err);
  };

  dc.onmessage = function(msgData) {
    emit(msgData);
  };
}

// ===== Paced Batched Chat Fallback =====

function startChatFallbackWorker() {
  if (chatFlushTimer) clearInterval(chatFlushTimer);
  chatFlushTimer = setInterval(function() {
    if (isDCOpen || chatQueue.length === 0 || peerUserID === 0) return;

    // Send queued packets in small batches to respect MTS Link API rate limits
    var count = 0;
    while (chatQueue.length > 0 && count < 3) {
      var pkt = chatQueue.shift();
      if (!pkt) break;
      count++;
      var b64 = base64.encode(pkt);
      var dataMsg = "OF1:DATA:" + peerUserID + ":" + userId + ":" + b64;
      sendChatMessage(dataMsg).catch(function(e) {
        console.error("[MTS-WebRTC] Chat fallback error:", e);
      });
    }
  }, 100);
}

// ===== Main Connection Flow =====

async function connectAndServe() {
  try {
    console.log("[MTS-WebRTC] Starting WebRTC connection flow...");

    await getEventInfo();
    console.log("[MTS-WebRTC] ✓ EventSessionId:", eventSessionID);

    await Promise.all([
      loadInitialPage(),
      detectPublicIP()
    ]);

    await guestLogin();
    console.log("[MTS-WebRTC] ✓ Logged in (UserID: " + userId + ")");

    await registerConnection();

    var channels = await getUserChannels();
    console.log("[MTS-WebRTC] ✓ Got " + channels.length + " Comet channels");

    await connectCometWebSocket(channels);
    startChatFallbackWorker();

    await startWebRTCSignaling();
    console.log("[MTS-WebRTC] ✓ Signaling active");
  } catch (e) {
    console.error("[MTS-WebRTC] Connection error:", e);
    setState("reconnecting", String(e));
    scheduleReconnect();
  }
}

function scheduleReconnect() {
  if (!running) return;

  var next = reconnectAttempt + 1;
  if (next > 10) next = 10;

  var d = reconnectBackoff(next);
  console.log("[MTS-WebRTC] Reconnect in " + d + "ms (attempt " + next + ")");

  setTimeout(function() {
    if (!running) return;
    reconnectAttempt = next;
    connectAndServe();
  }, d);
}

function cleanupPeerConnection() {
  isDCOpen = false;
  isChatFallback = false;
  if (fallbackTimer) {
    clearTimeout(fallbackTimer);
    fallbackTimer = null;
  }
  if (candTimer) {
    clearTimeout(candTimer);
    candTimer = null;
  }
  if (dataChannel) {
    try { dataChannel.close(); } catch (e) {}
    dataChannel = null;
  }
  if (peerPC) {
    try { peerPC.close(); } catch (e) {}
    peerPC = null;
  }
  haveRemoteDesc = false;
  pendingCandidates = [];
  localCandidates = [];
}

function cleanup() {
  running = false;
  offerStopFlag = true;

  if (offerTimer) {
    clearTimeout(offerTimer);
    offerTimer = null;
  }

  if (fallbackTimer) {
    clearTimeout(fallbackTimer);
    fallbackTimer = null;
  }

  if (candTimer) {
    clearTimeout(candTimer);
    candTimer = null;
  }

  if (chatFlushTimer) {
    clearInterval(chatFlushTimer);
    chatFlushTimer = null;
  }

  if (cometPingTimer) {
    clearInterval(cometPingTimer);
    cometPingTimer = null;
  }

  cleanupPeerConnection();

  if (cometWS) {
    try { cometWS.close(); } catch (e) {}
    cometWS = null;
  }
}

// ===== Transport Interface =====

var Transport = {
  info: function() {
    return {
      name: "mtslink-webrtc",
      version: "1.2.0",
      cookieDomain: "",
      mtu: 1200,
      reliable: false,
      ordered: false,
      halfDuplex: false,
      minIntervalMs: 0,
      params: [
        { key: "url", label: "MTS-Link Meeting URL", type: "url", required: true },
        { key: "public_ip", label: "Public IP (for NAT/Docker servers)", type: "string" },
        { key: "stun", label: "STUN Servers (comma-separated, or 'none')", type: "string" },
        { key: "turn", label: "TURN Server (turn:host:port)", type: "string" },
        { key: "turn_user", label: "TURN Username", type: "string" },
        { key: "turn_pass", label: "TURN Password", type: "password" }
      ]
    };
  },

  open: function(cfg) {
    meetingURL = cfg.url || (cfg.params && cfg.params.url);
    if (!meetingURL) {
      setState("dead", "no meeting URL provided");
      return;
    }

    var exitParam = cfg.params && cfg.params.exit;
    isExit = (exitParam === "true" || exitParam === true);

    activeStunServers = defaultStunServers.slice();
    if (cfg.params) {
      if (cfg.params.public_ip) {
        publicIP = String(cfg.params.public_ip).trim();
      }
      if (cfg.params.stun) {
        var sVal = cfg.params.stun.trim();
        if (sVal === "none" || sVal === "off" || sVal === "false") {
          activeStunServers = [];
        } else {
          activeStunServers = sVal.split(",").map(function(s) { return s.trim(); });
        }
      }
      if (cfg.params.turn) customTurn = cfg.params.turn;
      if (cfg.params.turn_user) customTurnUser = cfg.params.turn_user;
      if (cfg.params.turn_pass) customTurnPass = cfg.params.turn_pass;
    }

    var roleName = isExit ? "Exit" : "Client";
    userName = "OpenFlux-" + roleName;

    deviceID = generateDeviceID();
    runID = generateRunID();

    console.log("[MTS-WebRTC] Starting transport (role=" + roleName + ", runID=" + runID + ", stunServers=" + activeStunServers.length + ")");

    running = true;
    setState("connecting");
    connectAndServe();
  },

  write: function(bytes) {
    if (!running) throw new Error("not connected");

    // Priority 1: High-speed P2P DataChannel
    if (dataChannel && isDCOpen) {
      try {
        dataChannel.send(bytes);
        return;
      } catch (e) {
        isDCOpen = false;
      }
    }

    // Priority 2: Chat fallback
    chatQueue.push(bytes);
    if (chatQueue.length > 500) {
      chatQueue.shift();
    }
  },

  close: function() {
    cleanup();
  }
};
