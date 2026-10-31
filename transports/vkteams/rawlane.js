// Extra relay allocations for the vkteams carrier, beside the PeerConnection.
//
// The call's TURN server polices what it carries by the first byte of each datagram: a
// PeerConnection's traffic is DTLS (0x16/0x17) and gets the slow class (~10 KB/s per allocation),
// an RTP packet (0x80) gets the media class (~260 KB/s). One PeerConnection is one allocation, so
// the way up is more allocations, all ours, all to the same peer.
//
// Each lane here opens its own UDP socket to the same relay, allocates its own relayed address,
// trades that address with the peer over the call's custom-data bus, binds a channel to it, and
// then sends the tunnel's batches with a plain 12-byte RTP header in front. lib/turn.js does the
// STUN/TURN part; this file does the striping, the pacing and the header.
//
// The data channel still exists and stays the fallback: a lane that is not bound yet, or a peer
// that does not answer, and traffic keeps flowing (slower) the old way.

var RTP_HEADER = 12;
var RTP_PT = 96;         // dynamic payload type, as the carrier's media lane uses
var RTP_TS_STEP = 3000;  // 90 kHz clock, 30 ms per tunnel packet — a plausible video cadence
var KEEPALIVE_MS = 15000; // a permission lives ten minutes, but the relay is happier with fresh traffic
var STATS_MS = 5000;

/** "turn:host:port?transport=udp" (or turns:) → {host, port}; null when the call gave no relay. */
function endpoint(urls) {
  for (var i = 0; i < (urls || []).length; i++) {
    var u = String(urls[i] || "");
    if (u.indexOf("turn:") !== 0 && u.indexOf("turns:") !== 0) continue;
    var rest = u.slice(u.indexOf(":") + 1).split("?")[0];
    var host = rest.split(":")[0];
    var port = parseInt(rest.split(":")[1], 10) || 3478;
    if (host) return { host: host, port: port };
  }
  return null;
}

function random32() {
  return Math.floor(Math.random() * 4294967296) >>> 0;
}

/**
 * opts: {lanes, paceKBps, isExit, sendCustom(obj), emit(bytes), log(line)}
 * Returns {start(cfg), handle(msg), write(bytes) → bool, stop(), lanes()}.
 */
function create(opts) {
  var turn = opts.turn;
  var log = opts.log || function () {};
  var lanes = [];
  var wanted = opts.lanes | 0;
  var pace = Math.max(0, (opts.paceKBps | 0)) * 1024; // bytes per second per lane
  var next = 0;
  var stopped = false;
  var timers = [];

  function rtpHeader(lane, len) {
    var h = new Uint8Array(RTP_HEADER);
    h[0] = 0x80;                                  // version 2
    h[1] = (lane.marker ? 0x80 : 0x00) | RTP_PT;   // marker on the first packet of a batch run
    lane.marker = false;
    h[2] = (lane.seq >>> 8) & 0xff;
    h[3] = lane.seq & 0xff;
    lane.seq = (lane.seq + 1) & 0xffff;
    h[4] = (lane.ts >>> 24) & 0xff;
    h[5] = (lane.ts >>> 16) & 0xff;
    h[6] = (lane.ts >>> 8) & 0xff;
    h[7] = lane.ts & 0xff;
    lane.ts = (lane.ts + RTP_TS_STEP) >>> 0;
    h[8] = (lane.ssrc >>> 24) & 0xff;
    h[9] = (lane.ssrc >>> 16) & 0xff;
    h[10] = (lane.ssrc >>> 8) & 0xff;
    h[11] = lane.ssrc & 0xff;
    return h;
  }

  function sendLane(lane, payload) {
    if (!lane.ready || !lane.alloc) return false;
    var size = RTP_HEADER + payload.length;
    if (pace > 0 && lane.budget < size) return false; // the relay would drop it anyway
    var pkt = new Uint8Array(size);
    pkt.set(rtpHeader(lane, payload.length), 0);
    pkt.set(payload, RTP_HEADER);
    try {
      turn.send(lane.alloc, pkt);
    } catch (e) {
      log("[VKTEAMS] raw lane " + lane.idx + " send: " + (e && e.message ? e.message : e));
      return false;
    }
    if (pace > 0) lane.budget -= size;
    lane.sent += size;
    lane.packets++;
    return true;
  }

  function write(bytes) {
    if (!lanes.length) return false;
    var payload = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    for (var i = 0; i < lanes.length; i++) {
      var lane = lanes[(next + i) % lanes.length];
      if (sendLane(lane, payload)) {
        next = (next + i + 1) % lanes.length;
        return true;
      }
    }
    return false;
  }

  function onData(lane, payload) {
    lane.recv += payload.length;
    lane.packetsIn++;
    if (payload.length > RTP_HEADER) opts.emit(payload.slice(RTP_HEADER));
  }

  function announce(lane) {
    lane.lastAnnounce = Date.now();
    opts.sendCustom({ type: "raw-addr", laneIdx: lane.idx, from: opts.isExit ? "exit" : "client", addr: lane.alloc.relayed });
    log("[VKTEAMS] raw lane " + lane.idx + " relayed " + lane.alloc.relayed);
  }

  function startLane(idx, cfg) {
    var ep = cfg.__turnEndpoint;
    var lane = {
      idx: idx,
      ch: 0x4000 + idx,   // channels 0x4000..0x7fff are ours to pick; one per lane
      seq: Math.floor(Math.random() * 65536),
      ts: random32(),
      ssrc: random32(),
      marker: true,
      budget: pace / 4,
      sent: 0, recv: 0, packets: 0, packetsIn: 0,
      ready: false,
    };
    lanes.push(lane);
    udp.open(ep.host + ":" + ep.port).then(function (sock) {
      lane.sock = sock;
      return turn.allocate(sock, {
        host: ep.host, port: ep.port,
        username: cfg.turn_server.username,
        password: cfg.turn_server.credential,
        channel: lane.ch,
      });
    }).then(function (alloc) {
      if (stopped) { try { lane.sock.close(); } catch (e) {} return; }
      lane.alloc = alloc;
      alloc.ondata = function (payload) { onData(lane, payload); };
      alloc.onclose = function (why) { log("[VKTEAMS] raw lane " + lane.idx + " closed: " + why); lane.ready = false; };
      announce(lane);
      // The peer may have told us its address while our allocation was still being made.
      if (lane.peerAddr) bindLane(lane);
    }).catch(function (e) {
      log("[VKTEAMS] raw lane " + lane.idx + " failed: " + (e && e.message ? e.message : e));
    });
  }

  /** Binds this lane's channel to the peer's relayed address once both ends know each other. */
  function bindLane(lane) {
    if (!lane.alloc || !lane.peerAddr || lane.binding || lane.ready) return;
    lane.binding = true;
    turn.bind(lane.alloc, lane.peerAddr.ip, lane.peerAddr.port, lane.ch).then(function () {
      lane.binding = false;
      lane.ready = true;
      log("[VKTEAMS] raw lane " + lane.idx + " bound to " + lane.peerAddr.ip + ":" + lane.peerAddr.port);
    }).catch(function (e) {
      lane.binding = false;
      log("[VKTEAMS] raw lane " + lane.idx + " bind " + lane.peerAddr.ip + ":" + lane.peerAddr.port + ": " + (e && e.message ? e.message : e));
    });
  }

  /** A peer's relayed address: bind a channel to it and the lane is live. */
  function handle(msg) {
    if (!msg || msg.type !== "raw-addr") return false;
    var idx = msg.laneIdx | 0;
    var lane = null;
    for (var i = 0; i < lanes.length; i++) if (lanes[i].idx === idx) lane = lanes[i];
    if (!lane) return true;
    var parts = String(msg.addr || "").split(":");
    var ip = parts[0], port = parseInt(parts[1], 10);
    if (!ip || !port) return true;
    lane.peerAddr = { ip: ip, port: port };
    bindLane(lane);
    return true;
  }

  function start(cfg) {
    if (wanted < 1 || stopped) return;
    var ep = endpoint(cfg && cfg.turn_server && cfg.turn_server.urls);
    if (!ep) {
      log("[VKTEAMS] raw lanes: the call gave no turn url");
      return;
    }
    cfg.__turnEndpoint = ep;
    log("[VKTEAMS] raw lanes: " + wanted + " over " + ep.host + ":" + ep.port + ", pacing " + Math.round(pace / 1024) + " KB/s each");
    for (var i = 1; i <= wanted; i++) startLane(i, cfg);

    timers.push(setInterval(function () {
      var now = Date.now();
      for (var j = 0; j < lanes.length; j++) {
        var lane = lanes[j];
        if (!lane.ready) continue;
        lane.budget = Math.min(lane.budget + pace / 10, pace / 4); // 100 ms of credit, never a burst
        if (pace === 0) lane.budget = 1 << 30;
        // The custom-data bus keeps nothing: an end that joined first announced its addresses
        // before the other one was there to hear them. Repeat for as long as the lane lives — a
        // bound lane still owes the peer its address (the peer may have joined later, or restarted).
        if (lane.alloc && now - (lane.lastAnnounce || 0) >= 5000) announce(lane);
        if (now - (lane.lastKeepalive || 0) >= KEEPALIVE_MS) {
          lane.lastKeepalive = now;
          var pad = new Uint8Array(24); // an RTP packet with padding, enough to hold the permission
          pad[0] = 0x80;
          pad[1] = RTP_PT;
          try { turn.send(lane.alloc, pad); } catch (e) {}
        }
      }
    }, 100));

    timers.push(setInterval(function () {
      var sent = 0, recv = 0, ready = 0;
      for (var j = 0; j < lanes.length; j++) {
        sent += lanes[j].sent;
        recv += lanes[j].recv;
        if (lanes[j].ready) ready++;
      }
      log("[VKTEAMS] raw lanes: " + ready + "/" + lanes.length + " ready, sent " + Math.round(sent / 1024) + " KB, received " + Math.round(recv / 1024) + " KB");
    }, STATS_MS));
  }

  function stop() {
    stopped = true;
    for (var i = 0; i < timers.length; i++) clearInterval(timers[i]);
    timers = [];
    for (var j = 0; j < lanes.length; j++) {
      try { if (lanes[j].sock) lanes[j].sock.close(); } catch (e) {}
      lanes[j].ready = false;
    }
    lanes = [];
  }

  return { start: start, handle: handle, write: write, stop: stop, lanes: function () { return lanes; } };
}

module.exports = { create: create, endpoint: endpoint, RTP_HEADER: RTP_HEADER };
