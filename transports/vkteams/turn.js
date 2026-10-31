// A STUN/TURN client in plain JS, for the script transports that need their own relay allocation
// instead of the one a PeerConnection makes for them.
//
// Why: a TURN relay classifies what it carries by the first byte of each datagram. A PeerConnection's
// traffic arrives as DTLS (0x16/0x17) and is held to the slow class; a datagram that starts with an
// RTP header (0x80) is treated as call media and gets the media budget. This module owns the
// allocation, so the caller decides what the bytes look like: it sends ChannelData whose payload is
// whatever the caller hands it (the vkteams carrier puts an RTP header in front of the tunnel batch).
//
// What is here: STUN message encode/decode, long-term credentials (MD5 key, HMAC-SHA1
// MESSAGE-INTEGRITY), Allocate / CreatePermission / ChannelBind / Refresh, and the ChannelData
// framing. MD5 and SHA-1 are written out because the host gives a script only sha256.
//
// What is not: ICE, DTLS, SRTP, retransmission of the tunnel's own data. The caller owns pacing and
// reliability; the relay just forwards datagrams.
//
// Usage:
//   var turn = ./turn.js (the entry requires it and hands it to the raw lanes)
//   var alloc = await turn.allocate(sock, { host: "1.2.3.4", port: 19302, username: u, password: p });
//   await alloc.bind(peerIP, peerPort, channelNumber);
//   alloc.send(bytes);              // ChannelData to the peer through the relay
//   alloc.ondata = function (bytes) { ... };

// ── bytes ───────────────────────────────────────────────────────────────────────────────────────────

function u8(n) {
  var a = new Uint8Array(n);
  return a;
}

function concat(parts) {
  var len = 0, i;
  for (i = 0; i < parts.length; i++) len += parts[i].length;
  var out = new Uint8Array(len), at = 0;
  for (i = 0; i < parts.length; i++) {
    out.set(parts[i], at);
    at += parts[i].length;
  }
  return out;
}

function be16(n) {
  return new Uint8Array([(n >>> 8) & 0xff, n & 0xff]);
}

function be32(n) {
  return new Uint8Array([(n >>> 24) & 0xff, (n >>> 16) & 0xff, (n >>> 8) & 0xff, n & 0xff]);
}

function read16(b, at) {
  return ((b[at] << 8) | b[at + 1]) & 0xffff;
}

function read32(b, at) {
  return ((b[at] << 24) | (b[at + 1] << 16) | (b[at + 2] << 8) | b[at + 3]) >>> 0;
}

function le32(n) {
  return new Uint8Array([n & 0xff, (n >>> 8) & 0xff, (n >>> 16) & 0xff, (n >>> 24) & 0xff]);
}

function read32le(b, at) {
  return ((b[at + 3] << 24) | (b[at + 2] << 16) | (b[at + 1] << 8) | b[at]) >>> 0;
}

/** A bytes-like value (ArrayBuffer, TypedArray, array of numbers) as a Uint8Array. */
function bytes(v) {
  if (v instanceof Uint8Array) return v;
  if (typeof ArrayBuffer !== "undefined" && v instanceof ArrayBuffer) return new Uint8Array(v);
  if (v && v.buffer instanceof ArrayBuffer) return new Uint8Array(v.buffer, v.byteOffset || 0, v.length);
  if (typeof v === "string") return utf8(v);
  return new Uint8Array(v || []);
}

function utf8(s) {
  var out = [], i, c;
  for (i = 0; i < s.length; i++) {
    c = s.charCodeAt(i);
    if (c < 0x80) out.push(c);
    else if (c < 0x800) out.push(0xc0 | (c >> 6), 0x80 | (c & 0x3f));
    else if (c < 0xd800 || c >= 0xe000) out.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 0x3f), 0x80 | (c & 0x3f));
    else {
      i++;
      c = 0x10000 + (((c & 0x3ff) << 10) | (s.charCodeAt(i) & 0x3ff));
      out.push(0xf0 | (c >> 18), 0x80 | ((c >> 12) & 0x3f), 0x80 | ((c >> 6) & 0x3f), 0x80 | (c & 0x3f));
    }
  }
  return new Uint8Array(out);
}

function hex(b) {
  var s = "", i;
  for (i = 0; i < b.length; i++) s += (b[i] < 16 ? "0" : "") + b[i].toString(16);
  return s;
}

/** Bytes as the string a script's `send` would accept (kept for logs and tests). */
function toBase64(b) {
  var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/", out = "", i;
  for (i = 0; i < b.length; i += 3) {
    var n = (b[i] << 16) | ((b[i + 1] || 0) << 8) | (b[i + 2] || 0);
    out += chars[(n >> 18) & 63] + chars[(n >> 12) & 63] + (i + 1 < b.length ? chars[(n >> 6) & 63] : "=") + (i + 2 < b.length ? chars[n & 63] : "=");
  }
  return out;
}

// ── MD5 (the long-term credential key is MD5(username ":" realm ":" password)) ─────────────────────

var MD5_S = [7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
  5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
  4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
  6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21];

var MD5_K = (function () {
  var k = new Uint32Array(64), i;
  for (i = 0; i < 64; i++) k[i] = Math.floor(Math.abs(Math.sin(i + 1)) * 4294967296) >>> 0;
  return k;
})();

function md5(input) {
  var b = bytes(input);
  var len = b.length;
  var padded = new Uint8Array((((len + 8) >> 6) + 1) << 6);
  padded.set(b);
  padded[len] = 0x80;
  var bits = len * 8;
  // 64-bit little-endian length; the high half only matters above 512 MiB, and files never are.
  padded[padded.length - 8] = bits & 0xff;
  padded[padded.length - 7] = (bits >>> 8) & 0xff;
  padded[padded.length - 6] = (bits >>> 16) & 0xff;
  padded[padded.length - 5] = (bits >>> 24) & 0xff;

  var h0 = 0x67452301, h1 = 0xefcdab89, h2 = 0x98badcfe, h3 = 0x10325476;
  var m = new Uint32Array(16), i, j;

  for (i = 0; i < padded.length; i += 64) {
    for (j = 0; j < 16; j++) m[j] = read32le(padded, i + j * 4);
    var a = h0, bb = h1, c = h2, d = h3;
    for (j = 0; j < 64; j++) {
      var f, g;
      if (j < 16) { f = (bb & c) | (~bb & d); g = j; }
      else if (j < 32) { f = (d & bb) | (~d & c); g = (5 * j + 1) & 15; }
      else if (j < 48) { f = bb ^ c ^ d; g = (3 * j + 5) & 15; }
      else { f = c ^ (bb | ~d); g = (7 * j) & 15; }
      var tmp = d;
      d = c;
      c = bb;
      var sum = (a + (f >>> 0) + MD5_K[j] + m[g]) >>> 0;
      var rot = MD5_S[j];
      bb = (bb + (((sum << rot) | (sum >>> (32 - rot))) >>> 0)) >>> 0;
      a = tmp;
    }
    h0 = (h0 + a) >>> 0;
    h1 = (h1 + bb) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
  }
  // MD5 writes its state little-endian, unlike SHA-1 below.
  return concat([le32(h0), le32(h1), le32(h2), le32(h3)]);
}

// ── SHA-1 and HMAC-SHA1 (MESSAGE-INTEGRITY) ────────────────────────────────────────────────────────

function sha1(input) {
  var b = bytes(input);
  var len = b.length;
  var padded = new Uint8Array((((len + 8) >> 6) + 1) << 6);
  padded.set(b);
  padded[len] = 0x80;
  var bits = len * 8;
  padded[padded.length - 4] = (bits >>> 24) & 0xff;
  padded[padded.length - 3] = (bits >>> 16) & 0xff;
  padded[padded.length - 2] = (bits >>> 8) & 0xff;
  padded[padded.length - 1] = bits & 0xff;

  var h0 = 0x67452301, h1 = 0xefcdab89, h2 = 0x98badcfe, h3 = 0x10325476, h4 = 0xc3d2e1f0;
  var w = new Uint32Array(80), i, j;

  for (i = 0; i < padded.length; i += 64) {
    for (j = 0; j < 16; j++) w[j] = read32(padded, i + j * 4);
    for (j = 16; j < 80; j++) {
      var x = w[j - 3] ^ w[j - 8] ^ w[j - 14] ^ w[j - 16];
      w[j] = ((x << 1) | (x >>> 31)) >>> 0;
    }
    var a = h0, bb = h1, c = h2, d = h3, e = h4;
    for (j = 0; j < 80; j++) {
      var f, k;
      if (j < 20) { f = (bb & c) | (~bb & d); k = 0x5a827999; }
      else if (j < 40) { f = bb ^ c ^ d; k = 0x6ed9eba1; }
      else if (j < 60) { f = (bb & c) | (bb & d) | (c & d); k = 0x8f1bbcdc; }
      else { f = bb ^ c ^ d; k = 0xca62c1d6; }
      var t = (((a << 5) | (a >>> 27)) + f + e + k + w[j]) >>> 0;
      e = d;
      d = c;
      c = ((bb << 30) | (bb >>> 2)) >>> 0;
      bb = a;
      a = t;
    }
    h0 = (h0 + a) >>> 0;
    h1 = (h1 + bb) >>> 0;
    h2 = (h2 + c) >>> 0;
    h3 = (h3 + d) >>> 0;
    h4 = (h4 + e) >>> 0;
  }
  return concat([be32(h0), be32(h1), be32(h2), be32(h3), be32(h4)]);
}

function hmacSha1(key, message) {
  var k = bytes(key);
  if (k.length > 64) k = sha1(k);
  var block = new Uint8Array(64);
  block.set(k);
  var ipad = new Uint8Array(64), opad = new Uint8Array(64), i;
  for (i = 0; i < 64; i++) {
    ipad[i] = block[i] ^ 0x36;
    opad[i] = block[i] ^ 0x5c;
  }
  return sha1(concat([opad, sha1(concat([ipad, bytes(message)]))]));
}

// ── STUN ───────────────────────────────────────────────────────────────────────────────────────────

var MAGIC = 0x2112a442;

var ATTR = {
  MAPPED_ADDRESS: 0x0001,
  USERNAME: 0x0006,
  MESSAGE_INTEGRITY: 0x0008,
  ERROR_CODE: 0x0009,
  CHANNEL_NUMBER: 0x000c,
  LIFETIME: 0x000d,
  XOR_PEER_ADDRESS: 0x0012,
  REALM: 0x0014,
  NONCE: 0x0015,
  XOR_RELAYED_ADDRESS: 0x0016,
  REQUESTED_TRANSPORT: 0x0019,
  XOR_MAPPED_ADDRESS: 0x0020,
  DATA: 0x0013,
};

var METHOD = {
  BIND: 0x001,
  ALLOCATE: 0x003,
  REFRESH: 0x004,
  SEND: 0x006,
  DATA: 0x007,
  CREATE_PERMISSION: 0x008,
  CHANNEL_BIND: 0x009,
};

var CLASS = {REQUEST: 0, INDICATION: 1, SUCCESS: 2, ERROR: 3};

/** The 14-bit STUN type of a method in a class. */
function msgType(method, cls) {
  return ((method & 0x000f) | ((cls & 1) << 4) | ((method & 0x0070) << 1) | ((cls & 2) << 7) | ((method & 0x0f80) << 2)) & 0x3fff;
}

function attr(type, value) {
  var b = bytes(value);
  var pad = (4 - (b.length % 4)) % 4;
  return concat([be16(type), be16(b.length), b, u8(pad)]);
}

function xorAddress(addr) {
  var parts = String(addr).split(":");
  // IPv4 only: a relay on an IPv6 address would need the family and the 16-byte XOR form.
  var ip = parts[0].split(".").map(function (n) { return parseInt(n, 10); });
  if (ip.length !== 4) throw new Error("turn: only IPv4 peer addresses are handled: " + addr);
  var port = parseInt(parts[1], 10) || 0;
  var v = new Uint8Array(8);
  v[1] = 0x01;
  v[2] = ((port >> 8) & 0xff) ^ ((MAGIC >>> 24) & 0xff);
  v[3] = (port & 0xff) ^ ((MAGIC >>> 16) & 0xff);
  for (var i = 0; i < 4; i++) v[4 + i] = ip[i] ^ ((MAGIC >>> (24 - 8 * i)) & 0xff);
  return v;
}

function parseXorAddress(v) {
  if (v.length < 8 || v[1] !== 0x01) return null;
  var port = ((v[2] << 8) | v[3]) ^ ((MAGIC >>> 16) & 0xffff);
  var ip = [];
  for (var i = 0; i < 4; i++) ip.push(v[4 + i] ^ ((MAGIC >>> (24 - 8 * i)) & 0xff));
  return { address: ip.join("."), port: port };
}

function newTxid() {
  var t = u8(12);
  for (var i = 0; i < 12; i++) t[i] = Math.floor(Math.random() * 256);
  return t;
}

/** The wire bytes of a message; with [key], MESSAGE-INTEGRITY is appended (the length counts it). */
function encode(method, cls, txid, attrs, key) {
  var body = concat(attrs || []);
  var type = msgType(method, cls);
  if (key) {
    var head = concat([be16(type), be16(body.length + 24), be32(MAGIC), txid, body]);
    body = concat([body, attr(ATTR.MESSAGE_INTEGRITY, hmacSha1(key, head))]);
  }
  return concat([be16(type), be16(body.length), be32(MAGIC), txid, body]);
}

function decode(buf) {
  var b = bytes(buf);
  if (b.length < 20) return null;
  var type = read16(b, 0);
  var len = read16(b, 2);
  if (read32(b, 4) !== MAGIC) return null;
  var out = { type: type, txid: b.slice(8, 20), attrs: [] };
  var at = 20, end = Math.min(20 + len, b.length);
  while (at + 4 <= end) {
    var t = read16(b, at), l = read16(b, at + 2);
    out.attrs.push({ type: t, value: b.slice(at + 4, at + 4 + l) });
    at += 4 + l + ((4 - (l % 4)) % 4);
  }
  return out;
}

function attrOf(msg, type) {
  for (var i = 0; i < msg.attrs.length; i++) if (msg.attrs[i].type === type) return msg.attrs[i].value;
  return null;
}

function methodOf(type) {
  return ((type & 0x000f) | ((type >> 1) & 0x0070) | ((type >> 2) & 0x0f80)) & 0x0fff;
}

function classOf(type) {
  return (((type >> 4) & 1) | ((type >> 7) & 2)) >>> 0;
}

function errorOf(msg) {
  var v = attrOf(msg, ATTR.ERROR_CODE);
  if (!v || v.length < 4) return { code: 0, reason: "" };
  var code = (v[2] & 0x07) * 100 + v[3];
  var reason = "";
  for (var i = 4; i < v.length; i++) reason += String.fromCharCode(v[i]);
  return { code: code, reason: reason };
}

function utf8Decode(b) {
  var s = "", i = 0;
  while (i < b.length) {
    var c = b[i++];
    if (c < 0x80) s += String.fromCharCode(c);
    else if (c < 0xe0) s += String.fromCharCode(((c & 0x1f) << 6) | (b[i++] & 0x3f));
    else if (c < 0xf0) s += String.fromCharCode(((c & 0x0f) << 12) | ((b[i++] & 0x3f) << 6) | (b[i++] & 0x3f));
    else {
      var cp = ((c & 0x07) << 18) | ((b[i++] & 0x3f) << 12) | ((b[i++] & 0x3f) << 6) | (b[i++] & 0x3f);
      cp -= 0x10000;
      s += String.fromCharCode(0xd800 + (cp >> 10), 0xdc00 + (cp & 0x3ff));
    }
  }
  return s;
}

// ── the client ────────────────────────────────────────────────────────────────────────────────────

/**
 * One allocation on a TURN server over an already-dialed UDP socket.
 *
 * [sock] is the host's `udp.open` socket (onmessage/send/close). The relay is the only address the
 * socket talks to, so a connected socket is enough. Resolves once Allocate returned a relayed address.
 *
 * opts: {host, port, username, password, lifetime?, channel?}
 */
function allocate(sock, opts) {
  var username = opts.username || "";
  var password = opts.password || "";
  var lifetime = opts.lifetime || 600;
  var requested = [attr(ATTR.REQUESTED_TRANSPORT, [17, 0, 0, 0]), attr(ATTR.LIFETIME, be32(lifetime))];

  var client = {
    sock: sock,
    username: username,
    relayed: null,
    channel: opts.channel || 0x4000,
    lifetime: lifetime,
    ondata: null,
    onclose: null,
    closed: false,
    realm: "",
    nonce: "",
    key: null,
  };
  var pending = {};

  function send(out) {
    if (!client.closed) sock.send(out);
  }

  function request(method, attrs, key) {
    var txid = newTxid();
    var id = hex(txid);
    var wire = encode(method, CLASS.REQUEST, txid, attrs, key);
    return new Promise(function (resolve, reject) {
      var tries = 0;
      function attempt() {
        var p = pending[id];
        if (!p) return;
        p.tries++;
        send(wire);
        p.timer = setTimeout(function () {
          if (!pending[id]) return;
          if (p.tries >= 4) {
            delete pending[id];
            reject(new Error("turn: no answer to " + method + " (" + p.tries + " tries)"));
            return;
          }
          attempt();
        }, 400 * p.tries);
      }
      pending[id] = { resolve: resolve, reject: reject, tries: 0 };
      attempt();
    });
  }

  sock.onmessage = function (data) {
    var b = bytes(data);
    if (!b.length) return;
    if ((b[0] & 0xc0) === 0x40) {
      // ChannelData: 2 bytes channel, 2 bytes length, then the payload (padded to 4 bytes).
      if (b.length < 4) return;
      var len = read16(b, 2);
      if (client.ondata) client.ondata(b.slice(4, 4 + len));
      return;
    }
    if ((b[0] & 0xc0) !== 0) return;
    var msg = decode(b);
    if (!msg) return;
    var cls = classOf(msg.type), method = methodOf(msg.type);
    if (cls === CLASS.INDICATION) {
      if (method === METHOD.DATA) {
        var d = attrOf(msg, ATTR.DATA);
        if (d && client.ondata) client.ondata(d);
      }
      return;
    }
    if (cls !== CLASS.SUCCESS && cls !== CLASS.ERROR) return;
    var p = pending[hex(msg.txid)];
    if (!p) return;
    clearTimeout(p.timer);
    delete pending[hex(msg.txid)];
    if (cls === CLASS.SUCCESS) {
      p.resolve(msg);
      return;
    }
    var e = errorOf(msg);
    var err = new Error("turn: " + method + " failed: " + e.code + " " + e.reason);
    err.code = e.code;
    err.realm = attrOf(msg, ATTR.REALM);
    err.nonce = attrOf(msg, ATTR.NONCE);
    p.reject(err);
  };

  client._request = request;

  sock.onclose = function (reason) {
    client.closed = true;
    for (var id in pending) {
      clearTimeout(pending[id].timer);
      pending[id].reject(new Error("turn: socket closed: " + reason));
      delete pending[id];
    }
    if (client.onclose) client.onclose(reason);
  };

  function withCredentials(err) {
    client.realm = utf8Decode(err.realm);
    client.nonce = err.nonce;
    client.key = md5(utf8(username + ":" + client.realm + ":" + password));
    var auth = [
      attr(ATTR.USERNAME, utf8(username)),
      attr(ATTR.REALM, utf8(client.realm)),
      attr(ATTR.NONCE, client.nonce),
    ];
    return request(METHOD.ALLOCATE, requested.concat(auth), client.key);
  }

  function finish(msg) {
    var ra = attrOf(msg, ATTR.XOR_RELAYED_ADDRESS);
    if (ra) {
      var a = parseXorAddress(ra);
      if (!a) throw new Error("turn: relayed address is not IPv4");
      client.relayed = a.address + ":" + a.port;
    } else {
      var m = attrOf(msg, ATTR.MAPPED_ADDRESS);
      if (!m) throw new Error("turn: allocation has no relayed address");
      client.relayed = [m[4], m[5], m[6], m[7]].join(".") + ":" + read16(m, 2);
    }
    var lt = attrOf(msg, ATTR.LIFETIME);
    if (lt) client.lifetime = read32(lt, 0);
    return client;
  }

  return request(METHOD.ALLOCATE, requested)
    .then(finish)
    .catch(function (err) {
      if (err.code !== 401 || !err.realm || !err.nonce) throw err;
      return withCredentials(err).then(finish);
    });
}

/**
 * Permission and channel for one peer, so the relay forwards that peer's datagrams to us and ours
 * to it. The channel number must be even-ish unique per peer on this allocation (0x4000..0x7fff).
 */
function bind(client, peerIP, peerPort, channel) {
  var ch = channel || client.channel;
  var peer = attr(ATTR.XOR_PEER_ADDRESS, xorAddress(peerIP + ":" + peerPort));
  var auth = [
    attr(ATTR.USERNAME, utf8(client.username || "")),
    attr(ATTR.REALM, utf8(client.realm)),
    attr(ATTR.NONCE, client.nonce),
  ];
  return Promise.resolve()
    .then(function () {
      // CreatePermission names the peer's IP; the port there does not matter.
      return client._request(METHOD.CREATE_PERMISSION, [attr(ATTR.XOR_PEER_ADDRESS, xorAddress(peerIP + ":0"))].concat(auth), client.key);
    })
    .then(function () {
      return client._request(METHOD.CHANNEL_BIND, [attr(ATTR.CHANNEL_NUMBER, concat([be16(ch), be16(0)])), peer].concat(auth), client.key);
    })
    .then(function () {
      client.channel = ch;
      client.peer = peerIP + ":" + peerPort;
    });
}

/** Sends [payload] to the bound peer as ChannelData (4-byte header, padded to 4 bytes). */
function sendData(client, payload) {
  var b = bytes(payload);
  var pad = (4 - (b.length % 4)) % 4;
  client.sock.send(concat([be16(client.channel), be16(b.length), b, u8(pad)]));
}

module.exports = {
  allocate: allocate,
  bind: bind,
  send: sendData,
  // exposed for tests and for callers that need the primitives
  md5: md5,
  sha1: sha1,
  hmacSha1: hmacSha1,
  hex: hex,
  base64: toBase64,
  _encode: encode,
  _decode: decode,
  _attr: attr,
  _methodOf: methodOf,
  _classOf: classOf,
};
