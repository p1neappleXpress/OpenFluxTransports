// yandex-caldav — OpenFlux packets ride a private Yandex Calendar (CalDAV).
//
// Why CalDAV: a calendar on caldav.yandex.ru is ordinary HTTPS to a service
// that is not on any block list, keeps the traffic inside the account the
// user already has, and survives restarts (the events live on the server,
// not in the process). The reverse side of that: a calendar is a slow
// carrier - every PUT and GET is a round trip - so the channel is spread
// over several independent event pairs ("slots") and one PUT carries a
// whole window, not one packet.
//
// Layout (each slot is an independent pair of resources):
//
//   <calendar>/ofx-<channel>-c2s-0.ics   written by the client, read by the exit
//   <calendar>/ofx-<channel>-s2c-0.ics   written by the exit, read by the client
//   ... -1.ics, -2.ics ...
//
// Both ends use the same Yandex account and the same calendar (the exit
// creates it on first run, displayname "OpenFlux" by default). Yandex
// normalises events on PUT and drops unknown X- properties (verified
// 2026-10-31), so everything this transport needs lives in DESCRIPTION:
//
//   DESCRIPTION:OFX1-<ack-epoch>.<ack-seq>-<base64 frames>
//
// Every PUT carries all unacknowledged frames of its slot that fit in one
// event (~640 KiB). The ack pair in the peer's event says how far that slot
// has been read; acknowledged frames leave the writer's ring. The slots are
// independent and run in parallel, so the carrier is not one stop-and-wait
// link: while slot 0 waits for its ack, slots 1 and 2 are already putting.
// That, and 640 KiB (rather than 48 KiB) per PUT, is what takes the carrier
// from ~15 KB/s to the hundreds.
//
// Wire format of one frame (big-endian):
//
//   0..3   epoch     random id of the writer's run, changes on restart
//   4..7   seq       per-slot frame number, starts at 1
//   8..11  ts        Unix seconds the frame was queued
//   12..13 len       payload length
//   14..   payload   the bytes write() was given
//
// Both sides unfold the response (iCalendar folds long lines). The first
// read of a peer event decides what is a leftover: if the server says the
// event was stored more than 10 s ago (Date vs LAST-MODIFIED, both the
// server's own clock, so a phone with a skewed clock changes nothing), its
// frames are acknowledged but not replayed into this session. An event
// written just now is read, which is what keeps a simultaneous start from
// discarding the two ends' first handshake frames.

var NAME = "yandex-caldav";
var VERSION = "0.3.0";
var UA = "OpenFlux/" + VERSION + " (yandex-caldav)";
var DEFAULT_SERVER = "https://caldav.yandex.ru";
var DEFAULT_CALENDAR = "OpenFlux";
var DEFAULT_CHANNEL = "main";
var DEFAULT_POLL_MS = 120;
var DEFAULT_BATCH_MS = 150;
var DEFAULT_SLOTS = 1;
var MAX_PACKET_BYTES = 65535;     // one write(): a batch handed over by the session
var MAX_EVENT_BYTES = 640 * 1024; // frames per event: ~850 KB of ICS, under Yandex's ~1.4 MB
var RING_MS = 15000;             // unacknowledged frames older than this are dropped
var MAX_RING_FRAMES = 4096;      // per slot
var MAX_RING_BYTES = 3 * 1024 * 1024;
var MAX_PENDING_FRAMES = 512;    // per slot
var FRAME_HEADER = 14;
var DATA_PREFIX = "OFX1-";

var PF_PRINCIPAL =
  '<?xml version="1.0" encoding="utf-8"?>' +
  '<d:propfind xmlns:d="DAV:"><d:prop><d:current-user-principal/></d:prop></d:propfind>';
var PF_HOME =
  '<?xml version="1.0" encoding="utf-8"?>' +
  '<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">' +
  '<d:prop><c:calendar-home-set/><d:displayname/></d:prop></d:propfind>';
var PF_CALENDARS =
  '<?xml version="1.0" encoding="utf-8"?>' +
  '<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">' +
  '<d:prop><d:resourcetype/><d:displayname/>' +
  '<c:supported-calendar-component-set/></d:prop></d:propfind>';
var PF_ETAG =
  '<?xml version="1.0" encoding="utf-8"?>' +
  '<d:propfind xmlns:d="DAV:"><d:prop><d:getetag/></d:prop></d:propfind>';

// ---- settings and live state ----

var running = false;
var isExit = false;
var serverOrigin = "";
var email = "";
var password = "";
var authHeader = "";
var calendarHint = "";
var channel = DEFAULT_CHANNEL;
var pollMs = DEFAULT_POLL_MS;
var batchMs = DEFAULT_BATCH_MS;
var slotsCount = DEFAULT_SLOTS;
var statsOn = false;

var calendarURL = "";      // collection path, always ends with "/"

var startEpoch = 0;
var epoch = 0;             // one run id shared by all slots of this end
var slots = [];            // independent event pairs, see newSlot()
var rrCursor = 0;

var connected = false;
var linkState = "";
var lastErrText = "";
var authFailed = false;
var startAttempts = 0;
var flushTimer = null;

var cfgParams = {};
var cfgURL = "";

function sleep(ms) {
  return new Promise(function (resolve) { setTimeout(resolve, ms); });
}

function log(msg) {
  console.log("[CALDAV] " + msg);
}

function randomEpoch() {
  return (Math.floor(Math.random() * 0xfffffff0) + 1) >>> 0;
}

function str(v) {
  return v === undefined || v === null ? "" : String(v);
}

function setLink(state, err) {
  var text = err ? String(err) : "";
  if (linkState === state && lastErrText === text) return;
  linkState = state;
  lastErrText = text;
  setState(state, text || undefined);
}

function fatal(msg) {
  var e = new Error(msg);
  e.fatal = true;
  return e;
}

function authError(status) {
  var msg = status === 401
    ? "Яндекс.Календарь: HTTP 401 — проверьте почту и пароль приложения " +
      "(в Яндекс ID нужен пароль типа «Календарь», обычный пароль не подходит)"
    : "Яндекс.Календарь: HTTP " + status + " — у аккаунта нет доступа к этому календарю " +
      "(нужно право записи; чужой календарь, открытый только на чтение, не подойдёт)";
  var e = new Error(msg);
  e.fatal = true;
  return e;
}

// ---- XML / URL helpers ----

function xmlEscape(s) {
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function xmlUnescape(s) {
  return String(s).replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&amp;/g, "&");
}

function tagOf(xml, tag) {
  var re = new RegExp("<(?:[A-Za-z0-9_.-]+:)?" + tag +
    "\\b[^>]*>([\\s\\S]*?)</(?:[A-Za-z0-9_.-]+:)?" + tag + ">", "i");
  var m = re.exec(xml);
  return m ? xmlUnescape(m[1].trim()) : "";
}

function hrefOf(xml, prop) {
  var re = new RegExp("<(?:[A-Za-z0-9_.-]+:)?" + prop +
    "\\b[^>]*>([\\s\\S]*?)</(?:[A-Za-z0-9_.-]+:)?" + prop + ">", "i");
  var m = re.exec(xml);
  return m ? tagOf(m[1], "href") : "";
}

// Yandex routes the principal/calendar paths with a literal "@" and answers
// 404 to the percent-encoded form it puts in its own hrefs, so every href
// this script follows gets %40 decoded back.
function fixHref(href) {
  return str(href).replace(/%40/gi, "@");
}

function hrefPath(href) {
  var h = fixHref(href);
  if (!h) return "";
  if (/^https?:\/\//i.test(h)) {
    var u = url.parse(h);
    if (u.host && serverOrigin && serverOrigin.indexOf(u.host) === -1) {
      throw fatal("календарь на другом сервере (" + u.host + "), а не на " + serverOrigin);
    }
    h = u.pathname || "/";
    if (u.search) h += u.search;
  }
  if (h.charAt(0) !== "/") h = "/" + h;
  return h;
}

function absURL(path) {
  if (/^https?:\/\//i.test(path)) return path;
  return serverOrigin + path;
}

// ---- HTTP ----

async function dav(method, target, body, depth, contentType) {
  var headers = {
    "Authorization": authHeader,
    "User-Agent": UA
  };
  if (depth !== null && depth !== undefined) headers["Depth"] = String(depth);
  if (body) headers["Content-Type"] = contentType || 'application/xml; charset="utf-8"';
  var res;
  try {
    res = await http.fetch({
      url: absURL(target),
      method: method,
      headers: headers,
      body: body || "",
      redirect: "manual"
    });
  } catch (e) {
    var ne = new Error(method + " " + target + ": " + str((e && e.message) || e));
    ne.transient = true;
    throw ne;
  }
  if (res.status === 401 || res.status === 403) throw authError(res.status);
  return res;
}

// ---- calendar discovery ----

function parseCalendars(xml) {
  var out = [];
  var re = /<(?:[A-Za-z0-9_.-]+:)?response\b[^>]*>([\s\S]*?)<\/(?:[A-Za-z0-9_.-]+:)?response>/gi;
  var m;
  while ((m = re.exec(xml)) !== null) {
    var frag = m[1];
    var href = hrefPath(tagOf(frag, "href"));
    if (!href) continue;
    if (/\/inbox\/?$/.test(href) || /\/outbox\/?$/.test(href)) continue;
    var rt = tagOf(frag, "resourcetype");
    var isCal = rt.indexOf("calendar") !== -1 &&
      !/<(?:[A-Za-z0-9_.-]+:)?addressbook\b/i.test(rt);
    if (!isCal) continue;
    var comps = [];
    var cr = /<(?:[A-Za-z0-9_.-]+:)?comp\b[^>]*\bname="([^"]+)"/gi;
    var cm;
    while ((cm = cr.exec(frag)) !== null) comps.push(cm[1].toLowerCase());
    out.push({
      href: href,
      name: tagOf(frag, "displayname"),
      comps: comps
    });
  }
  return out;
}

function pickCalendar(list, want) {
  var matches = [];
  for (var i = 0; i < list.length; i++) {
    var c = list[i];
    if (c.comps.length && c.comps.indexOf("vevent") === -1) continue;
    if (str(c.name).toLowerCase() === want.toLowerCase()) matches.push(c);
  }
  // The server may let a race create two collections with the same name;
  // picking the first path keeps both ends on the same one.
  matches.sort(function (a, b) { return a.href < b.href ? -1 : a.href > b.href ? 1 : 0; });
  return matches.length ? matches[0] : null;
}

async function listCalendars(home) {
  var res = await dav("PROPFIND", home, PF_CALENDARS, 1);
  if (res.status !== 207) {
    var e = new Error("PROPFIND " + home + ": HTTP " + res.status);
    e.transient = res.status >= 500 || res.status === 429;
    throw e;
  }
  return parseCalendars(res.body);
}

async function createCalendar(home, name) {
  var slug = safeSlug(name);
  var body =
    '<?xml version="1.0" encoding="utf-8"?>' +
    '<c:mkcalendar xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav">' +
    '<d:set><d:prop><d:displayname>' + xmlEscape(name) + '</d:displayname>' +
    '<c:supported-calendar-component-set><c:comp name="VEVENT"/></c:supported-calendar-component-set>' +
    '</d:prop></d:set></c:mkcalendar>';
  var res = await dav("MKCALENDAR", home + slug + "/", body, null);
  if (res.status !== 200 && res.status !== 201 && res.status !== 204) {
    if (res.status === 405 || res.status === 403) {
      throw fatal("Яндекс.Календарь: не удалось создать календарь «" + name +
        "» (HTTP " + res.status + ") — создайте его в calendar.yandex.ru вручную");
    }
    var e = new Error("MKCALENDAR: HTTP " + res.status);
    e.transient = true;
    throw e;
  }
  // The server chooses the collection's path (Yandex renames it to
  // events-<id>), so the calendar is found by displayname afterwards.
  for (var i = 0; i < 6; i++) {
    await sleep(500);
    var found = pickCalendar(await listCalendars(home), name);
    if (found) return found;
  }
  throw new Error("календарь «" + name + "» создан, но не появился в списке");
}

async function resolveCalendar() {
  if (calendarHint.indexOf("/") !== -1) {
    var direct = hrefPath(calendarHint);
    if (!/\/$/.test(direct)) direct += "/";
    var check = await dav("PROPFIND", direct, PF_CALENDARS, 0);
    if (check.status === 404) {
      throw fatal("Яндекс.Календарь: календарь не найден по пути " + direct);
    }
    if (check.status !== 207) {
      throw fatal("Яндекс.Календарь: PROPFIND " + direct + " -> HTTP " + check.status);
    }
    log("calendar from settings: " + direct);
    return direct;
  }
  var want = calendarHint || DEFAULT_CALENDAR;

  var root = await dav("PROPFIND", serverOrigin + "/", PF_PRINCIPAL, 0);
  if (root.status !== 207) throw fatal("CalDAV: PROPFIND / -> HTTP " + root.status + " (не CalDAV-сервер?)");
  var principal = hrefPath(hrefOf(root.body, "current-user-principal"));
  if (!principal) throw fatal("CalDAV: сервер не назвал principal (current-user-principal)");

  var pr = await dav("PROPFIND", principal, PF_HOME, 0);
  if (pr.status !== 207) throw fatal("CalDAV: PROPFIND principal -> HTTP " + pr.status);
  var home = hrefPath(hrefOf(pr.body, "calendar-home-set"));
  if (!home) throw fatal("CalDAV: сервер не назвал calendar-home-set");
  if (!/\/$/.test(home)) home += "/";

  var found = pickCalendar(await listCalendars(home), want);
  if (found) {
    log("calendar: " + found.href + " (" + found.name + ")");
    return found.href;
  }
  if (!isExit) {
    var waiting = new Error("календарь «" + want + "» ещё не создан — жду ноду");
    waiting.waiting = true;
    throw waiting;
  }
  log("creating calendar " + want);
  var created = await createCalendar(home, want);
  log("calendar created: " + created.href);
  return created.href;
}

// ---- iCalendar ----

function utcStamp(ms) {
  var d = new Date(ms);
  function p(n) { return (n < 10 ? "0" : "") + n; }
  return d.getUTCFullYear() + p(d.getUTCMonth() + 1) + p(d.getUTCDate()) + "T" +
    p(d.getUTCHours()) + p(d.getUTCMinutes()) + p(d.getUTCSeconds()) + "Z";
}

// Folds one content line at 73 characters (iCalendar's 75-octet limit minus
// CRLF) with a single leading space on continuation lines. base64 is ASCII,
// so octets and characters are the same here.
function foldLine(line) {
  if (line.length <= 73) return line;
  var parts = [line.slice(0, 73)];
  for (var i = 73; i < line.length; i += 72) parts.push(" " + line.slice(i, i + 72));
  return parts.join("\r\n");
}

function buildICS(uid, ackE, ackS, base64data) {
  return "BEGIN:VCALENDAR\r\n" +
    "VERSION:2.0\r\n" +
    "PRODID:-//OpenFlux//" + NAME + " " + VERSION + "//EN\r\n" +
    "BEGIN:VEVENT\r\n" +
    "UID:" + uid + "\r\n" +
    "DTSTAMP:" + utcStamp(Date.now()) + "\r\n" +
    "DTSTART:20300101T000000Z\r\n" +
    "DTEND:20300101T000100Z\r\n" +
    "SUMMARY:OpenFlux\r\n" +
    foldLine("DESCRIPTION:" + DATA_PREFIX + (ackE >>> 0) + "." + (ackS >>> 0) + "-" + (base64data || "")) + "\r\n" +
    "TRANSP:TRANSPARENT\r\n" +
    "CLASS:PRIVATE\r\n" +
    "END:VEVENT\r\n" +
    "END:VCALENDAR\r\n";
}

function unfold(ics) {
  return str(ics).replace(/\r?\n[ \t]/g, "");
}

// serverAgeMs is how long ago the server stored the peer's event, measured
// against the server's own Date header. Both numbers are the server's clock,
// so a phone with a skewed clock cannot make its fresh frames look stale.
function serverAgeMs(res, ics) {
  var date = "";
  if (res.headers) date = res.headers["Date"] || res.headers["date"] || "";
  if (!date) return -1;
  var m = /LAST-MODIFIED:(\d{8}T\d{6}Z)/.exec(unfold(ics));
  if (!m) return -1;
  var t = /^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/.exec(m[1]);
  if (!t) return -1;
  var lm = Date.UTC(+t[1], +t[2] - 1, +t[3], +t[4], +t[5], +t[6]);
  var server = Date.parse(date);
  if (!server) return -1;
  return server - lm;
}

function base64OfFrames(frames) {
  var total = 0;
  for (var i = 0; i < frames.length; i++) total += FRAME_HEADER + frames[i].bytes.length;
  var buf = new Uint8Array(total);
  var off = 0;
  for (var j = 0; j < frames.length; j++) {
    var f = frames[j];
    buf[off] = (f.epoch >>> 24) & 0xff;
    buf[off + 1] = (f.epoch >>> 16) & 0xff;
    buf[off + 2] = (f.epoch >>> 8) & 0xff;
    buf[off + 3] = f.epoch & 0xff;
    buf[off + 4] = (f.seq >>> 24) & 0xff;
    buf[off + 5] = (f.seq >>> 16) & 0xff;
    buf[off + 6] = (f.seq >>> 8) & 0xff;
    buf[off + 7] = f.seq & 0xff;
    buf[off + 8] = (f.ts >>> 24) & 0xff;
    buf[off + 9] = (f.ts >>> 16) & 0xff;
    buf[off + 10] = (f.ts >>> 8) & 0xff;
    buf[off + 11] = f.ts & 0xff;
    buf[off + 12] = (f.bytes.length >>> 8) & 0xff;
    buf[off + 13] = f.bytes.length & 0xff;
    buf.set(f.bytes, off + FRAME_HEADER);
    off += FRAME_HEADER + f.bytes.length;
  }
  return base64.encode(buf);
}

// ---- per-slot state ----

function newSlot(i) {
  return {
    idx: i,
    ownUID: "", peerUID: "", ownURL: "", peerURL: "",
    ring: [],            // {epoch, seq, ts, bytes}, oldest first
    pending: [],         // bytes handed to write(), not framed yet
    ringBytes: 0, pendingBytes: 0,
    seq: 0,
    lastPeerEpoch: -1, lastSeq: -1,
    ackEpoch: 0, ackSeq: 0,
    lastAckedSeq: 0,
    lastInEtag: "", haveInEtag: false, firstSnapshot: false,
    dirty: false, flushBusy: false, pollBusy: false, pollTimer: null,
    idlePolls: 0,
    lastPutAt: 0, lastPutSeq: 0, lastPutBytes: 0
  };
}

function slotBacklog(s) {
  return s.ringBytes + s.pendingBytes;
}

// pickSlot maps one IP flow to one slot when write() is handed a bare IP
// packet. In the real core the batching layer usually hands over an opaque
// batched frame instead, which cannot be split without its codec; such a
// frame goes to the least loaded slot. Slots above 1 are therefore an
// experiment: a single stream is best on one slot, because spreading its
// packets over several event pairs reorders them and TCP pays for that.
function hashByte(h, b) {
  return (((h << 5) + h + b) | 0) >>> 0; // h*33 + b, 32-bit
}

function hashU32(h, v) {
  for (var i = 0; i < 4; i++) h = hashByte(h, (v >>> (i * 8)) & 0xff);
  return h;
}

function pickSlotFor(bytes) {
  if (slots.length <= 1) return slots[0];
  var u;
  try {
    u = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  } catch (e) {
    return leastLoaded();
  }
  if (u.length < 20 || (u[0] >>> 4) !== 4) return leastLoaded();
  var ihl = (u[0] & 0x0f) * 4;
  var proto = u[9];
  if ((proto !== 6 && proto !== 17) || ihl < 20 || u.length < ihl + 4) return leastLoaded();
  var aIP = (((u[12] << 24) | (u[13] << 16) | (u[14] << 8) | u[15]) >>> 0);
  var bIP = (((u[16] << 24) | (u[17] << 16) | (u[18] << 8) | u[19]) >>> 0);
  var aPort = (u[ihl] << 8) | u[ihl + 1];
  var bPort = (u[ihl + 2] << 8) | u[ihl + 3];
  if (aIP > bIP || (aIP === bIP && aPort > bPort)) {
    var tIP = aIP, tPort = aPort;
    aIP = bIP; aPort = bPort; bIP = tIP; bPort = tPort;
  }
  var h = 2166136261;
  h = hashU32(h, aIP); h = hashByte(h, (aPort >>> 8) & 0xff); h = hashByte(h, aPort & 0xff);
  h = hashU32(h, bIP); h = hashByte(h, (bPort >>> 8) & 0xff); h = hashByte(h, bPort & 0xff);
  h = hashByte(h, proto);
  return slots[h % slots.length];
}

// leastLoaded is the fallback for a packet pickSlotFor cannot classify.
function leastLoaded() {
  var best = slots[0], bestB = slotBacklog(best);
  for (var k = 1; k < slots.length; k++) {
    var b = slotBacklog(slots[k]);
    if (b < bestB) { best = slots[k]; bestB = b; }
  }
  return best;
}

function trimSlotRing(s, nowMs) {
  var cutoff = Math.floor(nowMs / 1000) - Math.floor(RING_MS / 1000);
  while (s.ring.length > 1 && s.ring[0].ts < cutoff) {
    s.ringBytes -= FRAME_HEADER + s.ring.shift().bytes.length;
  }
  while (s.ring.length > MAX_RING_FRAMES) {
    s.ringBytes -= FRAME_HEADER + s.ring.shift().bytes.length;
  }
  while (s.ring.length > 1 && s.ringBytes > MAX_RING_BYTES) {
    s.ringBytes -= FRAME_HEADER + s.ring.shift().bytes.length;
  }
  while (s.ring.length && s.ring[0].epoch === epoch && s.ring[0].seq <= s.lastAckedSeq) {
    s.ringBytes -= FRAME_HEADER + s.ring.shift().bytes.length;
  }
}

function unackedFrames(s) {
  var take = [];
  var total = 0;
  for (var i = 0; i < s.ring.length; i++) {
    var f = s.ring[i];
    var size = FRAME_HEADER + f.bytes.length;
    if (take.length && total + size > MAX_EVENT_BYTES) break;
    take.push(f);
    total += size;
    if (total >= MAX_EVENT_BYTES) break;
  }
  return take;
}

function hasUnacked(s) {
  for (var i = 0; i < s.ring.length; i++) {
    if (s.ring[i].epoch === epoch && s.ring[i].seq > s.lastAckedSeq) return true;
  }
  return false;
}

function ingest(s, ics, emitFrames) {
  var flat = unfold(ics);
  var m = /OFX1-([0-9]+)\.([0-9]+)-([A-Za-z0-9+/=]*)/.exec(flat);
  if (!m) return 0;
  var peerAckEpoch = parseInt(m[1], 10) >>> 0;
  var peerAckSeq = parseInt(m[2], 10) >>> 0;
  var data = m[3];

  var ackAdvanced = false;
  if (peerAckEpoch === epoch && peerAckSeq > s.lastAckedSeq) {
    s.lastAckedSeq = peerAckSeq;
    trimSlotRing(s, Date.now());
    ackAdvanced = true;
    if (statsOn && s.lastPutAt) {
      log("s" + s.idx + " acked " + s.lastAckedSeq + " / put " + s.lastPutSeq + " " +
        Math.round(s.lastPutBytes / 1024) + "K after " + Math.round(Date.now() - s.lastPutAt) + "ms");
    }
  }
  if (ackAdvanced && hasUnacked(s)) s.dirty = true; // drain the rest of the backlog

  if (!data) return 0;
  var bytes;
  try {
    bytes = new Uint8Array(base64.decode(data));
  } catch (e) {
    log("s" + s.idx + " bad base64 in the peer's event: " + e);
    return 0;
  }

  var off = 0;
  var got = 0;
  var newAck = false;
  while (off + FRAME_HEADER <= bytes.length) {
    var ep = ((bytes[off] << 24) | (bytes[off + 1] << 16) | (bytes[off + 2] << 8) | bytes[off + 3]) >>> 0;
    var sq = ((bytes[off + 4] << 24) | (bytes[off + 5] << 16) | (bytes[off + 6] << 8) | bytes[off + 7]) >>> 0;
    var ts = ((bytes[off + 8] << 24) | (bytes[off + 9] << 16) | (bytes[off + 10] << 8) | bytes[off + 11]) >>> 0;
    var ln = (bytes[off + 12] << 8) | bytes[off + 13];
    off += FRAME_HEADER;
    if (ln > bytes.length - off) {
      log("s" + s.idx + " truncated frame in the peer's event");
      break;
    }
    var payload = bytes.slice(off, off + ln);
    off += ln;
    if (ep !== s.lastPeerEpoch) {
      s.lastPeerEpoch = ep;
      s.lastSeq = -1;
    }
    if (sq <= s.lastSeq) continue;
    if (s.lastSeq >= 0 && sq > s.lastSeq + 1) {
      log("s" + s.idx + " gap: " + (sq - s.lastSeq - 1) + " frame(s) lost");
    }
    s.lastSeq = sq;
    newAck = true;
    got++;
    if (emitFrames) emit(payload.buffer);
  }
  if (newAck) {
    s.ackEpoch = s.lastPeerEpoch >>> 0;
    s.ackSeq = s.lastSeq >>> 0;
    s.dirty = true; // tell the peer what we consumed
  }
  if (got && emitFrames) log("s" + s.idx + " in: " + got + " frame(s)");
  else if (got) log("s" + s.idx + " acknowledged " + got + " stale frame(s)");
  return got;
}

// ---- per-slot I/O ----

function ownBody(s) {
  var frames = unackedFrames(s);
  return buildICS(s.ownUID, s.ackEpoch, s.ackSeq, frames.length ? base64OfFrames(frames) : "");
}

async function putOwn(s, body) {
  var res = await dav("PUT", s.ownURL, body, null, "text/calendar; charset=utf-8");
  if (res.status >= 200 && res.status < 300) return true;
  if (res.status === 409) {
    throw fatal("Яндекс.Календарь: календарь не найден или пропал (" + calendarURL +
      ") — проверьте настройку «Календарь»");
  }
  if (res.status === 413) {
    throw fatal("Яндекс.Календарь: событие слишком большое (HTTP 413)");
  }
  var e = new Error("PUT " + s.ownURL + ": HTTP " + res.status);
  e.transient = res.status >= 500 || res.status === 429;
  throw e;
}

async function flushSlot(s) {
  if (!running || !connected || s.flushBusy || !s.dirty || authFailed) return;
  s.flushBusy = true;
  try {
    var now = Date.now();
    while (s.pending.length) {
      var bytes = s.pending.shift();
      s.pendingBytes -= FRAME_HEADER + bytes.length;
      s.ring.push({
        epoch: epoch,
        seq: ++s.seq,
        ts: Math.floor(now / 1000),
        bytes: bytes
      });
      s.ringBytes += FRAME_HEADER + bytes.length;
    }
    trimSlotRing(s, now);
    var body = ownBody(s);
    s.lastPutAt = Date.now();
    s.lastPutSeq = s.seq;
    s.lastPutBytes = body.length;
    var t0 = Date.now();
    await putOwn(s, body);
    s.dirty = s.pending.length > 0;
    if (statsOn) {
      log("s" + s.idx + " put " + Math.round(body.length / 1024) + "K " +
        ((Date.now() - t0) / 1000).toFixed(2) + "s ack=" + s.lastAckedSeq + " ring=" +
        Math.round(s.ringBytes / 1024) + "K pending=" + s.pending.length);
    }
    if (linkState !== "connected") setLink("connected");
  } catch (e) {
    if (e.fatal) {
      authFailed = true;
      setLink("dead", str(e.message || e));
      return;
    }
    s.dirty = true;
    setLink("degraded", str((e && e.message) || e));
  } finally {
    s.flushBusy = false;
  }
}

function flushAll() {
  for (var i = 0; i < slots.length; i++) flushSlot(slots[i]);
}

async function pollSlot(s) {
  if (!running || !connected || s.pollBusy || authFailed) return;
  s.pollBusy = true;
  var next = pollMs;
  var sawChange = false;
  try {
    var res = await dav("PROPFIND", s.peerURL, PF_ETAG, 0);
    if (res.status === 404) {
      s.haveInEtag = false; // the peer has not written its first event yet
    } else if (res.status === 207) {
      var etag = tagOf(res.body, "getetag");
      if (!s.haveInEtag || etag !== s.lastInEtag) {
        var got = await dav("GET", s.peerURL, "", null);
        if (got.status === 200) {
          s.lastInEtag = etag;
          s.haveInEtag = true;
          var tGet = Date.now();
          var emitFrames = true;
          if (!s.firstSnapshot) {
            s.firstSnapshot = true;
            var age = serverAgeMs(got, got.body);
            if (age > 10000) {
              emitFrames = false; // a previous session's event: acknowledge, do not replay
              log("s" + s.idx + " ignoring the peer event from " + Math.round(age / 1000) + "s ago");
            }
          }
          var nFrames = ingest(s, got.body, emitFrames);
          if (statsOn) {
            log("s" + s.idx + " get " + Math.round(got.body.length / 1024) + "K " +
              ((Date.now() - tGet) / 1000).toFixed(2) + "s frames=" + nFrames + " seq=" + s.lastSeq);
          }
          sawChange = true;
          if (s.dirty) setTimeout(function () { flushSlot(s); }, 0); // ack without waiting for the tick
        } else if (got.status !== 404) {
          var ge = new Error("GET " + s.peerURL + ": HTTP " + got.status);
          ge.transient = got.status >= 500 || got.status === 429;
          throw ge;
        }
      }
    } else {
      var pe = new Error("PROPFIND " + s.peerURL + ": HTTP " + res.status);
      pe.transient = res.status >= 500 || res.status === 429;
      throw pe;
    }
    if (linkState !== "connected") setLink("connected");
    if (sawChange) {
      s.idlePolls = 0;
    } else {
      s.idlePolls++;
      // No idle back-off while a transfer is being measured: every extra
      // 100 ms of poll delay is 100 ms added to the tunnel's TCP RTT, and
      // TCP throughput is window/RTT here. Cap at 250 ms so an idle profile
      // still costs only a few requests a second.
      next = Math.min(250, pollMs + Math.floor(s.idlePolls / 3) * 50);
    }
  } catch (e) {
    if (e.fatal) {
      authFailed = true;
      setLink("dead", str(e.message || e));
      return;
    }
    setLink("degraded", str((e && e.message) || e));
    next = Math.min(8000, pollMs * 2);
  } finally {
    s.pollBusy = false;
    if (running && !authFailed) {
      s.pollTimer = setTimeout(function () { pollSlot(s); }, next);
    }
  }
}

// ---- connect ----

function parseAccounts(raw) {
  var out = [];
  var parts = str(raw).split(/[|\n]+/);
  for (var i = 0; i < parts.length; i++) {
    var part = parts[i].trim();
    if (!part) continue;
    var colon = part.indexOf(":");
    if (colon <= 0) continue;
    out.push({ email: part.slice(0, colon).trim(), password: part.slice(colon + 1).trim() });
  }
  return out;
}

function normalizeServer(raw) {
  var s = str(raw).trim() || DEFAULT_SERVER;
  if (!/^https?:\/\//i.test(s)) s = "https://" + s;
  var u = url.parse(s);
  if (!u.host) throw fatal("Яндекс.Календарь: не разобрать адрес сервера «" + s + "»");
  return u.protocol + "//" + u.host;
}

function clampInt(v, min, max, dflt) {
  var n = parseInt(str(v), 10);
  if (!isFinite(n) || n <= 0) return dflt;
  return Math.max(min, Math.min(max, n));
}

function safeSlug(s) {
  var out = str(s).toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/^[-.]+|[-.]+$/g, "");
  if (out.length > 48) out = out.slice(0, 48);
  return out || DEFAULT_CHANNEL;
}

async function connectOnce() {
  serverOrigin = normalizeServer(str(cfgParams.server));
  email = str(cfgParams.email).trim();
  password = str(cfgParams.password);
  calendarHint = str(cfgParams.calendar).trim() || DEFAULT_CALENDAR;
  channel = safeSlug(str(cfgURL) || DEFAULT_CHANNEL);

  var laneAccount = /^a([1-9][0-9]*)-/.exec(channel);
  if (laneAccount) {
    var accountIndex = parseInt(laneAccount[1], 10) - 1;
    var accounts = parseAccounts(cfgParams.accounts);
    if (accounts.length > 0) {
      if (!accounts[accountIndex]) {
        throw fatal("Яндекс.Календарь: для канала «" + channel + "» нет аккаунта a" +
          (accountIndex + 1) + " в настройке «Аккаунты для полос»");
      }
      email = accounts[accountIndex].email;
      password = accounts[accountIndex].password;
    } else if (accountIndex > 0) {
      throw fatal("Яндекс.Календарь: канал «" + channel + "» просит аккаунт a" +
        (accountIndex + 1) + ", а список аккаунтов пуст");
    }
  }

  pollMs = clampInt(cfgParams.poll_ms, 80, 10000, DEFAULT_POLL_MS);
  batchMs = clampInt(cfgParams.batch_ms, 80, 5000, DEFAULT_BATCH_MS);
  slotsCount = clampInt(cfgParams.slots, 1, 6, DEFAULT_SLOTS);
  statsOn = str(cfgParams.stats) === "true";

  if (!email || email.indexOf("@") === -1) {
    throw fatal("Яндекс.Календарь: в настройках не указана почта (например, ilyastaleb@yandex.ru)");
  }
  if (!password) {
    throw fatal("Яндекс.Календарь: в настройках нет пароля приложения (тип «Календарь»)");
  }
  authHeader = "Basic " + base64.encode(text.encode(email + ":" + password));

  calendarURL = await resolveCalendar();

  slots = [];
  for (var i = 0; i < slotsCount; i++) {
    var s = newSlot(i);
    s.ownUID = "ofx-" + channel + (isExit ? "-s2c-" : "-c2s-") + i;
    s.peerUID = "ofx-" + channel + (isExit ? "-c2s-" : "-s2c-") + i;
    s.ownURL = calendarURL + s.ownUID + ".ics";
    s.peerURL = calendarURL + s.peerUID + ".ics";
    slots.push(s);
  }

  await Promise.all(slots.map(function (s) { return putOwn(s, ownBody(s)); }));
  connected = true;
  startAttempts = 0;
  setLink("connected");
  log("ready: " + slotsCount + " slot(s), " + (isExit ? "s2c" : "c2s") + " " +
    calendarURL + "ofx-" + channel + "-" + (isExit ? "s2c" : "c2s") + "-*");
  flushTimer = setInterval(flushAll, batchMs);
  for (var j = 0; j < slots.length; j++) {
    (function (slot) {
      slot.pollTimer = setTimeout(function () { pollSlot(slot); }, 50);
    })(slots[j]);
  }
}

function resetRun() {
  startEpoch = Math.floor(Date.now() / 1000);
  epoch = randomEpoch();
  slots = [];
  rrCursor = 0;
  connected = false;
  linkState = "";
  lastErrText = "";
  authFailed = false;
  startAttempts = 0;
  if (flushTimer) { clearInterval(flushTimer); flushTimer = null; }
}

async function launch() {
  while (running && !authFailed) {
    try {
      await connectOnce();
      return;
    } catch (e) {
      startAttempts++;
      if (e && e.fatal) {
        authFailed = true;
        setLink("dead", str(e.message || e));
        return;
      }
      var msg = str((e && e.message) || e);
      setLink(connected ? "degraded" : "connecting", msg);
      log("connect attempt " + startAttempts + ": " + msg);
      var wait = e && e.waiting ? 2000 : Math.min(15000, 700 * Math.pow(2, Math.min(4, startAttempts)));
      await sleep(wait);
    }
  }
}

var Transport = {
  info: function () {
    return {
      name: NAME,
      version: VERSION,
      mtu: 0,
      batchBytes: 16384,
      reliable: false,
      ordered: false,
      httpMaxConnsPerHost: 8,
      params: [
        { key: "url", label: "Канал", type: "text", required: false,
          placeholder: "main",
          description: "Имя канала: клиент и нода должны указать одно и то же (например, main)." },
        { key: "email", label: "Почта Яндекса", type: "text", required: true,
          scope: "settings", placeholder: "ilyastaleb@yandex.ru",
          description: "Полный адрес аккаунта, на чьём календаре идёт канал." },
        { key: "password", label: "Пароль приложения («Календарь»)", type: "secret", required: true,
          scope: "settings",
          description: "Яндекс ID → Безопасность → Пароли приложений → Календарь. Обычный пароль не подойдёт." },
        { key: "accounts", label: "Аккаунты для полос", type: "secret", scope: "settings", advanced: true,
          description: "email:пароль|email:пароль — аккаунт N для каналов с префиксом aN-. Пусто: у всех полос основной аккаунт профиля." },
        { key: "calendar", label: "Календарь", type: "text", scope: "settings", default: "OpenFlux",
          description: "Имя календаря или его путь. Нода создаёт календарь, если его нет." },
        { key: "slots", label: "Полос", type: "number", scope: "settings", default: String(DEFAULT_SLOTS),
          advanced: true, min: 1, max: 6,
          description: "Сколько пар событий держать в календаре. Больше полос — выше скорость ценой большего числа запросов." },
        { key: "server", label: "Сервер CalDAV", type: "text", scope: "settings", default: DEFAULT_SERVER,
          advanced: true,
          description: "Для Яндекса: https://caldav.yandex.ru" },
        { key: "poll_ms", label: "Опрос, мс", type: "number", scope: "settings", default: String(DEFAULT_POLL_MS),
          advanced: true, min: 80, max: 10000,
          description: "Как часто проверять события собеседника (у каждой полосы свой опрос)." },
        { key: "batch_ms", label: "Сбор пакетов, мс", type: "number", scope: "settings", default: String(DEFAULT_BATCH_MS),
          advanced: true, min: 80, max: 5000,
          description: "Как часто отправлять окно данных, если появилось новое." }
      ]
    };
  },

  open: function (cfg) {
    cfg = cfg || {};
    cfgParams = cfg.params || {};
    cfgURL = str(cfg.url);
    isExit = (cfgParams.exit === "true" || cfgParams.exit === true || cfgParams.role === "exit");
    resetRun();
    running = true;
    setState("connecting");
    launch();
  },

  write: function (bytes) {
    if (!running) throw new Error("yandex-caldav: stopped");
    if (bytes.byteLength > MAX_PACKET_BYTES) {
      throw new Error("yandex-caldav: packet " + bytes.byteLength + " bytes over the " + MAX_PACKET_BYTES + " limit");
    }
    var s = pickSlotFor(bytes);
    if (s.pending.length >= MAX_PENDING_FRAMES) s = leastLoaded();
    if (s.pending.length >= MAX_PENDING_FRAMES) throw new Error("yandex-caldav: slot " + s.idx + " queue is full");
    // The host still owns the buffer after write() returns; copy before
    // queueing so a reused ArrayBuffer cannot change a queued packet.
    var src = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    var copy = new Uint8Array(src);
    s.pending.push(copy);
    s.pendingBytes += FRAME_HEADER + copy.length;
    s.dirty = true;
    return;
  },

  close: function () {
    running = false;
    if (flushTimer) { clearInterval(flushTimer); flushTimer = null; }
    for (var i = 0; i < slots.length; i++) {
      var s = slots[i];
      if (s.pollTimer) { clearTimeout(s.pollTimer); s.pollTimer = null; }
      s.ring = [];
      s.pending = [];
      s.ringBytes = 0;
      s.pendingBytes = 0;
    }
    connected = false;
    setState("dead");
  }
};
