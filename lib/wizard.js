// Setup wizard for the transports that take one link (a board, a document, a room): the page (wizard.html, carried
// here as wizard_html.js), the loopback server that serves it and answers its "check this link", and the helpers a
// check is written with. A transport gives the texts of its service as a config and one function that walks the
// link the way the transport itself does; this module does the rest - the steps the person sees, what a failure
// is told as, what the page hands back to the app.
//
//   var wizard = <this module, pulled in the way a transport pulls in lib/captcha.js>;
//   var server = wizard.serve({
//     tag: "[BOARDS]", service: "Яндекс Доски", labels: { link: "Ссылка", join: "Вход на доску" },
//     cfg: { ... },                 // wizard.html's config: texts, the site to create on, the options
//     values: { url: "..." },       // what the transport already has
//     reason: "Подключение через Доски",
//     check: function (url, rep) { ... resolve {title, lines, url, warn} or throw wizard.StepError }
//   });
//   ...
//   server.close();
//
// The page hands the app {client: {...}, node: {...}} through window.openfluxSubmit; the answer for this side comes
// back to the transport as cookiesApplied. Nothing a step notes may be a secret: a status, a host, the names of the
// keys of an answer.

var PAGE = require("./wizard_html.js");

// StepError is a check failing at a step: which one, what status the service answered (0: none) and whether there
// was no way to the service at all.
function StepError(step, message, status, network) {
  this.name = "StepError";
  this.message = message;
  this.step = step;
  this.status = status || 0;
  this.network = !!network;
  this.code = 0;
}
StepError.prototype = Object.create(Error.prototype);
StepError.prototype.constructor = StepError;

function parse(body) {
  try { return JSON.parse(body); } catch (e) { return null; }
}

// errText is an error's text, cut short for a note.
function errText(e) {
  return String(e && e.message || e).slice(0, 160);
}

// hostName names an address by its host, never by its path or query.
function hostName(url) {
  var m = /^[a-z]+:\/\/([^\/?#]+)/i.exec(String(url));
  return m ? m[1] : "?";
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

// send is http.fetch with no answer at all told as a StepError.
async function send(step, req) {
  try {
    return await http.fetch(req);
  } catch (e) {
    throw new StepError(step, "no connection: " + errText(e), 0, true);
  }
}

// openSocket opens a websocket for a check to talk to and returns a small handle: send(text), wait(predicate, ms) which
// resolves with the first message (already received or yet to come) the predicate accepts, or null on the timeout or
// when the socket closed, and close(). An Engine.IO ping ("2") is answered here, whatever the check is waiting for.
async function openSocket(step, url, headers, opts) {
  var sock;
  try {
    sock = await ws.open(url, headers || {}, opts || {});
  } catch (e) {
    throw new StepError(step, "no connection: " + errText(e), 0, true);
  }
  var queue = [];
  var waiters = [];
  var closed = false;
  sock.onmessage = function (msg) {
    if (msg === "2") {
      try { sock.send("3"); } catch (e) {}
      return;
    }
    for (var i = 0; i < waiters.length; i++) {
      if (waiters[i].pred(msg)) {
        var w = waiters.splice(i, 1)[0];
        clearTimeout(w.timer);
        w.resolve(msg);
        return;
      }
    }
    queue.push(msg);
  };
  sock.onclose = function () {
    closed = true;
    var left = waiters;
    waiters = [];
    for (var i = 0; i < left.length; i++) {
      clearTimeout(left[i].timer);
      left[i].resolve(null);
    }
  };
  return {
    send: function (msg) { sock.send(msg); },
    wait: function (pred, ms) {
      for (var i = 0; i < queue.length; i++) {
        if (pred(queue[i])) return Promise.resolve(queue.splice(i, 1)[0]);
      }
      if (closed) return Promise.resolve(null);
      return new Promise(function (resolve) {
        var waiter = { pred: pred, resolve: resolve, timer: null };
        waiter.timer = setTimeout(function () {
          var at = waiters.indexOf(waiter);
          if (at >= 0) waiters.splice(at, 1);
          resolve(null);
        }, ms);
        waiters.push(waiter);
      });
    },
    close: function () {
      sock.onmessage = null;
      sock.onclose = null;
      try { sock.close(); } catch (e) {}
    }
  };
}

// newReport is how a check tells each step: the log keeps it, the page shows it.
function newReport(tag, labels) {
  var steps = [];
  return {
    steps: steps,
    rep: function (step, ok, note) {
      steps.push({ id: step, label: (labels && labels[step]) || step, ok: ok, note: note || "" });
      console.log(tag + " wizard " + step + (ok ? " ok" : " FAILED") + (note ? " (" + note + ")" : ""));
    }
  };
}

function jsonResponse(obj) {
  return { status: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify(obj) };
}

function readBody(req) {
  return parse(text.decode(req.body)) || {};
}

// failureAnswer turns a check's failure into the page's answer: the words for the person, whether there was no way to
// the service, and the steps so far. The words say the step and the status, never what the service said.
function failureAnswer(e, report, opts) {
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
    out.error = "Нет связи с " + opts.service + ": " + e.message.replace(/^no connection: /, "").slice(0, 160);
    return out;
  }
  var own = opts.explain ? opts.explain(e) : "";
  out.error = own || (opts.service + " отказал на шаге «" + ((opts.labels && opts.labels[e.step]) || e.step) + "»" + st);
  return out;
}

// pageFor is the page with the config and the values written into it.
function pageFor(cfg, values) {
  var init = JSON.stringify({ cfg: cfg || {}, values: values || {} }).replace(/</g, "\\u003c");
  var tag = '<script id="init" type="application/json">';
  // A function, not a string: the values may hold "$&" and the like, which a replacement string would expand.
  return PAGE.replace(tag + "{}</script>", function () { return tag + init + "</script>"; });
}

// serve starts the wizard's server and asks the app to show the page. opts: see the top of the file; routes adds
// POST routes of the transport's own ({"/api/x": function (body, report) -> answer or a promise of one}).
function serve(opts) {
  var page = pageFor(opts.cfg, opts.values);
  var routes = opts.routes || {};

  function check(req) {
    var body = readBody(req);
    var url = String(body.url || "").trim();
    var report = newReport(opts.tag, opts.labels);
    return Promise.resolve().then(function () {
      return opts.check(url, report.rep, body);
    }).then(function (found) {
      var f = found || {};
      return jsonResponse({
        ok: true, title: f.title || "", lines: f.lines || [], url: f.url || "", warn: f.warn || "", steps: report.steps
      });
    }).catch(function (e) {
      return jsonResponse(failureAnswer(e, report, opts));
    });
  }

  function route(path, handler, req) {
    var report = newReport(opts.tag, opts.labels);
    return Promise.resolve().then(function () {
      return handler(readBody(req), report.rep, report);
    }).then(function (answer) {
      var out = answer || {};
      if (out.ok === undefined) out.ok = true;
      if (!out.steps) out.steps = report.steps;
      return jsonResponse(out);
    }).catch(function (e) {
      return jsonResponse(failureAnswer(e, report, opts));
    });
  }

  var server = httpserver.listen(function (req) {
    if (req.path === "/" || req.path === "") {
      return { status: 200, headers: { "Content-Type": "text/html; charset=utf-8" }, body: page };
    }
    if (req.method === "POST") {
      if (req.path === "/api/check") return check(req);
      if (routes[req.path]) return route(req.path, routes[req.path], req);
    }
    return { status: 404, body: "not found" };
  });
  raise("needsSetup", {
    url: "http://" + server.addr + "/",
    reason: opts.reason || ("Подключение через " + opts.service)
  });
  return server;
}

module.exports = {
  StepError: StepError,
  parse: parse,
  errText: errText,
  hostName: hostName,
  shape: shape,
  send: send,
  openSocket: openSocket,
  newReport: newReport,
  jsonResponse: jsonResponse,
  readBody: readBody,
  failureAnswer: failureAnswer,
  pageFor: pageFor,
  serve: serve
};
