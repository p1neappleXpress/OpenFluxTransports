// SDP helpers for a script that carries a PeerConnection's offer and answer by itself and has no
// trickle channel for ICE candidates. The host's createOffer()/createAnswer() hand back the SDP as
// it was BEFORE gathering, so on its own it names no address to connect to: the candidates found
// after setLocalDescription() (onicecandidate) have to be put into it before it is sent, and the
// other side picks them up from the description it applies.

// candidateLine turns what onicecandidate gave ("candidate:... typ relay ...") into an SDP line.
function candidateLine(c) {
  var s = String(c);
  if (s.indexOf("a=") === 0) s = s.slice(2);
  if (s.indexOf("candidate:") !== 0) s = "candidate:" + s;
  return "a=" + s;
}

// withCandidates returns sdp with the candidates appended to its media section (a data-channel-only
// description has just the one, and it is the last), closed with end-of-candidates so the peer does
// not wait for more.
function withCandidates(sdp, candidates) {
  var lines = String(sdp).split(/\r?\n/);
  while (lines.length && lines[lines.length - 1] === "") lines.pop();
  var out = [];
  for (var i = 0; i < lines.length; i++) {
    var l = lines[i];
    if (l.indexOf("a=candidate:") === 0 || l === "a=end-of-candidates") continue; // never twice
    out.push(l);
  }
  var seen = {};
  for (var j = 0; j < candidates.length; j++) {
    var line = candidateLine(candidates[j]);
    if (seen[line]) continue;
    seen[line] = true;
    out.push(line);
  }
  out.push("a=end-of-candidates");
  return out.join("\r\n") + "\r\n";
}

// candidateCount says how many candidates an SDP carries.
function candidateCount(sdp) {
  var n = 0;
  var lines = String(sdp).split(/\r?\n/);
  for (var i = 0; i < lines.length; i++) {
    if (lines[i].indexOf("a=candidate:") === 0) n++;
  }
  return n;
}

module.exports = {
  candidateLine: candidateLine,
  withCandidates: withCandidates,
  candidateCount: candidateCount,
};
