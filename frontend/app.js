// Live Reality Fact-Check Overlay — frontend driver.
// 1. POST /api/sessions with a YouTube URL.
// 2. Mount the YouTube IFrame Player with the resolved video id.
// 3. Open SSE to /api/sessions/{id}/stream; buffer claims keyed by id.
// 4. 100ms loop reveals each claim in the log + overlay once playback reaches
//    its timestamp, so the sidebar stays in sync with the video instead of
//    dumping every claim the moment the backend emits it.

// How long an overlay pill lingers past the claim's end time.
const DISPLAY_HOLD_SECONDS = 8;
// Reveal a claim this many seconds before its start time. 0 = show it exactly
// when the statement is spoken; bump up to surface claims slightly ahead.
const REVEAL_LEAD_SECONDS = 0;

const els = {
  form: document.getElementById('session-form'),
  url: document.getElementById('url'),
  mode: document.getElementById('mode'),
  stop: document.getElementById('stop'),
  status: document.getElementById('status'),
  overlay: document.getElementById('overlay-layer'),
  claimList: document.getElementById('claim-list'),
  queueHint: document.getElementById('claim-queue-hint'),
};

let ytPlayer = null;
let ytApiReady = false;
let pendingVideoId = null;
let session = null;        // { sessionId, kind, startedAt }
let claims = new Map();    // claim_id -> { text, tStart, tEnd, status, summary, hasVerdict, revealed, li }
let evtSource = null;
let renderTimer = null;
let sawError = false;

function setStatus(text, mode = 'idle') {
  els.status.textContent = text;
  els.status.className = `status ${mode}`;
}

function extractVideoId(url) {
  try {
    const u = new URL(url);
    if (u.hostname.includes('youtu.be')) return u.pathname.slice(1);
    if (u.searchParams.get('v')) return u.searchParams.get('v');
    const m = u.pathname.match(/\/(live|embed|shorts)\/([\w-]{6,})/);
    if (m) return m[2];
  } catch (_) {}
  return null;
}

window.onYouTubeIframeAPIReady = () => {
  ytApiReady = true;
  if (pendingVideoId) mountPlayer(pendingVideoId);
};

function mountPlayer(videoId) {
  if (!ytApiReady) { pendingVideoId = videoId; return; }
  if (ytPlayer) { try { ytPlayer.destroy(); } catch (_) {} }
  ytPlayer = new YT.Player('player', {
    videoId,
    playerVars: { autoplay: 1, modestbranding: 1, rel: 0, playsinline: 1 },
    events: {
      onReady: () => { try { ytPlayer.playVideo(); } catch (_) {} },
    },
  });
}

async function startSession(url) {
  setStatus('starting…');
  const videoId = extractVideoId(url);
  if (!videoId) { setStatus('invalid URL', 'error'); return; }
  mountPlayer(videoId);

  const mode = els.mode?.value ?? 'audio';
  let res;
  try {
    res = await fetch('/api/sessions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ youtube_url: url, mode }),
    });
  } catch (e) { setStatus(`network: ${e.message}`, 'error'); return; }
  if (!res.ok) {
    let detail = `error ${res.status}`;
    try {
      const err = await res.json();
      detail = err.detail || err.error || detail;
    } catch (_) {}
    setStatus(detail, 'error');
    return;
  }
  const body = await res.json();

  session = { sessionId: body.session_id, kind: body.kind, startedAt: Date.now() };
  claims = new Map();
  sawError = false;
  els.claimList.innerHTML = '';
  els.overlay.innerHTML = '';
  if (els.queueHint) els.queueHint.hidden = true;

  setStatus(`${body.kind} · ${mode} · ${body.title ?? 'session active'}`, 'live');
  openStream();
  startRenderLoop();
}

function openStream() {
  if (evtSource) evtSource.close();
  evtSource = new EventSource(`/api/sessions/${session.sessionId}/stream`);

  evtSource.addEventListener('claim_detected', (e) => {
    const data = JSON.parse(e.data);
    upsertClaim(data.claim, { status: 'yellow', summary: '(checking…)', hasVerdict: false });
  });

  evtSource.addEventListener('verdict', (e) => {
    const data = JSON.parse(e.data);
    const status = data.verdict?.status ?? 'yellow';
    upsertClaim(data.claim, {
      status,
      summary: data.verdict?.summary ?? '',
      hasVerdict: true,
    });
  });

  evtSource.addEventListener('error', (e) => {
    // EventSource fires this on both server-sent `event: error` payloads AND on
    // raw transport errors (no e.data). Only the former carries info worth
    // showing in the sidebar.
    if (!e.data) return;
    let msg = 'error';
    try { msg = JSON.parse(e.data).message ?? 'error'; } catch (_) {}
    setStatus(msg, 'error');
    sawError = true;
    appendErrorToLog(msg);
  });

  evtSource.addEventListener('session_ended', () => {
    if (!sawError) setStatus('session ended', 'idle');
    evtSource.close();
  });
}

function appendErrorToLog(message) {
  const li = document.createElement('li');
  li.className = 'red';
  li.innerHTML = `
    <span class="timestamp">error</span>
    <span class="text">${escapeHtml(message)}</span>
  `;
  els.claimList.prepend(li);
}

// Buffer a claim by id. The render loop decides *when* it appears; the backend
// may stream a claim_detected then a verdict (or just a verdict, when cached)
// for the same id, so we merge into one entry instead of two log rows.
function upsertClaim(claim, { status, summary, hasVerdict }) {
  if (!claim) return;
  const id = claim.claim_id;
  let entry = claims.get(id);
  if (!entry) {
    entry = {
      text: claim.text ?? '',
      tStart: claim.t_start ?? 0,
      tEnd: claim.t_end ?? claim.t_start ?? 0,
      status,
      summary,
      hasVerdict,
      revealed: false,
      li: null,
    };
    claims.set(id, entry);
  } else if (hasVerdict || !entry.hasVerdict) {
    // A verdict always wins; a late claim_detected must not stomp a verdict.
    entry.status = status;
    entry.summary = summary;
    entry.hasVerdict = entry.hasVerdict || hasVerdict;
  }
  if (entry.revealed) renderLogRow(entry);
}

function renderLogRow(entry) {
  const li = entry.li ?? document.createElement('li');
  li.className = entry.status;
  li.innerHTML = `
    <span class="timestamp">${formatTime(entry.tStart)}</span>
    <span class="text">${escapeHtml(entry.text)}</span>
    <span class="summary">${escapeHtml(entry.summary)}</span>
  `;
  entry.li = li;
  return li;
}

function startRenderLoop() {
  stopRenderLoop();
  renderTimer = setInterval(renderTick, 100);
}

function stopRenderLoop() {
  if (renderTimer) { clearInterval(renderTimer); renderTimer = null; }
}

function currentTime() {
  if (!session) return 0;
  if (session.kind === 'live') {
    return (Date.now() - session.startedAt) / 1000;
  }
  try { return ytPlayer?.getCurrentTime?.() ?? 0; } catch (_) { return 0; }
}

function renderTick() {
  const t = currentTime();
  revealDueClaims(t);
  renderOverlay(t);
  renderQueueHint();
}

// Recorded videos get fact-checked in one fast pass, so every claim can be
// buffered seconds after Start while playback is still near 0:00. Without a
// cue the sidebar looks broken ("session ended" but empty), so surface how
// many claims are waiting and when the next one is due.
function renderQueueHint() {
  if (!els.queueHint) return;
  let pending = 0;
  let next = Infinity;
  for (const entry of claims.values()) {
    if (!entry.revealed) {
      pending += 1;
      if (entry.tStart < next) next = entry.tStart;
    }
  }
  if (pending === 0) {
    els.queueHint.hidden = true;
    els.queueHint.textContent = '';
    return;
  }
  const noun = pending === 1 ? 'fact-check' : 'fact-checks';
  els.queueHint.textContent = `${pending} ${noun} queued · next at ${formatTime(next)} — plays in sync with the video`;
  els.queueHint.hidden = false;
}

// Reveal claims in the sidebar once playback reaches their start time, oldest
// first so the newest spoken claim ends up on top. A claim the backend has
// already verdicted simply shows its final status the moment it's revealed.
function revealDueClaims(t) {
  const due = [];
  for (const entry of claims.values()) {
    if (!entry.revealed && entry.tStart <= t + REVEAL_LEAD_SECONDS) due.push(entry);
  }
  due.sort((a, b) => a.tStart - b.tStart);
  for (const entry of due) {
    els.claimList.prepend(renderLogRow(entry));
    entry.revealed = true;
  }
}

function renderOverlay(t) {
  const active = [];
  for (const entry of claims.values()) {
    if (!entry.revealed) continue;
    if (t >= entry.tStart && t <= entry.tEnd + DISPLAY_HOLD_SECONDS) active.push(entry);
  }
  active.sort((a, b) => a.tStart - b.tStart);

  els.overlay.innerHTML = '';
  for (const entry of active.slice(-3)) {
    const pill = document.createElement('div');
    pill.className = `pill ${entry.status}`;
    pill.innerHTML = `
      <span class="dot"></span>
      <span class="claim-text">${escapeHtml(entry.text)}</span>
    `;
    els.overlay.appendChild(pill);
  }
}

function formatTime(s) {
  const m = Math.floor(s / 60);
  const r = Math.floor(s % 60);
  return `${m}:${String(r).padStart(2, '0')}`;
}

function escapeHtml(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  }[c]));
}

els.form.addEventListener('submit', (e) => {
  e.preventDefault();
  startSession(els.url.value.trim());
});

els.stop.addEventListener('click', async () => {
  if (!session) return;
  try { await fetch(`/api/sessions/${session.sessionId}`, { method: 'DELETE' }); } catch (_) {}
  if (evtSource) evtSource.close();
  stopRenderLoop();
  if (els.queueHint) els.queueHint.hidden = true;
  setStatus('stopped');
  session = null;
});
