// Live Reality Fact-Check Overlay — frontend driver.
// 1. POST /api/sessions with a YouTube URL.
// 2. Mount the YouTube IFrame Player with the resolved video id.
// 3. Open SSE to /api/sessions/{id}/stream; buffer events.
// 4. 100ms RAF loop renders overlay pills filtered by player time.

const DISPLAY_HOLD_SECONDS = 8;

const els = {
  form: document.getElementById('session-form'),
  url: document.getElementById('url'),
  mode: document.getElementById('mode'),
  stop: document.getElementById('stop'),
  status: document.getElementById('status'),
  overlay: document.getElementById('overlay-layer'),
  claimList: document.getElementById('claim-list'),
};

let ytPlayer = null;
let ytApiReady = false;
let pendingVideoId = null;
let session = null;        // { sessionId, kind, startedAt }
let events = [];           // verdict events with t_start/t_end
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
  events = [];
  sawError = false;
  els.claimList.innerHTML = '';
  els.overlay.innerHTML = '';

  setStatus(`${body.kind} · ${mode} · ${body.title ?? 'session active'}`, 'live');
  openStream();
  startRenderLoop();
}

function openStream() {
  if (evtSource) evtSource.close();
  evtSource = new EventSource(`/api/sessions/${session.sessionId}/stream`);

  evtSource.addEventListener('claim_detected', (e) => {
    const data = JSON.parse(e.data);
    events.push({ kind: 'pending', ...data });
    appendToLog(data, 'yellow', '(checking…)');
  });

  evtSource.addEventListener('verdict', (e) => {
    const data = JSON.parse(e.data);
    const status = data.verdict?.status ?? 'yellow';
    events.push({ kind: 'verdict', status, ...data });
    appendToLog(data, status, data.verdict?.summary ?? '');
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

function appendToLog(data, status, summary) {
  const li = document.createElement('li');
  li.className = status;
  const t = data.claim?.t_start ?? data.t_start ?? 0;
  li.innerHTML = `
    <span class="timestamp">${formatTime(t)}</span>
    <span class="text">${escapeHtml(data.claim?.text ?? '')}</span>
    <span class="summary">${escapeHtml(summary)}</span>
  `;
  els.claimList.prepend(li);
}

function startRenderLoop() {
  stopRenderLoop();
  renderTimer = setInterval(renderOverlay, 100);
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

function renderOverlay() {
  const t = currentTime();
  const active = events.filter((e) => {
    const t0 = e.claim?.t_start ?? e.t_start ?? 0;
    const t1 = (e.claim?.t_end ?? e.t_end ?? t0) + DISPLAY_HOLD_SECONDS;
    return t >= t0 && t <= t1;
  });

  els.overlay.innerHTML = '';
  for (const e of active.slice(-3)) {
    const status = e.kind === 'verdict' ? e.status : 'yellow';
    const pill = document.createElement('div');
    pill.className = `pill ${status}`;
    pill.innerHTML = `
      <span class="dot"></span>
      <span class="claim-text">${escapeHtml(e.claim?.text ?? '')}</span>
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
  setStatus('stopped');
  session = null;
});
