"""Transcript-mode ingestion: pull YouTube's caption track, group cues into
complete-sentence statements, yield them as transcript chunks.

Captions are fetched up-front via yt-dlp. Cues (typically 1-3s each) are
buffered until a sentence terminator (`. ! ?`) is seen, with safety caps on
buffer time and length so we never sit on a runaway monologue. The output
matches the shape of the audio-mode pipeline — `(Chunk, text)` — so the
downstream claim-extractor / search / verdict path is reused as-is.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import tempfile
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from typing import AsyncIterator

from backend.ingestion.youtube import IngestionError
from backend.schemas import Chunk

log = logging.getLogger(__name__)

# A statement is "complete" when it ends with one of these.
_SENTENCE_END = re.compile(r"[.!?]['\"\)\]]?\s*$")
# Safety caps so transcript buffering never stalls or blows up.
_MAX_BUFFER_SECONDS = 12.0
_MAX_BUFFER_CHARS = 400

# Per-process LRU cache of parsed cues, keyed on URL. Recorded videos'
# captions are immutable, so a long TTL is fine; this is mainly here to
# soak up YouTube's caption-endpoint 429s when a user re-tries the same URL.
_CAPTION_CACHE_TTL_SECONDS = 3600.0
_CAPTION_CACHE_MAX_ENTRIES = 100
_caption_cache: OrderedDict[str, tuple[float, list[tuple[float, float, str]]]] = (
    OrderedDict()
)


class NoCaptionsError(IngestionError):
    """Raised when no usable English caption track is available for the URL."""


def _cache_get(url: str) -> list[tuple[float, float, str]] | None:
    now = time.monotonic()
    entry = _caption_cache.get(url)
    if entry is None:
        return None
    ts, cues = entry
    if now - ts > _CAPTION_CACHE_TTL_SECONDS:
        _caption_cache.pop(url, None)
        return None
    _caption_cache.move_to_end(url)
    return cues


def _cache_put(url: str, cues: list[tuple[float, float, str]]) -> None:
    _caption_cache[url] = (time.monotonic(), cues)
    _caption_cache.move_to_end(url)
    while len(_caption_cache) > _CAPTION_CACHE_MAX_ENTRIES:
        _caption_cache.popitem(last=False)


def _clear_caption_cache() -> None:
    """Test helper: drop all cached captions."""
    _caption_cache.clear()


def _vtt_timestamp_to_seconds(s: str) -> float:
    parts = s.replace(",", ".").split(":")
    if len(parts) == 3:
        h, m, sec = parts
        return int(h) * 3600 + int(m) * 60 + float(sec)
    m, sec = parts
    return int(m) * 60 + float(sec)


_TIMING_RE = re.compile(
    r"(\d{1,2}:\d{2}:\d{2}\.\d{3}|\d{1,2}:\d{2}\.\d{3})"
    r"\s*-->\s*"
    r"(\d{1,2}:\d{2}:\d{2}\.\d{3}|\d{1,2}:\d{2}\.\d{3})"
)
_TAG_RE = re.compile(r"<[^>]+>")


def parse_vtt(content: str) -> list[tuple[float, float, str]]:
    """Parse a WebVTT caption file into ``[(t_start, t_end, text), ...]``.

    YouTube auto-captions emit overlapping/rolling cues where each cue
    repeats the previous line plus one new word. We dedupe by only keeping
    text that wasn't already present in the immediately preceding cue.
    """
    cues: list[tuple[float, float, str]] = []
    prev_text = ""
    for block in re.split(r"\n\s*\n", content):
        lines = [ln.rstrip() for ln in block.splitlines() if ln.strip()]
        timing_line = next((ln for ln in lines if "-->" in ln), None)
        if not timing_line:
            continue
        m = _TIMING_RE.search(timing_line)
        if not m:
            continue
        t_start = _vtt_timestamp_to_seconds(m.group(1))
        t_end = _vtt_timestamp_to_seconds(m.group(2))
        text_lines = [ln for ln in lines if "-->" not in ln and ln != "WEBVTT"]
        # First line of a cue block can be a numeric/string ID; skip it if no spaces.
        if text_lines and len(text_lines) > 1 and " " not in text_lines[0]:
            text_lines = text_lines[1:]
        text = _TAG_RE.sub("", " ".join(text_lines)).strip()
        if not text:
            continue
        # Dedupe rolling auto-captions: keep only the suffix that's new.
        if prev_text and text.startswith(prev_text):
            new_part = text[len(prev_text):].strip()
            if not new_part:
                continue
            text = new_part
        prev_text = (prev_text + " " + text).strip()[-_MAX_BUFFER_CHARS:]
        cues.append((t_start, t_end, text))
    return cues


def buffer_into_statements(
    cues: list[tuple[float, float, str]],
    max_seconds: float = _MAX_BUFFER_SECONDS,
    max_chars: int = _MAX_BUFFER_CHARS,
) -> list[tuple[float, float, str]]:
    """Group consecutive cues until a complete-sentence boundary is reached.

    Flush triggers (in priority order):
      1. The accumulated text ends with `. ! ?` (optionally followed by a
         closing quote/paren) — this is the "complete statement" guarantee.
      2. Accumulated duration ≥ ``max_seconds``.
      3. Accumulated length ≥ ``max_chars``.
    """
    out: list[tuple[float, float, str]] = []
    buf_start: float | None = None
    buf_end: float = 0.0
    buf_text = ""

    for t_start, t_end, text in cues:
        if buf_start is None:
            buf_start = t_start
        buf_end = t_end
        buf_text = (buf_text + " " + text).strip() if buf_text else text

        terminator = bool(_SENTENCE_END.search(buf_text))
        too_long_secs = (buf_end - buf_start) >= max_seconds
        too_long_chars = len(buf_text) >= max_chars
        if terminator or too_long_secs or too_long_chars:
            out.append((buf_start, buf_end, buf_text))
            buf_start = None
            buf_text = ""

    if buf_text and buf_start is not None:
        out.append((buf_start, buf_end, buf_text))
    return out


_RATE_LIMIT_RE = re.compile(r"\b429\b|too many requests", re.IGNORECASE)


def _classify_yt_dlp_error(stderr: str) -> NoCaptionsError:
    """Map a yt-dlp stderr blob to a user-facing NoCaptionsError."""
    if "Sign in to confirm" in stderr or "bot" in stderr.lower():
        return NoCaptionsError(
            "YouTube is blocking this server's IP (bot check). "
            "Use 'Full transcript · Gemini video' mode — it fetches the video "
            "through Google instead of this server — or mount a cookies.txt file "
            "and set YT_DLP_COOKIES."
        )
    if _RATE_LIMIT_RE.search(stderr):
        return NoCaptionsError(
            "YouTube rate-limited the caption endpoint (HTTP 429). "
            "Use 'Full transcript · Gemini video' mode, which fetches the video "
            "through Google and isn't affected by this server's rate limits."
        )
    return NoCaptionsError(f"yt-dlp could not fetch captions: {stderr[:300]}")


async def fetch_captions(url: str, session_id: str) -> list[tuple[float, float, str]]:
    """Download and parse the best-available English caption track for ``url``.

    Results are cached per-URL for ``_CAPTION_CACHE_TTL_SECONDS``, so a retry
    after a transient 429 (or a second session on the same video) doesn't
    re-hit YouTube. Raises :class:`NoCaptionsError` on bot wall, rate limit,
    or genuinely missing captions.
    """
    cached = _cache_get(url)
    if cached is not None:
        log.info("caption cache hit for %s (%d cues)", url[:120], len(cached))
        return cached

    with tempfile.TemporaryDirectory(prefix=f"factcheck-subs-{session_id}-") as tmpdir:
        out_template = str(Path(tmpdir) / "subs.%(ext)s")
        args = [
            "yt-dlp",
            "--quiet",
            "--no-warnings",
            "--skip-download",
            "--write-sub",
            "--write-auto-sub",
            "--sub-langs", "en.*,en",
            "--sub-format", "vtt/best",
            "--no-playlist",
            # Backoff for transient HTTP failures (esp. 429s) from the
            # timedtext endpoint. `exp=2:8` caps at 8s, so total wait across
            # 4 retries is 2+4+8+8 = 22s before we give up and let the runner
            # fall back to audio mode.
            "--retries", "4",
            "--retry-sleep", "http:exp=2:8",
            # The Android/iOS player clients hit a different caption endpoint
            # than web and are rate-limited far less aggressively from
            # data-center IPs — same trick we use for the metadata probe.
            "--extractor-args", "youtube:player_client=android,ios,web",
            "-o", out_template,
        ]
        cookies_file = os.environ.get("YT_DLP_COOKIES")
        if cookies_file and os.path.exists(cookies_file):
            args.extend(["--cookies", cookies_file])
        args.append(url)

        log.info("fetching captions for %s", url[:120])
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            stdin=asyncio.subprocess.DEVNULL,
        )
        _, stderr = await proc.communicate()
        if proc.returncode != 0:
            raise _classify_yt_dlp_error(stderr.decode("utf-8", errors="replace").strip())

        vtt_files = sorted(Path(tmpdir).glob("*.vtt"))
        if not vtt_files:
            raise NoCaptionsError(
                "No English captions available for this video. "
                "Switch to Audio mode to transcribe via Gemini."
            )
        # Prefer manually-authored over auto-generated (filename hint).
        vtt_files.sort(key=lambda p: ("auto" in p.name.lower(), p.name))
        content = vtt_files[0].read_text(encoding="utf-8", errors="replace")
        cues = parse_vtt(content)
        if not cues:
            raise NoCaptionsError("Caption file was empty after parsing.")
        _cache_put(url, cues)
        return cues


async def prepare_statements(
    url: str, session_id: str
) -> list[tuple[float, float, str]]:
    """Fetch + sentence-buffer captions. Raises before any streaming starts,
    so the caller can fall back to audio mode without partial output."""
    cues = await fetch_captions(url, session_id)
    statements = buffer_into_statements(cues)
    log.info(
        "transcript: %d cues → %d statements for session %s",
        len(cues), len(statements), session_id,
    )
    return statements


async def stream_statements(
    statements: list[tuple[float, float, str]],
    session_id: str,
    pace_seconds: float = 0.0,
) -> AsyncIterator[tuple[Chunk, str]]:
    """Yield prepared statements as ``(Chunk, text)`` tuples for the runner."""
    for t_start, t_end, text in statements:
        chunk = Chunk(
            chunk_id=f"{session_id}:{uuid.uuid4().hex[:8]}",
            session_id=session_id,
            t_start=float(t_start),
            t_end=float(t_end),
            mime_type="text/plain",
        )
        yield chunk, text
        if pace_seconds > 0:
            await asyncio.sleep(pace_seconds)


async def stream_transcript(
    url: str,
    session_id: str,
    pace_seconds: float = 0.0,
) -> AsyncIterator[tuple[Chunk, str]]:
    """Combined prepare + stream — kept for direct callers / tests.

    The runner uses :func:`prepare_statements` + :func:`stream_statements`
    separately so it can fall back to audio mode on caption failure.
    """
    statements = await prepare_statements(url, session_id)
    async for item in stream_statements(statements, session_id, pace_seconds):
        yield item
