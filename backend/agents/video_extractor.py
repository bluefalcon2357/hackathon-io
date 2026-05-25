"""Direct-video mode: hand the YouTube URL to Gemini, get all claims back.

One ``generate_content`` call with the URL as a ``file_data`` Part — Google's
infra fetches and decodes the video, the model returns structured claims with
timestamps in a single shot. Bypasses yt-dlp / ffmpeg entirely and dodges the
"YouTube blocks our Cloud Run IP" problem, since the fetch happens from
Google's network.

Trade-off: latency is one big call instead of rolling 5s chunks, so this is
recorded-video-only — the runner falls back to audio mode for livestreams.
"""
from __future__ import annotations

import json
import logging
import uuid

from backend.agents._retry import with_retry
from backend.config import get_settings
from backend.schemas import Claim

log = logging.getLogger(__name__)

_VIDEO_PROMPT = """You are a fact-checking assistant. Produce the COMPLETE
transcript of this YouTube video: every statement spoken by the speakers, in
order, from start to finish. Do NOT skip, summarize, paraphrase, or filter —
transcribe factual claims, opinions, questions, and narration alike, verbatim.

Break the transcript into individual statements (about one sentence each). For
each statement, give the time range when it is spoken as timestamps on the
video's own playback clock, in MM:SS format (use HH:MM:SS for videos longer than
an hour). Timestamps must line up with when the words are actually heard and
advance through the entire video — do not bunch them near the start.

Return JSON ONLY in this shape:
{
  "claims": [
    {
      "text": "<the verbatim statement>",
      "t_start": "MM:SS",
      "t_end": "MM:SS",
      "speaker": "speaker_1"
    }
  ]
}
"""


async def extract_claims_from_video(
    youtube_url: str, session_id: str
) -> list[Claim]:
    """Hand the YouTube URL to Gemini directly; get back all extracted claims."""
    settings = get_settings()
    if settings.stub_llm:
        return [
            Claim(
                claim_id=f"video:{session_id}:0:{uuid.uuid4().hex[:6]}",
                chunk_id=f"video:{session_id}",
                text="[stub] sample claim from video mode.",
                t_start=0.0,
                t_end=5.0,
                check_worthy=True,
                confidence=0.8,
                speaker="speaker_1",
            )
        ]

    from google import genai
    from google.genai import types

    client = genai.Client(
        vertexai=settings.google_genai_use_vertexai,
        project=settings.google_cloud_project or None,
        location=settings.vertex_location,
    )

    response = await with_retry(
        lambda: client.aio.models.generate_content(
            model=settings.gemini_model,
            contents=types.Content(
                role="user",
                parts=[
                    types.Part.from_uri(file_uri=youtube_url, mime_type="video/*"),
                    types.Part.from_text(text=_VIDEO_PROMPT),
                ],
            ),
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.1,
                max_output_tokens=settings.video_max_output_tokens,
            ),
        ),
        label="video_extractor",
    )

    return _parse_claims(response.text or "", session_id)


def _to_seconds(value: object) -> float | None:
    """Coerce a timestamp into seconds.

    Accepts raw seconds (int/float or a numeric string) and clock strings in
    ``MM:SS`` or ``HH:MM:SS`` form. Returns ``None`` for anything unparseable so
    the caller can fall back to a default.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    if ":" in s:
        try:
            parts = [float(p) for p in s.split(":")]
        except ValueError:
            return None
        seconds = 0.0
        for part in parts:
            seconds = seconds * 60 + part
        return seconds
    try:
        return float(s)
    except ValueError:
        return None


def _loads_lenient(raw: str, session_id: str) -> dict | list:
    """Parse the model's JSON, salvaging a truncated claims array if needed.

    A complete transcript of a long video can exceed the output-token budget and
    get cut off mid-object. Rather than drop the whole response, trim to the last
    complete object and close the array so the finished statements still land.
    """
    try:
        return json.loads(raw or "{}")
    except json.JSONDecodeError:
        last = raw.rfind("}")
        if last != -1:
            try:
                salvaged = json.loads(raw[: last + 1] + "]}")
            except json.JSONDecodeError:
                pass
            else:
                log.warning(
                    "video extractor JSON truncated for %s; salvaged partial transcript",
                    session_id,
                )
                return salvaged
        log.warning("video extractor returned non-JSON: %s", raw[:200])
        return {}


def _parse_claims(raw: str, session_id: str) -> list[Claim]:
    """Pure JSON-to-Claim parsing, factored out so tests can exercise it."""
    parsed = _loads_lenient(raw, session_id)
    # Gemini sometimes returns a bare array instead of the {"claims": [...]} wrapper.
    if isinstance(parsed, list):
        parsed = {"claims": parsed}
    if not isinstance(parsed, dict):
        return []

    claims: list[Claim] = []
    for i, item in enumerate(parsed.get("claims", [])):
        if not isinstance(item, dict):
            continue
        text = (item.get("text") or "").strip()
        if not text:
            continue
        t_start = _to_seconds(item.get("t_start"))
        if t_start is None:
            t_start = 0.0
        t_end = _to_seconds(item.get("t_end"))
        if t_end is None:
            t_end = t_start
        claims.append(
            Claim(
                claim_id=f"video:{session_id}:{i}:{uuid.uuid4().hex[:6]}",
                chunk_id=f"video:{session_id}",
                text=text,
                t_start=t_start,
                t_end=t_end,
                # Full-transcript mode: keep every statement, never filter here.
                check_worthy=True,
                confidence=float(item.get("confidence", 0.5)),
                speaker=item.get("speaker"),
            )
        )
    return claims
