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

_VIDEO_PROMPT = """You are a fact-checking assistant. Watch this YouTube video
and extract every verifiable factual claim spoken by the speakers. Skip
opinions, hypotheticals, rhetorical questions, and pure narration.

For each claim, include the time range in seconds from the start of the video
when the claim is spoken.

Return JSON ONLY in this shape:
{
  "claims": [
    {
      "text": "<the verbatim claim>",
      "t_start": <float seconds>,
      "t_end": <float seconds>,
      "speaker": "speaker_1",
      "check_worthy": true,
      "confidence": 0.0-1.0
    }
  ]
}

If no claims are check-worthy, return {"claims": []}.
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
            ),
        ),
        label="video_extractor",
    )

    return _parse_claims(response.text or "", session_id)


def _parse_claims(raw: str, session_id: str) -> list[Claim]:
    """Pure JSON-to-Claim parsing, factored out so tests can exercise it."""
    try:
        parsed = json.loads(raw or "{}")
    except json.JSONDecodeError:
        log.warning("video extractor returned non-JSON: %s", raw[:200])
        return []

    claims: list[Claim] = []
    for i, item in enumerate(parsed.get("claims", [])):
        text = (item.get("text") or "").strip()
        if not text:
            continue
        try:
            t_start = float(item.get("t_start", 0.0))
            t_end = float(item.get("t_end", t_start))
        except (TypeError, ValueError):
            t_start, t_end = 0.0, 0.0
        claims.append(
            Claim(
                claim_id=f"video:{session_id}:{i}:{uuid.uuid4().hex[:6]}",
                chunk_id=f"video:{session_id}",
                text=text,
                t_start=t_start,
                t_end=t_end,
                check_worthy=bool(item.get("check_worthy", True)),
                confidence=float(item.get("confidence", 0.5)),
                speaker=item.get("speaker"),
            )
        )
    return claims
