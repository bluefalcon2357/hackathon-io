"""ClaimExtractorAgent: text + chunk-window → list of check-worthy claims.

Implemented as an ADK LlmAgent with structured JSON output.
"""
from __future__ import annotations

import json
import logging
import uuid

from backend.config import get_settings
from backend.schemas import Claim

log = logging.getLogger(__name__)

EXTRACTION_PROMPT = """You are a fact-checking assistant. Given a transcript
snippet from a video, extract every verifiable factual claim. Skip opinions,
hypotheticals, rhetorical questions, and pure narration.

Return JSON ONLY in this shape:
{{
  "claims": [
    {{
      "text": "<the verbatim claim>",
      "check_worthy": true,
      "confidence": 0.0-1.0
    }}
  ]
}}

If no claims are check-worthy, return {{"claims": []}}.

Transcript (spans [{t_start}s, {t_end}s] of the video):
\"\"\"
{transcript}
\"\"\"
"""


async def extract_claims(
    *, chunk_id: str, transcript: str, t_start: float, t_end: float, speaker: str | None
) -> list[Claim]:
    if not transcript.strip():
        return []

    settings = get_settings()
    if settings.stub_llm:
        return [
            Claim(
                claim_id=f"{chunk_id}:{uuid.uuid4().hex[:6]}",
                chunk_id=chunk_id,
                text=transcript.strip()[:200],
                t_start=t_start,
                t_end=t_end,
                check_worthy=True,
                confidence=0.7,
                speaker=speaker,
            )
        ]

    from google import genai
    from google.genai import types

    client = genai.Client(
        vertexai=settings.google_genai_use_vertexai,
        project=settings.google_cloud_project or None,
        location=settings.vertex_location,
    )
    prompt = EXTRACTION_PROMPT.format(
        t_start=int(t_start), t_end=int(t_end), transcript=transcript
    )
    response = await client.aio.models.generate_content(
        model=settings.gemini_model,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1,
        ),
    )
    try:
        parsed = json.loads(response.text or "{}")
    except json.JSONDecodeError:
        log.warning("claim extractor returned non-JSON: %s", (response.text or "")[:200])
        return []

    claims: list[Claim] = []
    for item in parsed.get("claims", []):
        text = (item.get("text") or "").strip()
        if not text:
            continue
        claims.append(
            Claim(
                claim_id=f"{chunk_id}:{uuid.uuid4().hex[:6]}",
                chunk_id=chunk_id,
                text=text,
                t_start=t_start,
                t_end=t_end,
                check_worthy=bool(item.get("check_worthy", True)),
                confidence=float(item.get("confidence", 0.5)),
                speaker=speaker,
            )
        )
    return claims
