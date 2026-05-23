"""Transcriber: takes a 5s audio chunk, returns text + best-effort speaker label.

Bypasses ADK's LlmAgent input pipeline because passing raw audio bytes through
ADK's text-oriented runner is brittle on current ADK versions. We call
google-genai directly with a multimodal Content (instruction + inline audio).
Orchestration above this still uses ADK.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from backend.agents._retry import with_retry
from backend.config import get_settings

log = logging.getLogger(__name__)

_TRANSCRIBE_PROMPT = """You will receive a short audio clip from a YouTube video.
Transcribe the speech verbatim. If multiple speakers are present, label them
"speaker_1", "speaker_2", etc. Return JSON only:
{"text": "...", "speaker": "speaker_1"}
If there is no intelligible speech, return {"text": "", "speaker": null}."""


@dataclass
class Transcription:
    text: str
    speaker: str | None


async def transcribe(audio_bytes: bytes, mime_type: str = "audio/ogg") -> Transcription:
    settings = get_settings()
    if settings.stub_llm:
        return Transcription(text="[stub] sample transcribed claim text.", speaker="speaker_1")

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
            contents=[
                types.Content(
                    role="user",
                    parts=[
                        types.Part.from_text(text=_TRANSCRIBE_PROMPT),
                        types.Part.from_bytes(data=audio_bytes, mime_type=mime_type),
                    ],
                )
            ],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.0,
            ),
        ),
        label="transcriber",
    )
    try:
        parsed = json.loads(response.text or "{}")
    except json.JSONDecodeError:
        log.warning("transcriber returned non-JSON: %s", (response.text or "")[:200])
        return Transcription(text="", speaker=None)
    if not isinstance(parsed, dict):
        return Transcription(text="", speaker=None)
    return Transcription(text=parsed.get("text", "") or "", speaker=parsed.get("speaker"))
