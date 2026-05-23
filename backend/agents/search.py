"""SearchAgent: Gemini Flash + built-in google_search grounding.

Gemini's grounding tool cannot be combined with custom FunctionTools in a
single LlmAgent request, which is why this is its own agent.
"""
from __future__ import annotations

import json
import logging

from backend.config import get_settings
from backend.schemas import SearchEvidence

log = logging.getLogger(__name__)

_SEARCH_PROMPT = """You are checking a factual claim using web search.
Search for authoritative recent information about the claim.

Claim: "{claim}"

Return JSON ONLY:
{{
  "snippet": "<one-paragraph summary of what reputable sources say>",
  "supports": "supports" | "contradicts" | "unrelated" | "unclear",
  "url": "<best source URL>",
  "domain": "<best source domain>"
}}
"""


async def search_claim(claim_text: str) -> SearchEvidence:
    settings = get_settings()
    if settings.stub_llm:
        return SearchEvidence(
            source="google_search",
            url="https://example.com",
            domain="example.com",
            snippet="[stub] no real search performed.",
            supports="unclear",
        )

    from google import genai
    from google.genai import types

    client = genai.Client(
        vertexai=settings.google_genai_use_vertexai,
        project=settings.google_cloud_project or None,
        location=settings.vertex_location,
    )

    try:
        response = await client.aio.models.generate_content(
            model=settings.gemini_model,
            contents=_SEARCH_PROMPT.format(claim=claim_text),
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())],
                temperature=0.1,
            ),
        )
        raw = (response.text or "").strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            raw = raw.split("\n", 1)[1] if "\n" in raw else raw
            raw = raw.rsplit("\n", 1)[0] if raw.endswith("```") else raw
        try:
            parsed = json.loads(raw or "{}")
        except json.JSONDecodeError:
            parsed = {"snippet": raw[:500], "supports": "unclear"}
        if not isinstance(parsed, dict):
            parsed = {"snippet": raw[:500], "supports": "unclear"}
    except Exception as exc:
        log.exception("search grounding failed: %s", exc)
        return SearchEvidence(
            source="google_search", snippet=f"search failed: {exc}", supports="unclear"
        )

    return SearchEvidence(
        source="google_search",
        url=parsed.get("url"),
        domain=parsed.get("domain"),
        snippet=parsed.get("snippet", "")[:1000],
        supports=parsed.get("supports", "unclear"),
    )
