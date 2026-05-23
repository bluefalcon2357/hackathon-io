"""VerdictAgent: fuses google_search + trusted evidence into a green/yellow/red verdict."""
from __future__ import annotations

import json
import logging

from backend.config import get_settings
from backend.schemas import Claim, SearchEvidence, Verdict

log = logging.getLogger(__name__)

_VERDICT_PROMPT = """You are a fact-check adjudicator. Decide a verdict for a claim
given the evidence below.

Verdict rules:
- green = clearly supported by reputable evidence
- red = clearly contradicted by reputable evidence
- yellow = mixed, unverifiable, or insufficient evidence

Claim: "{claim}"

Evidence:
{evidence}

Return JSON ONLY:
{{"status": "green" | "yellow" | "red", "summary": "<one short sentence>"}}
"""


async def adjudicate(claim: Claim, evidence: list[SearchEvidence]) -> Verdict:
    settings = get_settings()

    if settings.stub_llm:
        return Verdict(
            claim_id=claim.claim_id,
            status="yellow",
            summary="Stub mode (LLM disabled).",
            citations=evidence,
        )
    if not evidence:
        return Verdict(
            claim_id=claim.claim_id,
            status="yellow",
            summary="No supporting evidence found.",
            citations=evidence,
        )

    from google import genai
    from google.genai import types

    client = genai.Client(
        vertexai=settings.google_genai_use_vertexai,
        project=settings.google_cloud_project or None,
        location=settings.vertex_location,
    )

    evidence_block = "\n\n".join(
        f"- source={e.source} supports={e.supports}\n  snippet: {e.snippet}\n  url: {e.url or '-'}"
        for e in evidence
    )

    try:
        response = await client.aio.models.generate_content(
            model=settings.gemini_model,
            contents=_VERDICT_PROMPT.format(claim=claim.text, evidence=evidence_block),
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.0,
            ),
        )
        parsed = json.loads(response.text or "{}")
    except Exception as exc:
        log.warning("verdict adjudication failed: %s", exc)
        return Verdict(
            claim_id=claim.claim_id,
            status="yellow",
            summary="Evidence inconclusive.",
            citations=evidence,
        )

    status = parsed.get("status", "yellow")
    if status not in {"green", "yellow", "red"}:
        status = "yellow"
    return Verdict(
        claim_id=claim.claim_id,
        status=status,
        summary=parsed.get("summary", "")[:240],
        citations=evidence,
    )
