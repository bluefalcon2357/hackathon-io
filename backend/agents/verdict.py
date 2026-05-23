"""VerdictAgent: fuses google_search + trusted evidence into a green/yellow/red verdict."""
from __future__ import annotations

import json
import logging

from backend.agents._retry import with_retry
from backend.config import get_settings
from backend.schemas import Claim, SearchEvidence, Verdict

log = logging.getLogger(__name__)

_logged_model_version = False


def _log_model_version_once(model_version: str | None) -> None:
    """Log the resolved Gemini model_version on the first response.

    `gemini-flash-latest` is a server-side alias; the actual concrete
    version is only visible in the response metadata. Logging once per
    process is enough to verify what Vertex routed us to.
    """
    global _logged_model_version
    if _logged_model_version or not model_version:
        return
    log.info("gemini resolved model_version=%s", model_version)
    _logged_model_version = True


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
        response = await with_retry(
            lambda: client.aio.models.generate_content(
                model=settings.gemini_model,
                contents=_VERDICT_PROMPT.format(claim=claim.text, evidence=evidence_block),
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.0,
                ),
            ),
            label="verdict",
        )
        _log_model_version_once(getattr(response, "model_version", None))
        parsed = json.loads(response.text or "{}")
        if not isinstance(parsed, dict):
            parsed = {}
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
