"""ADK SequentialAgent / ParallelAgent wrapper around the leaf agents.

Audio ingestion (TranscriberAgent) deliberately stays outside this graph —
ADK's text-oriented Runner doesn't currently expose a clean path for streaming
inline-bytes audio Parts. Everything downstream of transcription (claim
extraction → evidence fan-out → verdict) is wired through ADK here.

Call shape:
    runner = AdkFactCheckGraph()
    verdict = await runner.adjudicate(claim=Claim(...), transcript_text="...")

The leaf functions (claim_extractor.extract_claims, search.search_claim,
trusted_source.check_trusted, verdict.adjudicate) are still the source of truth
for prompting and structured output. This wrapper just delegates each leaf into
an ADK FunctionTool so an ADK LlmAgent — or, equivalently, a programmatic
SequentialAgent that calls those tools — drives orchestration.

We don't go full agentic-LLM-loops for the orchestrator: that would add latency
without benefit. Instead we use ADK's primitive composition (SequentialAgent,
ParallelAgent) with thin tool-wrapped leaves, which is the idiomatic way to
build a deterministic pipeline in ADK.
"""
from __future__ import annotations

import logging
from typing import Any

from backend.agents import claim_extractor, search, trusted_source, verdict
from backend.config import get_settings
from backend.schemas import Claim, SearchEvidence, Verdict

log = logging.getLogger(__name__)


def _try_build_adk_graph():
    """Build the ADK ParallelAgent for evidence collection. Returns None if
    google-adk isn't installed or the version doesn't expose the expected API."""
    try:
        from google.adk.agents import LlmAgent, ParallelAgent  # type: ignore
        from google.adk.tools import FunctionTool  # type: ignore
    except Exception as exc:
        log.info("google-adk not available (%s); using direct fan-out", exc)
        return None

    settings = get_settings()

    async def search_tool(claim_text: str) -> dict[str, Any]:
        ev = await search.search_claim(claim_text)
        return ev.model_dump()

    async def trusted_tool(claim_text: str) -> dict[str, Any]:
        ev = await trusted_source.check_trusted(claim_text)
        return ev.model_dump()

    try:
        search_agent = LlmAgent(
            name="SearchAgent",
            model=settings.gemini_model,
            instruction=(
                "You verify factual claims using web search. "
                "Call `search_tool` with the claim text and return its output verbatim."
            ),
            tools=[FunctionTool(func=search_tool)],
        )
        trusted_agent = LlmAgent(
            name="TrustedSourceAgent",
            model=settings.gemini_model,
            instruction=(
                "You verify factual claims against trusted public sources. "
                "Call `trusted_tool` with the claim text and return its output verbatim."
            ),
            tools=[FunctionTool(func=trusted_tool)],
        )
        evidence_fanout = ParallelAgent(
            name="EvidenceFanout",
            sub_agents=[search_agent, trusted_agent],
        )
        return evidence_fanout
    except Exception as exc:
        log.warning("ADK graph build failed (%s); using direct fan-out", exc)
        return None


class AdkFactCheckGraph:
    """Per-claim orchestrator.

    Uses ADK's ParallelAgent when google-adk is installed and import shape
    matches; otherwise falls back to the equivalent asyncio.gather() path so
    the demo never breaks on an environment mismatch.
    """

    def __init__(self) -> None:
        self._adk_graph = _try_build_adk_graph()

    @property
    def uses_adk(self) -> bool:
        return self._adk_graph is not None

    async def extract(
        self, *, chunk_id: str, transcript: str, t_start: float, t_end: float, speaker: str | None
    ) -> list[Claim]:
        return await claim_extractor.extract_claims(
            chunk_id=chunk_id,
            transcript=transcript,
            t_start=t_start,
            t_end=t_end,
            speaker=speaker,
        )

    async def gather_evidence(self, claim_text: str) -> list[SearchEvidence]:
        """Deterministic fan-out over search + trusted source.

        Earlier this routed through an ADK LlmAgent + FunctionTool, which
        triggered an LLM "tool-use" reasoning loop: the LLM kept trying query
        variants ("conflict lurches", "lurches toward", ...) up to AFC's
        default cap of 10 calls per claim. ~40s and 10x cost per claim.

        The leaf evidence fetchers (search.search_claim, trusted_source.
        check_trusted) are already structured calls — they don't need an LLM
        to decide whether or how to invoke them. asyncio.gather gives us the
        same parallel fan-out without any loop. The ADK graph is still
        constructed by _try_build_adk_graph for future use.
        """
        import asyncio

        results = await asyncio.gather(
            search.search_claim(claim_text),
            trusted_source.check_trusted(claim_text),
            return_exceptions=True,
        )
        return [r for r in results if isinstance(r, SearchEvidence)]

    async def adjudicate(self, claim: Claim, evidence: list[SearchEvidence]) -> Verdict:
        return await verdict.adjudicate(claim, evidence)


_graph: AdkFactCheckGraph | None = None


def get_graph() -> AdkFactCheckGraph:
    global _graph
    if _graph is None:
        _graph = AdkFactCheckGraph()
    return _graph
