"""Root orchestration. Wires the per-chunk and per-claim fan-out.

The orchestration runs as plain asyncio rather than through ADK's Runner because
ADK's text-oriented input pipeline doesn't cleanly accept rolling audio chunk
streams. The leaf agents are still implemented as separate, testable units
(transcriber, claim_extractor, search, trusted_source, verdict) following the
ADK agent-factory pattern; a future iteration can hoist them into a single
ADK SequentialAgent + ParallelAgent graph once audio input through ADK is
verified to work end-to-end.
"""
from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator

from backend.agents import context, transcriber
from backend.agents.graph import get_graph
from backend.runtime.dedupe import ClaimDeduper
from backend.runtime.firestore_cache import VerdictCache
from backend.schemas import Chunk, OverlayEvent

log = logging.getLogger(__name__)


async def _process_claim(
    *,
    session_id: str,
    claim,
    deduper: ClaimDeduper,
    cache: VerdictCache,
    out_queue: asyncio.Queue,
) -> None:
    if deduper.seen(claim.text):
        log.debug("dedup suppressed: %s", claim.text[:60])
        return

    cached = await cache.get(claim.text)
    if cached:
        await out_queue.put(
            OverlayEvent(event="verdict", session_id=session_id, verdict=cached, claim=claim)
        )
        return

    await out_queue.put(
        OverlayEvent(
            event="claim_detected",
            session_id=session_id,
            t_start=claim.t_start,
            t_end=claim.t_end,
            claim=claim,
        )
    )

    graph = get_graph()
    evidence = await graph.gather_evidence(claim.text)
    final = await graph.adjudicate(claim, evidence)
    await cache.put(claim.text, final)
    await out_queue.put(
        OverlayEvent(event="verdict", session_id=session_id, claim=claim, verdict=final)
    )


async def process_chunk(
    *,
    session_id: str,
    chunk: Chunk,
    audio_bytes: bytes,
    deduper: ClaimDeduper,
    cache: VerdictCache,
    out_queue: asyncio.Queue,
    max_claims_remaining: int,
) -> int:
    """Run transcribe → extract → fan-out for one chunk. Returns claims processed."""
    transcription = await transcriber.transcribe(audio_bytes, mime_type=chunk.mime_type)
    if not transcription.text.strip():
        return 0

    claims = await get_graph().extract(
        chunk_id=chunk.chunk_id,
        transcript=transcription.text,
        t_start=chunk.t_start,
        t_end=chunk.t_end,
        speaker=transcription.speaker,
    )

    processed = 0
    claim_tasks = []
    for claim in claims:
        if processed >= max_claims_remaining:
            break
        if not claim.check_worthy:
            continue
        context.update_context(session_id, speaker=claim.speaker, claim_text=claim.text)
        claim_tasks.append(
            _process_claim(
                session_id=session_id,
                claim=claim,
                deduper=deduper,
                cache=cache,
                out_queue=out_queue,
            )
        )
        processed += 1

    if claim_tasks:
        await asyncio.gather(*claim_tasks, return_exceptions=True)
    return processed


async def run_session(
    *,
    session_id: str,
    chunks: AsyncIterator[tuple[Chunk, bytes]],
    out_queue: asyncio.Queue,
    max_claims: int,
) -> None:
    """Drive the full per-chunk pipeline. Pushes OverlayEvents into out_queue."""
    deduper = ClaimDeduper()
    cache = VerdictCache()
    claims_processed = 0

    try:
        async for chunk, data in chunks:
            remaining = max_claims - claims_processed
            if remaining <= 0:
                log.info("session %s hit claim cap (%d)", session_id, max_claims)
                break
            try:
                claims_processed += await process_chunk(
                    session_id=session_id,
                    chunk=chunk,
                    audio_bytes=data,
                    deduper=deduper,
                    cache=cache,
                    out_queue=out_queue,
                    max_claims_remaining=remaining,
                )
            except Exception as exc:
                log.exception("chunk %s failed: %s", chunk.chunk_id, exc)
                await out_queue.put(
                    OverlayEvent(event="error", session_id=session_id, message=str(exc))
                )
    finally:
        context.reset_context(session_id)
        await out_queue.put(OverlayEvent(event="session_ended", session_id=session_id))
