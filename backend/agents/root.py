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
    # Caller (process_chunk) already ran dedup before scheduling this task.
    del deduper  # kept in signature for future use; suppress unused warning
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
    try:
        evidence = await asyncio.wait_for(graph.gather_evidence(claim.text), timeout=6.0)
    except asyncio.TimeoutError:
        evidence = []
        log.warning("evidence gather timed out for claim: %s", claim.text[:60])
    try:
        final = await asyncio.wait_for(graph.adjudicate(claim, evidence), timeout=4.0)
    except asyncio.TimeoutError:
        from backend.schemas import Verdict
        final = Verdict(
            claim_id=claim.claim_id,
            status="yellow",
            summary="Verdict timed out.",
            citations=evidence,
        )
        log.warning("adjudicate timed out for claim: %s", claim.text[:60])
    await cache.put(claim.text, final)
    await out_queue.put(
        OverlayEvent(event="verdict", session_id=session_id, claim=claim, verdict=final)
    )


async def _process_text(
    *,
    session_id: str,
    chunk: Chunk,
    transcript: str,
    speaker: str | None,
    deduper: ClaimDeduper,
    cache: VerdictCache,
    out_queue: asyncio.Queue,
    max_claims_remaining: int,
    background_tasks: list[asyncio.Task],
) -> int:
    """Run extract → fan-out for already-transcribed text. Returns claims processed.

    Per-claim search/verdict work is fire-and-forget — appended to
    `background_tasks` so the run_session caller can await them at
    end-of-session, but not blocking this chunk's return. That way chunk N+1
    can start being processed while chunk N's verdicts are still resolving.
    """
    if not transcript.strip():
        return 0

    claims = await get_graph().extract(
        chunk_id=chunk.chunk_id,
        transcript=transcript,
        t_start=chunk.t_start,
        t_end=chunk.t_end,
        speaker=speaker,
    )

    processed = 0
    for claim in claims:
        if processed >= max_claims_remaining:
            break
        if not claim.check_worthy:
            continue
        if deduper.seen(claim.text):
            continue
        context.update_context(session_id, speaker=claim.speaker, claim_text=claim.text)
        background_tasks.append(
            asyncio.create_task(
                _process_claim(
                    session_id=session_id,
                    claim=claim,
                    deduper=deduper,
                    cache=cache,
                    out_queue=out_queue,
                )
            )
        )
        processed += 1

    return processed


async def process_chunk(
    *,
    session_id: str,
    chunk: Chunk,
    audio_bytes: bytes,
    deduper: ClaimDeduper,
    cache: VerdictCache,
    out_queue: asyncio.Queue,
    max_claims_remaining: int,
    background_tasks: list[asyncio.Task],
) -> int:
    """Run transcribe → extract → fan-out for one audio chunk."""
    transcription = await transcriber.transcribe(audio_bytes, mime_type=chunk.mime_type)
    return await _process_text(
        session_id=session_id,
        chunk=chunk,
        transcript=transcription.text,
        speaker=transcription.speaker,
        deduper=deduper,
        cache=cache,
        out_queue=out_queue,
        max_claims_remaining=max_claims_remaining,
        background_tasks=background_tasks,
    )


async def run_session(
    *,
    session_id: str,
    chunks: AsyncIterator[tuple[Chunk, bytes]],
    out_queue: asyncio.Queue,
    max_claims: int,
) -> None:
    """Drive the full per-chunk pipeline for audio mode."""
    deduper = ClaimDeduper()
    cache = VerdictCache()
    claims_processed = 0
    background_tasks: list[asyncio.Task] = []

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
                    background_tasks=background_tasks,
                )
            except Exception as exc:
                log.exception("chunk %s failed: %s", chunk.chunk_id, exc)
                await out_queue.put(
                    OverlayEvent(event="error", session_id=session_id, message=str(exc))
                )
    finally:
        # Drain in-flight claim processing so all verdicts make it onto the
        # SSE stream before we emit session_ended.
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        context.reset_context(session_id)
        await out_queue.put(OverlayEvent(event="session_ended", session_id=session_id))


async def run_transcript_session(
    *,
    session_id: str,
    statements: AsyncIterator[tuple[Chunk, str]],
    out_queue: asyncio.Queue,
    max_claims: int,
) -> None:
    """Drive the pipeline for transcript mode — text in, no audio transcription."""
    deduper = ClaimDeduper()
    cache = VerdictCache()
    claims_processed = 0
    background_tasks: list[asyncio.Task] = []

    try:
        async for chunk, text in statements:
            remaining = max_claims - claims_processed
            if remaining <= 0:
                log.info("session %s hit claim cap (%d)", session_id, max_claims)
                break
            try:
                claims_processed += await _process_text(
                    session_id=session_id,
                    chunk=chunk,
                    transcript=text,
                    speaker=None,
                    deduper=deduper,
                    cache=cache,
                    out_queue=out_queue,
                    max_claims_remaining=remaining,
                    background_tasks=background_tasks,
                )
            except Exception as exc:
                log.exception("statement %s failed: %s", chunk.chunk_id, exc)
                await out_queue.put(
                    OverlayEvent(event="error", session_id=session_id, message=str(exc))
                )
    finally:
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        context.reset_context(session_id)
        await out_queue.put(OverlayEvent(event="session_ended", session_id=session_id))
