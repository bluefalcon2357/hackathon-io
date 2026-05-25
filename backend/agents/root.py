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
import uuid
from typing import AsyncIterator

from backend.agents import context, transcriber, video_extractor
from backend.agents.graph import get_graph
from backend.config import get_settings
from backend.runtime.dedupe import ClaimDeduper
from backend.runtime.firestore_cache import VerdictCache
from backend.schemas import Chunk, Claim, OverlayEvent, Verdict

log = logging.getLogger(__name__)


async def _resolve_verdict(claim: Claim) -> Verdict:
    """Run evidence gather → adjudicate for one claim, with per-stage timeouts.

    Gemini + google_search grounding routinely takes 8-15s per claim; adjudicate
    is a single non-grounded call so it's faster. These caps are upper bounds —
    most claims return well under them.
    """
    graph = get_graph()
    try:
        evidence = await asyncio.wait_for(graph.gather_evidence(claim.text), timeout=20.0)
    except asyncio.TimeoutError:
        log.warning("evidence gather timed out for claim: %s", claim.text[:60])
        return Verdict(
            claim_id=claim.claim_id,
            status="yellow",
            summary="Evidence gather timed out.",
            citations=[],
        )
    try:
        return await asyncio.wait_for(graph.adjudicate(claim, evidence), timeout=10.0)
    except asyncio.TimeoutError:
        log.warning("adjudicate timed out for claim: %s", claim.text[:60])
        return Verdict(
            claim_id=claim.claim_id,
            status="yellow",
            summary="Verdict timed out.",
            citations=evidence,
        )


async def _process_claim(
    *,
    session_id: str,
    claim,
    deduper: ClaimDeduper,
    cache: VerdictCache,
    out_queue: asyncio.Queue,
    semaphore: asyncio.Semaphore | None = None,
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

    # The semaphore (transcript mode) bounds how many grounded checks run at
    # once; without it (audio/video) the leaf timeouts already cap each task.
    if semaphore is not None:
        async with semaphore:
            final = await _resolve_verdict(claim)
    else:
        final = await _resolve_verdict(claim)
    await cache.put(claim.text, final)
    await out_queue.put(
        OverlayEvent(event="verdict", session_id=session_id, claim=claim, verdict=final)
    )


def _dispatch_claims(
    *,
    session_id: str,
    claims: list[Claim],
    deduper: ClaimDeduper,
    cache: VerdictCache,
    out_queue: asyncio.Queue,
    max_claims_remaining: int,
    background_tasks: list[asyncio.Task],
    semaphore: asyncio.Semaphore | None = None,
) -> int:
    """Schedule fire-and-forget per-claim search/verdict tasks.

    Returns the number actually scheduled (excludes filtered/duplicate claims).
    Per-claim work is appended to ``background_tasks`` so the run_session
    caller can await them at end-of-session without blocking the next chunk.
    """
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
                    semaphore=semaphore,
                )
            )
        )
        processed += 1
    return processed


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
    """Run extract → fan-out for already-transcribed text. Returns claims processed."""
    if not transcript.strip():
        return 0

    claims = await get_graph().extract(
        chunk_id=chunk.chunk_id,
        transcript=transcript,
        t_start=chunk.t_start,
        t_end=chunk.t_end,
        speaker=speaker,
    )
    return _dispatch_claims(
        session_id=session_id,
        claims=claims,
        deduper=deduper,
        cache=cache,
        out_queue=out_queue,
        max_claims_remaining=max_claims_remaining,
        background_tasks=background_tasks,
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


def statement_to_claim(chunk: Chunk, text: str) -> Claim | None:
    """Wrap a complete-sentence transcript statement as a check-worthy claim.

    Transcript mode checks the *entire* transcript, so we skip claim extraction
    (which filters out everything that isn't a salient factual claim) and send
    each statement straight into the verdict pipeline verbatim.
    """
    text = text.strip()
    if not text:
        return None
    return Claim(
        claim_id=f"{chunk.chunk_id}:{uuid.uuid4().hex[:6]}",
        chunk_id=chunk.chunk_id,
        text=text,
        t_start=chunk.t_start,
        t_end=chunk.t_end,
        check_worthy=True,
        confidence=1.0,
        speaker=None,
    )


async def run_transcript_session(
    *,
    session_id: str,
    statements: AsyncIterator[tuple[Chunk, str]],
    out_queue: asyncio.Queue,
    max_claims: int,
) -> None:
    """Drive transcript mode: verify *every* statement of the full transcript.

    No claim-extraction filter — each buffered statement becomes a claim and
    runs the full search → verdict pipeline. A semaphore bounds concurrency so
    a long transcript doesn't fan out hundreds of grounded Gemini calls at once.
    """
    deduper = ClaimDeduper()
    cache = VerdictCache()
    semaphore = asyncio.Semaphore(get_settings().max_concurrent_checks)
    claims_processed = 0
    background_tasks: list[asyncio.Task] = []

    try:
        async for chunk, text in statements:
            remaining = max_claims - claims_processed
            if remaining <= 0:
                log.info("session %s hit statement cap (%d)", session_id, max_claims)
                break
            claim = statement_to_claim(chunk, text)
            if claim is None:
                continue
            try:
                claims_processed += _dispatch_claims(
                    session_id=session_id,
                    claims=[claim],
                    deduper=deduper,
                    cache=cache,
                    out_queue=out_queue,
                    max_claims_remaining=remaining,
                    background_tasks=background_tasks,
                    semaphore=semaphore,
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


async def run_video_session(
    *,
    session_id: str,
    youtube_url: str,
    out_queue: asyncio.Queue,
    max_claims: int,
) -> None:
    """Drive the pipeline for direct-video mode.

    Gemini receives the YouTube URL directly and returns the full transcript
    (every statement, with timestamps) in one shot. Each statement then fans
    out through the existing dedupe → search → verdict pipeline, with bounded
    concurrency so a long transcript doesn't trip rate limits.
    """
    deduper = ClaimDeduper()
    cache = VerdictCache()
    semaphore = asyncio.Semaphore(get_settings().max_concurrent_checks)
    background_tasks: list[asyncio.Task] = []

    try:
        claims = await video_extractor.extract_claims_from_video(
            youtube_url, session_id
        )
        log.info(
            "video mode: extracted %d statements for session %s",
            len(claims), session_id,
        )
        _dispatch_claims(
            session_id=session_id,
            claims=claims,
            deduper=deduper,
            cache=cache,
            out_queue=out_queue,
            max_claims_remaining=max_claims,
            background_tasks=background_tasks,
            semaphore=semaphore,
        )
    except Exception as exc:
        log.exception("session %s video mode failed: %s", session_id, exc)
        await out_queue.put(
            OverlayEvent(event="error", session_id=session_id, message=str(exc))
        )
    finally:
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        context.reset_context(session_id)
        await out_queue.put(OverlayEvent(event="session_ended", session_id=session_id))
