"""Per-session driver: choose ingestion path, run agent graph, push events."""
from __future__ import annotations

import asyncio
import logging

from backend.agents import root
from backend.config import get_settings
from backend.ingestion import live_hls, recorded, transcript, youtube
from backend.ingestion.chunker import cleanup_session
from backend.ingestion.youtube import IngestionError
from backend.runtime.session_manager import Session
from backend.schemas import IngestionMode, OverlayEvent, StreamKind

log = logging.getLogger(__name__)


async def run(session: Session) -> None:
    settings = get_settings()
    await session.emit(OverlayEvent(event="session_started", session_id=session.session_id))

    try:
        kind, _info = await youtube.classify(session.youtube_url)
        session.kind = kind
    except IngestionError as exc:
        log.warning("session %s ingestion error: %s", session.session_id, exc)
        await session.emit(
            OverlayEvent(event="error", session_id=session.session_id, message=str(exc))
        )
        await session.emit(OverlayEvent(event="session_ended", session_id=session.session_id))
        return
    except Exception as exc:
        log.exception("session %s classify crashed: %s", session.session_id, exc)
        await session.emit(
            OverlayEvent(
                event="error",
                session_id=session.session_id,
                message=f"Could not load YouTube URL: {exc}",
            )
        )
        await session.emit(OverlayEvent(event="session_ended", session_id=session.session_id))
        return

    try:
        if session.mode == IngestionMode.TRANSCRIPT:
            statements = transcript.stream_transcript(
                session.youtube_url, session.session_id
            )
            await root.run_transcript_session(
                session_id=session.session_id,
                statements=statements,
                out_queue=session.queue,
                max_claims=settings.max_claims_per_session,
            )
        else:
            if session.kind == StreamKind.LIVE:
                chunks = live_hls.stream_live(
                    session.youtube_url, session.session_id, settings.chunk_seconds
                )
            else:
                chunks = recorded.stream_recorded(
                    session.youtube_url, session.session_id, settings.chunk_seconds
                )
            await root.run_session(
                session_id=session.session_id,
                chunks=chunks,
                out_queue=session.queue,
                max_claims=settings.max_claims_per_session,
            )
    except asyncio.CancelledError:
        log.info("session %s cancelled", session.session_id)
        raise
    except IngestionError as exc:
        log.warning("session %s ingestion error: %s", session.session_id, exc)
        await session.emit(
            OverlayEvent(event="error", session_id=session.session_id, message=str(exc))
        )
        await session.emit(OverlayEvent(event="session_ended", session_id=session.session_id))
    except Exception as exc:
        log.exception("session %s failed: %s", session.session_id, exc)
        await session.emit(
            OverlayEvent(event="error", session_id=session.session_id, message=str(exc))
        )
    finally:
        cleanup_session(session.session_id)
