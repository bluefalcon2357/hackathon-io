from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field

from backend.schemas import IngestionMode, OverlayEvent, StreamKind

log = logging.getLogger(__name__)


@dataclass
class Session:
    session_id: str
    youtube_url: str
    kind: StreamKind
    mode: IngestionMode = IngestionMode.AUDIO
    queue: asyncio.Queue = field(default_factory=lambda: asyncio.Queue(maxsize=2048))
    task: asyncio.Task | None = None
    ended: bool = False

    async def emit(self, event: OverlayEvent) -> None:
        await self.queue.put(event)
        if event.event == "session_ended":
            self.ended = True


class SessionManager:
    def __init__(self) -> None:
        self._sessions: dict[str, Session] = {}

    def create(
        self,
        youtube_url: str,
        kind: StreamKind,
        mode: IngestionMode = IngestionMode.AUDIO,
    ) -> Session:
        session_id = uuid.uuid4().hex[:12]
        session = Session(
            session_id=session_id, youtube_url=youtube_url, kind=kind, mode=mode,
        )
        self._sessions[session_id] = session
        log.info("created session %s (%s, %s)", session_id, kind.value, mode.value)
        return session

    def get(self, session_id: str) -> Session | None:
        return self._sessions.get(session_id)

    def remove(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)


manager = SessionManager()
