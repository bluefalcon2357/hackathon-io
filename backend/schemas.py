from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field


class StreamKind(str, Enum):
    RECORDED = "recorded"
    LIVE = "live"


class IngestionMode(str, Enum):
    AUDIO = "audio"
    VIDEO = "video"


class Chunk(BaseModel):
    chunk_id: str
    session_id: str
    t_start: float
    t_end: float
    mime_type: str = "audio/ogg"


class Claim(BaseModel):
    claim_id: str
    chunk_id: str
    text: str
    t_start: float
    t_end: float
    check_worthy: bool = True
    confidence: float = Field(ge=0.0, le=1.0, default=0.5)
    speaker: str | None = None


class SearchEvidence(BaseModel):
    source: Literal["google_search", "trusted"]
    url: str | None = None
    domain: str | None = None
    snippet: str
    supports: Literal["supports", "contradicts", "unrelated", "unclear"] = "unclear"


class Verdict(BaseModel):
    claim_id: str
    status: Literal["green", "yellow", "red"] = "yellow"
    summary: str
    citations: list[SearchEvidence] = Field(default_factory=list)


class OverlayEvent(BaseModel):
    event: Literal["session_started", "claim_detected", "verdict", "session_ended", "error"]
    session_id: str
    t_start: float | None = None
    t_end: float | None = None
    claim: Claim | None = None
    verdict: Verdict | None = None
    message: str | None = None


class SessionRequest(BaseModel):
    youtube_url: str
    kind: StreamKind | None = None
    mode: IngestionMode = IngestionMode.AUDIO


class SessionResponse(BaseModel):
    session_id: str
    kind: StreamKind
    title: str | None = None
    duration: float | None = None
