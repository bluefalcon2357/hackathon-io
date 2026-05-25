"""Runner-level test: transcript mode falls back to audio when captions fail."""
from typing import AsyncIterator

import pytest

from backend.agents import claim_extractor, search, transcriber, trusted_source, verdict
from backend.ingestion import recorded, transcript, youtube
from backend.ingestion.transcript import NoCaptionsError
from backend.runtime import runner
from backend.runtime.session_manager import manager
from backend.schemas import Chunk, Claim, IngestionMode, SearchEvidence, StreamKind, Verdict


@pytest.fixture
def stub_pipeline(monkeypatch):
    async def fake_classify(url):
        return StreamKind.RECORDED, {}

    async def fake_prepare_statements(url, session_id):
        raise NoCaptionsError(
            "YouTube rate-limited the caption endpoint (HTTP 429). "
            "Wait a minute and retry, or switch to Audio mode."
        )

    async def fake_stream_recorded(url, session_id, chunk_seconds) -> AsyncIterator[tuple[Chunk, bytes]]:
        yield (
            Chunk(
                chunk_id=f"{session_id}:0",
                session_id=session_id,
                t_start=0.0,
                t_end=5.0,
            ),
            b"\x00\x00\x00",
        )

    async def fake_transcribe(audio_bytes, mime_type="audio/ogg"):
        return transcriber.Transcription(text="Inflation hit 9%.", speaker="speaker_1")

    async def fake_extract_claims(*, chunk_id, transcript, t_start, t_end, speaker):
        return [
            Claim(
                claim_id=f"{chunk_id}:c0",
                chunk_id=chunk_id,
                text=transcript,
                t_start=t_start,
                t_end=t_end,
                check_worthy=True,
                confidence=0.9,
                speaker=speaker,
            )
        ]

    async def fake_search_claim(claim_text):
        return SearchEvidence(source="google_search", snippet="x", supports="supports")

    async def fake_check_trusted(claim_text):
        return SearchEvidence(source="trusted", snippet="y", supports="unclear")

    async def fake_adjudicate(claim, evidence):
        return Verdict(
            claim_id=claim.claim_id, status="green", summary="ok.", citations=evidence,
        )

    monkeypatch.setattr(youtube, "classify", fake_classify)
    monkeypatch.setattr(transcript, "prepare_statements", fake_prepare_statements)
    monkeypatch.setattr(recorded, "stream_recorded", fake_stream_recorded)
    monkeypatch.setattr(transcriber, "transcribe", fake_transcribe)
    monkeypatch.setattr(claim_extractor, "extract_claims", fake_extract_claims)
    monkeypatch.setattr(search, "search_claim", fake_search_claim)
    monkeypatch.setattr(trusted_source, "check_trusted", fake_check_trusted)
    monkeypatch.setattr(verdict, "adjudicate", fake_adjudicate)


@pytest.mark.asyncio
async def test_transcript_429_falls_back_to_audio(stub_pipeline):
    session = manager.create(
        "https://youtu.be/test", StreamKind.RECORDED, mode=IngestionMode.TRANSCRIPT,
    )
    try:
        await runner.run(session)

        events = []
        while not session.queue.empty():
            events.append(session.queue.get_nowait())

        kinds = [e.event for e in events]
        # We expect: session_started → error (fallback notice) → claim_detected
        # → verdict → session_ended (from the audio path, not from a hard fail).
        assert kinds[0] == "session_started"
        assert "error" in kinds
        fallback_msg = next(e for e in events if e.event == "error").message
        assert "Falling back to Audio mode" in fallback_msg
        assert "claim_detected" in kinds, (
            f"audio fallback did not produce a claim_detected event; got {kinds}"
        )
        assert "verdict" in kinds
        assert kinds[-1] == "session_ended"
    finally:
        manager.remove(session.session_id)
