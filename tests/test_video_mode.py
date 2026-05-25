"""Direct-video mode: extractor parsing + runner branching + live fallback."""
from typing import AsyncIterator

import pytest

from backend.agents import (
    claim_extractor,
    search,
    transcriber,
    trusted_source,
    verdict,
    video_extractor,
)
from backend.agents.video_extractor import _parse_claims
from backend.ingestion import youtube
from backend.runtime import runner
from backend.runtime.session_manager import manager
from backend.schemas import (
    Chunk,
    Claim,
    IngestionMode,
    SearchEvidence,
    StreamKind,
    Verdict,
)


def test_parse_claims_extracts_each_entry():
    raw = (
        '{"claims": ['
        '{"text": "Inflation hit 9% last year.", '
        '"t_start": 12.5, "t_end": 15.0, '
        '"speaker": "speaker_1", "check_worthy": true, "confidence": 0.9},'
        '{"text": "Apollo 11 landed in 1969.", '
        '"t_start": 30.0, "t_end": 33.0, '
        '"speaker": "speaker_2", "check_worthy": true, "confidence": 0.95}'
        ']}'
    )
    claims = _parse_claims(raw, session_id="s1")
    assert len(claims) == 2
    assert claims[0].text == "Inflation hit 9% last year."
    assert claims[0].t_start == 12.5
    assert claims[0].t_end == 15.0
    assert claims[0].speaker == "speaker_1"
    assert claims[1].text == "Apollo 11 landed in 1969."


def test_parse_claims_handles_empty():
    assert _parse_claims('{"claims": []}', session_id="s1") == []
    assert _parse_claims("", session_id="s1") == []


def test_parse_claims_skips_blank_text():
    raw = '{"claims": [{"text": "  ", "t_start": 0, "t_end": 1}]}'
    assert _parse_claims(raw, session_id="s1") == []


def test_parse_claims_defaults_t_end_to_t_start_when_missing():
    raw = '{"claims": [{"text": "X.", "t_start": 5.0}]}'
    claims = _parse_claims(raw, session_id="s1")
    assert len(claims) == 1
    assert claims[0].t_start == 5.0
    assert claims[0].t_end == 5.0


def test_parse_claims_survives_non_json():
    assert _parse_claims("not json at all", session_id="s1") == []


def test_parse_claims_handles_bad_timestamps():
    raw = '{"claims": [{"text": "X.", "t_start": "garbage", "t_end": null}]}'
    claims = _parse_claims(raw, session_id="s1")
    assert len(claims) == 1
    assert claims[0].t_start == 0.0
    assert claims[0].t_end == 0.0


def test_parse_claims_accepts_mmss_timestamps():
    raw = '{"claims": [{"text": "X.", "t_start": "01:30", "t_end": "01:35"}]}'
    claims = _parse_claims(raw, session_id="s1")
    assert len(claims) == 1
    assert claims[0].t_start == 90.0
    assert claims[0].t_end == 95.0


def test_parse_claims_accepts_hhmmss_timestamps():
    raw = '{"claims": [{"text": "X.", "t_start": "01:02:03", "t_end": "01:02:05"}]}'
    claims = _parse_claims(raw, session_id="s1")
    assert len(claims) == 1
    assert claims[0].t_start == 3723.0
    assert claims[0].t_end == 3725.0


def test_parse_claims_defaults_t_end_to_t_start_for_clock_strings():
    raw = '{"claims": [{"text": "X.", "t_start": "02:00"}]}'
    claims = _parse_claims(raw, session_id="s1")
    assert len(claims) == 1
    assert claims[0].t_start == 120.0
    assert claims[0].t_end == 120.0


def test_parse_claims_keeps_every_statement_unfiltered():
    # Full-transcript mode: even a statement the model flags not check-worthy stays.
    raw = (
        '{"claims": [{"text": "This is just an opinion.", '
        '"t_start": "00:01", "t_end": "00:03", "check_worthy": false}]}'
    )
    claims = _parse_claims(raw, session_id="s1")
    assert len(claims) == 1
    assert claims[0].check_worthy is True


def test_parse_claims_salvages_truncated_transcript():
    # A long transcript can be cut off mid-object by the output-token cap; the
    # completed statements should still be recovered.
    raw = (
        '{"claims": ['
        '{"text": "One.", "t_start": "00:01", "t_end": "00:02"}, '
        '{"text": "Two.", "t_start": "00:03", "t_end": "00:04"}, '
        '{"text": "Thr'
    )
    claims = _parse_claims(raw, session_id="s1")
    assert [c.text for c in claims] == ["One.", "Two."]


@pytest.fixture
def stub_pipeline(monkeypatch):
    async def fake_classify(url):
        return StreamKind.RECORDED, {}

    async def fake_extract_from_video(youtube_url, session_id):
        return [
            Claim(
                claim_id="video:s:0:abcdef",
                chunk_id=f"video:{session_id}",
                text="Unemployment fell to 3.5% last month.",
                t_start=10.0,
                t_end=13.0,
                check_worthy=True,
                confidence=0.9,
                speaker="speaker_1",
            ),
            Claim(
                claim_id="video:s:1:beefee",
                chunk_id=f"video:{session_id}",
                text="The Eiffel Tower is in Paris.",
                t_start=20.0,
                t_end=23.0,
                check_worthy=True,
                confidence=0.95,
                speaker="speaker_1",
            ),
        ]

    async def fake_search_claim(claim_text):
        return SearchEvidence(source="google_search", snippet="ok", supports="supports")

    async def fake_check_trusted(claim_text):
        return SearchEvidence(source="trusted", snippet="ok", supports="unclear")

    async def fake_adjudicate(claim, evidence):
        return Verdict(
            claim_id=claim.claim_id, status="green", summary="Supported.",
            citations=evidence,
        )

    monkeypatch.setattr(youtube, "classify", fake_classify)
    monkeypatch.setattr(
        video_extractor, "extract_claims_from_video", fake_extract_from_video
    )
    monkeypatch.setattr(search, "search_claim", fake_search_claim)
    monkeypatch.setattr(trusted_source, "check_trusted", fake_check_trusted)
    monkeypatch.setattr(verdict, "adjudicate", fake_adjudicate)


@pytest.mark.asyncio
async def test_video_mode_runs_extractor_and_emits_verdicts(stub_pipeline):
    session = manager.create(
        "https://youtu.be/test", StreamKind.RECORDED, mode=IngestionMode.VIDEO,
    )
    try:
        await runner.run(session)
        events = []
        while not session.queue.empty():
            events.append(session.queue.get_nowait())

        kinds = [e.event for e in events]
        assert kinds[0] == "session_started"
        assert kinds.count("claim_detected") == 2
        assert kinds.count("verdict") == 2
        assert kinds[-1] == "session_ended"
    finally:
        manager.remove(session.session_id)


@pytest.mark.asyncio
async def test_video_mode_skips_ytdlp_classify(monkeypatch):
    """Video mode must not invoke the yt-dlp probe — that's what keeps it immune
    to the YouTube 429 that breaks captions/audio. Even if classify would fail,
    the session must still produce verdicts."""
    called = {"classify": False}

    async def boom_classify(url):
        called["classify"] = True
        raise youtube.IngestionError("HTTP Error 429: Too Many Requests")

    async def fake_extract_from_video(youtube_url, session_id):
        return [
            Claim(
                claim_id="video:s:0:abcdef",
                chunk_id="video:s",
                text="A statement from the transcript.",
                t_start=1.0,
                t_end=3.0,
                check_worthy=True,
                confidence=0.9,
                speaker="speaker_1",
            )
        ]

    async def fake_search_claim(claim_text):
        return SearchEvidence(source="google_search", snippet="ok", supports="supports")

    async def fake_check_trusted(claim_text):
        return SearchEvidence(source="trusted", snippet="ok", supports="unclear")

    async def fake_adjudicate(claim, evidence):
        return Verdict(
            claim_id=claim.claim_id, status="green", summary="ok.", citations=evidence
        )

    monkeypatch.setattr(youtube, "classify", boom_classify)
    monkeypatch.setattr(video_extractor, "extract_claims_from_video", fake_extract_from_video)
    monkeypatch.setattr(search, "search_claim", fake_search_claim)
    monkeypatch.setattr(trusted_source, "check_trusted", fake_check_trusted)
    monkeypatch.setattr(verdict, "adjudicate", fake_adjudicate)

    session = manager.create(
        "https://www.youtube.com/watch?v=abc123", StreamKind.RECORDED,
        mode=IngestionMode.VIDEO,
    )
    try:
        await runner.run(session)
        events = []
        while not session.queue.empty():
            events.append(session.queue.get_nowait())

        kinds = [e.event for e in events]
        assert called["classify"] is False, "video mode must not call yt-dlp classify"
        assert "error" not in kinds
        assert kinds.count("claim_detected") == 1
        assert kinds.count("verdict") == 1
        assert kinds[-1] == "session_ended"
    finally:
        manager.remove(session.session_id)


@pytest.fixture
def stub_live_fallback(monkeypatch):
    """Video mode on a livestream should fall back to the audio path."""

    async def fake_classify(url):
        return StreamKind.LIVE, {}

    async def fake_stream_live(url, session_id, chunk_seconds) -> AsyncIterator[
        tuple[Chunk, bytes]
    ]:
        yield (
            Chunk(
                chunk_id=f"{session_id}:0",
                session_id=session_id,
                t_start=0.0,
                t_end=5.0,
            ),
            b"\x00",
        )

    async def fake_transcribe(audio_bytes, mime_type="audio/ogg"):
        return transcriber.Transcription(text="Some claim.", speaker="speaker_1")

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

    from backend.ingestion import live_hls

    monkeypatch.setattr(youtube, "classify", fake_classify)
    monkeypatch.setattr(live_hls, "stream_live", fake_stream_live)
    monkeypatch.setattr(transcriber, "transcribe", fake_transcribe)
    monkeypatch.setattr(claim_extractor, "extract_claims", fake_extract_claims)
    monkeypatch.setattr(search, "search_claim", fake_search_claim)
    monkeypatch.setattr(trusted_source, "check_trusted", fake_check_trusted)
    monkeypatch.setattr(verdict, "adjudicate", fake_adjudicate)


@pytest.mark.asyncio
async def test_video_mode_falls_back_to_audio_on_livestream(stub_live_fallback):
    # Video mode no longer probes via yt-dlp; live is detected from URL shape.
    session = manager.create(
        "https://www.youtube.com/live/abcd1234", StreamKind.RECORDED,
        mode=IngestionMode.VIDEO,
    )
    try:
        await runner.run(session)
        events = []
        while not session.queue.empty():
            events.append(session.queue.get_nowait())

        kinds = [e.event for e in events]
        assert kinds[0] == "session_started"
        assert "error" in kinds
        fallback_msg = next(e for e in events if e.event == "error").message
        assert "Falling back to Audio mode" in fallback_msg
        assert "claim_detected" in kinds, (
            f"audio fallback did not produce a claim_detected event; got {kinds}"
        )
        assert kinds[-1] == "session_ended"
    finally:
        manager.remove(session.session_id)
