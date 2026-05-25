"""Transcript mode verifies EVERY statement of the full transcript.

Unlike audio/video modes (which extract a filtered set of check-worthy claims),
transcript mode sends each buffered statement straight into the verdict
pipeline — opinions and questions included — with bounded concurrency.
"""
import asyncio
from typing import AsyncIterator

import pytest

from backend.agents import root, search, trusted_source, verdict
from backend.agents.root import run_transcript_session, statement_to_claim
from backend.schemas import Chunk, SearchEvidence, Verdict


@pytest.fixture
def stub_leaves(monkeypatch):
    async def fake_search_claim(claim_text):
        return SearchEvidence(source="google_search", snippet="x", supports="unclear")

    async def fake_check_trusted(claim_text):
        return SearchEvidence(source="trusted", snippet="y", supports="unclear")

    async def fake_adjudicate(claim, evidence):
        return Verdict(
            claim_id=claim.claim_id, status="yellow", summary="checked.", citations=evidence
        )

    monkeypatch.setattr(search, "search_claim", fake_search_claim)
    monkeypatch.setattr(trusted_source, "check_trusted", fake_check_trusted)
    monkeypatch.setattr(verdict, "adjudicate", fake_adjudicate)


def _statements(items: list[str]) -> AsyncIterator[tuple[Chunk, str]]:
    async def gen():
        for i, text in enumerate(items):
            yield (
                Chunk(
                    chunk_id=f"s:{i}",
                    session_id="s",
                    t_start=float(i * 3),
                    t_end=float(i * 3 + 3),
                    mime_type="text/plain",
                ),
                text,
            )

    return gen()


def _drain(queue: asyncio.Queue) -> list:
    events = []
    while not queue.empty():
        events.append(queue.get_nowait())
    return events


def test_statement_to_claim_is_verbatim_and_check_worthy():
    chunk = Chunk(chunk_id="s:0", session_id="s", t_start=1.0, t_end=4.0, mime_type="text/plain")
    claim = statement_to_claim(chunk, "  This is a statement.  ")
    assert claim is not None
    assert claim.text == "This is a statement."
    assert claim.check_worthy is True
    assert claim.t_start == 1.0
    assert claim.t_end == 4.0


def test_statement_to_claim_skips_blank():
    chunk = Chunk(chunk_id="s:0", session_id="s", t_start=0.0, t_end=1.0, mime_type="text/plain")
    assert statement_to_claim(chunk, "   ") is None


@pytest.mark.asyncio
async def test_transcript_verifies_every_statement(stub_leaves):
    # An opinion and a question are included on purpose: nothing is filtered out.
    lines = [
        "Inflation hit 9% last year.",
        "I think this is a great policy.",
        "Is the sky blue?",
    ]
    queue: asyncio.Queue = asyncio.Queue()
    await run_transcript_session(
        session_id="s", statements=_statements(lines), out_queue=queue, max_claims=100
    )
    events = _drain(queue)
    kinds = [e.event for e in events]
    assert kinds.count("claim_detected") == 3
    assert kinds.count("verdict") == 3
    assert kinds[-1] == "session_ended"
    detected = sorted(e.claim.text for e in events if e.event == "claim_detected")
    assert detected == sorted(lines), "every statement should be checked verbatim"


@pytest.mark.asyncio
async def test_transcript_respects_statement_cap(stub_leaves):
    lines = [f"Statement number {i}." for i in range(10)]
    queue: asyncio.Queue = asyncio.Queue()
    await run_transcript_session(
        session_id="s", statements=_statements(lines), out_queue=queue, max_claims=3
    )
    events = _drain(queue)
    kinds = [e.event for e in events]
    assert kinds.count("claim_detected") == 3
    assert kinds.count("verdict") == 3
    assert kinds[-1] == "session_ended"


@pytest.mark.asyncio
async def test_transcript_bounds_concurrency(monkeypatch):
    monkeypatch.setattr(
        root, "get_settings", lambda: type("S", (), {"max_concurrent_checks": 2})
    )

    active = 0
    peak = 0

    async def fake_search_claim(claim_text):
        return SearchEvidence(source="google_search", snippet="x", supports="unclear")

    async def fake_check_trusted(claim_text):
        return SearchEvidence(source="trusted", snippet="y", supports="unclear")

    async def fake_adjudicate(claim, evidence):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.02)
        active -= 1
        return Verdict(claim_id=claim.claim_id, status="yellow", summary="ok.", citations=evidence)

    monkeypatch.setattr(search, "search_claim", fake_search_claim)
    monkeypatch.setattr(trusted_source, "check_trusted", fake_check_trusted)
    monkeypatch.setattr(verdict, "adjudicate", fake_adjudicate)

    lines = [f"Statement {i}." for i in range(8)]
    queue: asyncio.Queue = asyncio.Queue()
    await run_transcript_session(
        session_id="s", statements=_statements(lines), out_queue=queue, max_claims=100
    )
    assert peak <= 2, f"concurrency exceeded the cap: peak={peak}"
    assert [e.event for e in _drain(queue)].count("verdict") == 8
