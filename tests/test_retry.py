"""Tests for the Gemini retry helper."""
import pytest
from google.genai import errors as genai_errors

from backend.agents._retry import with_retry


def _server_error(code: int = 502, msg: str = "Bad Gateway") -> genai_errors.ServerError:
    return genai_errors.ServerError(code, {"message": msg, "status": msg}, None)


def _client_error(code: int = 400, msg: str = "Bad Request") -> genai_errors.ClientError:
    return genai_errors.ClientError(code, {"message": msg, "status": msg}, None)


@pytest.mark.asyncio
async def test_returns_immediately_on_success():
    calls = 0

    async def fn():
        nonlocal calls
        calls += 1
        return "ok"

    result = await with_retry(fn, base_delay=0.0)
    assert result == "ok"
    assert calls == 1


@pytest.mark.asyncio
async def test_retries_then_succeeds_on_transient_5xx():
    calls = 0

    async def fn():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise _server_error(502)
        return "ok"

    result = await with_retry(fn, base_delay=0.0, max_attempts=5)
    assert result == "ok"
    assert calls == 3


@pytest.mark.asyncio
async def test_gives_up_after_max_attempts():
    calls = 0

    async def fn():
        nonlocal calls
        calls += 1
        raise _server_error(503)

    with pytest.raises(genai_errors.ServerError):
        await with_retry(fn, base_delay=0.0, max_attempts=3)
    assert calls == 3


@pytest.mark.asyncio
async def test_does_not_retry_4xx_client_errors():
    calls = 0

    async def fn():
        nonlocal calls
        calls += 1
        raise _client_error(400)

    with pytest.raises(genai_errors.ClientError):
        await with_retry(fn, base_delay=0.0, max_attempts=5)
    assert calls == 1


@pytest.mark.asyncio
async def test_backoff_doubles_each_attempt(monkeypatch):
    delays: list[float] = []

    async def fake_sleep(s: float) -> None:
        delays.append(s)

    monkeypatch.setattr("backend.agents._retry.asyncio.sleep", fake_sleep)

    async def fn():
        raise _server_error(502)

    with pytest.raises(genai_errors.ServerError):
        await with_retry(fn, base_delay=1.0, max_attempts=4)
    # Sleeps occur between attempts, so 3 sleeps for 4 attempts.
    assert delays == [1.0, 2.0, 4.0]
