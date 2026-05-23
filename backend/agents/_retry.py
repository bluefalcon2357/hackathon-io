"""Retry helper for transient Gemini server errors.

Gemini's edge occasionally returns 5xx (502 Bad Gateway, 503 Service Unavailable)
during load spikes or rolling deploys. The google-genai SDK's built-in tenacity
wrapper doesn't retry these by default, so a single blip kills the in-flight
call. Wrapping every `generate_content` site in `with_retry(...)` adds a small
exponential backoff that masks short-lived outages.

4xx errors (auth, bad request, content blocked) are deliberately NOT retried —
those are deterministic failures a retry won't fix.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, TypeVar

log = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BASE_DELAY = 1.0


def _retryable_excs() -> tuple[type[BaseException], ...]:
    """Resolved lazily so this module imports cleanly without google-genai."""
    try:
        from google.genai import errors as genai_errors
    except Exception:
        return ()
    return (genai_errors.ServerError,)


async def with_retry(
    coro_fn: Callable[[], Awaitable[T]],
    *,
    label: str = "gemini",
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    base_delay: float = DEFAULT_BASE_DELAY,
) -> T:
    """Retry an async Gemini call on transient 5xx server errors.

    Backoff doubles each attempt: ``base_delay * 2**attempt_index``. With the
    defaults that's 1s then 2s between attempts (worst case ≈ 3s of sleep on
    top of 3 RPC round-trips).
    """
    retryable = _retryable_excs()
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            return await coro_fn()
        except retryable as exc:
            last_exc = exc
            if attempt + 1 == max_attempts:
                log.warning("%s: %s after %d attempts", label, exc, max_attempts)
                raise
            delay = base_delay * (2 ** attempt)
            log.warning(
                "%s: %s; retrying in %.1fs (attempt %d/%d)",
                label, exc, delay, attempt + 2, max_attempts,
            )
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise last_exc
