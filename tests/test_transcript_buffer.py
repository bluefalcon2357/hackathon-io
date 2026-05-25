"""Sentence-buffering and VTT-parsing tests for transcript-mode ingestion."""
import pytest

from backend.ingestion import transcript
from backend.ingestion.transcript import (
    NoCaptionsError,
    _cache_get,
    _cache_put,
    _classify_yt_dlp_error,
    _clear_caption_cache,
    buffer_into_statements,
    parse_vtt,
)


@pytest.fixture(autouse=True)
def _isolate_caption_cache():
    _clear_caption_cache()
    yield
    _clear_caption_cache()


def test_buffer_flushes_on_sentence_terminator():
    cues = [
        (0.0, 1.0, "We landed on the moon"),
        (1.0, 2.0, "in 1969"),
        (2.0, 3.0, "during Apollo 11."),
        (3.0, 4.0, "It was a Sunday"),
        (4.0, 5.0, "in July."),
    ]
    out = buffer_into_statements(cues)
    assert len(out) == 2
    assert out[0][2].endswith("Apollo 11.")
    assert out[0][0] == 0.0
    assert out[0][1] == 3.0
    assert out[1][2].endswith("July.")


def test_buffer_flushes_on_time_cap():
    cues = [(float(i), float(i + 1), f"word{i}") for i in range(20)]
    out = buffer_into_statements(cues, max_seconds=5.0, max_chars=10_000)
    assert len(out) >= 4
    for t0, t1, _ in out:
        assert t1 - t0 <= 5.0 + 1.0


def test_buffer_flushes_on_char_cap():
    long_word = "supercalifragilisticexpialidocious"
    cues = [(float(i), float(i + 1), long_word) for i in range(20)]
    out = buffer_into_statements(cues, max_seconds=1_000.0, max_chars=100)
    assert len(out) >= 5
    for _, _, text in out:
        assert len(text) <= 100 + len(long_word) + 1


def test_buffer_handles_quoted_terminator():
    cues = [
        (0.0, 1.0, 'He said "we won.'),
        (1.0, 2.0, '" Then he left.'),
    ]
    out = buffer_into_statements(cues)
    assert len(out) >= 1
    full = " ".join(s for _, _, s in out)
    assert "we won" in full
    assert "left" in full


def test_buffer_empty_input():
    assert buffer_into_statements([]) == []


def test_parse_vtt_simple():
    content = """WEBVTT

00:00:00.000 --> 00:00:02.500
Hello world.

00:00:02.500 --> 00:00:05.000
This is a test.
"""
    cues = parse_vtt(content)
    assert len(cues) == 2
    assert cues[0] == (0.0, 2.5, "Hello world.")
    assert cues[1] == (2.5, 5.0, "This is a test.")


def test_parse_vtt_strips_tags():
    content = """WEBVTT

00:00:00.000 --> 00:00:02.000
<c.colorE5E5E5>Hello</c> <c>world</c>.
"""
    cues = parse_vtt(content)
    assert len(cues) == 1
    assert cues[0][2] == "Hello world."


def test_parse_vtt_dedupes_rolling_autocaptions():
    """YouTube auto-captions repeat the previous cue's text plus new words."""
    content = """WEBVTT

00:00:00.000 --> 00:00:01.000
the cat

00:00:01.000 --> 00:00:02.000
the cat sat

00:00:02.000 --> 00:00:03.000
the cat sat on the mat.
"""
    cues = parse_vtt(content)
    joined = " ".join(c[2] for c in cues)
    assert joined.count("the cat") == 1
    assert "sat" in joined
    assert "mat" in joined


def test_buffer_preserves_time_window():
    cues = [
        (10.0, 11.0, "Inflation hit 9% last year."),
        (11.0, 12.0, "That was a 40-year high."),
    ]
    out = buffer_into_statements(cues)
    assert out[0][0] == 10.0
    assert out[0][1] == 11.0
    assert out[1][0] == 11.0
    assert out[1][1] == 12.0


def test_classify_rate_limit_error_is_actionable():
    err = _classify_yt_dlp_error(
        "ERROR: Unable to download video subtitles for 'en': "
        "HTTP Error 429: Too Many Requests"
    )
    assert isinstance(err, NoCaptionsError)
    assert "429" in str(err)
    # Points at the mode that bypasses YouTube's rate limit, not Audio (which
    # hits the same wall from this server's IP).
    assert "Gemini video" in str(err)


def test_classify_bot_wall_error_mentions_cookies():
    err = _classify_yt_dlp_error(
        "ERROR: Sign in to confirm you're not a bot. Use --cookies-from-browser..."
    )
    assert "cookies" in str(err).lower() or "YT_DLP_COOKIES" in str(err)


def test_classify_unknown_error_passes_through():
    err = _classify_yt_dlp_error("ERROR: Some other yt-dlp failure")
    assert isinstance(err, NoCaptionsError)
    assert "Some other yt-dlp failure" in str(err)


def test_classify_truncates_long_stderr():
    long_err = "X" * 1000
    err = _classify_yt_dlp_error(long_err)
    # Message should be bounded so we don't dump 1KB into the SSE stream.
    assert len(str(err)) < 500


def test_caption_cache_hit_returns_stored_cues():
    cues = [(0.0, 1.0, "Hello.")]
    _cache_put("https://example.com/abc", cues)
    assert _cache_get("https://example.com/abc") == cues


def test_caption_cache_miss_returns_none():
    assert _cache_get("https://example.com/never-fetched") is None


def test_caption_cache_eviction_respects_max(monkeypatch):
    monkeypatch.setattr(transcript, "_CAPTION_CACHE_MAX_ENTRIES", 3)
    for i in range(5):
        _cache_put(f"url-{i}", [(0.0, 1.0, f"cue {i}")])
    # Oldest two should have been evicted.
    assert _cache_get("url-0") is None
    assert _cache_get("url-1") is None
    assert _cache_get("url-4") is not None


def test_caption_cache_ttl_expires(monkeypatch):
    monkeypatch.setattr(transcript, "_CAPTION_CACHE_TTL_SECONDS", 0.0)
    _cache_put("url-stale", [(0.0, 1.0, "stale")])
    assert _cache_get("url-stale") is None
