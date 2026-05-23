"""Sentence-buffering and VTT-parsing tests for transcript-mode ingestion."""
from backend.ingestion.transcript import (
    buffer_into_statements,
    parse_vtt,
)


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
