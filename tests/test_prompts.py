"""Guards against the str.format() KeyError bug — every prompt template that
gets .format()'d needs its literal JSON braces escaped (`{{` / `}}`).
"""
from backend.agents.claim_extractor import EXTRACTION_PROMPT
from backend.agents.search import _SEARCH_PROMPT
from backend.agents.verdict import _VERDICT_PROMPT


def test_claim_extractor_prompt_formats():
    out = EXTRACTION_PROMPT.format(t_start=0, t_end=5, transcript="hi")
    assert "{t_start}" not in out
    assert '"claims"' in out


def test_search_prompt_formats():
    out = _SEARCH_PROMPT.format(claim="x")
    assert "{claim}" not in out
    assert '"snippet"' in out


def test_verdict_prompt_formats():
    out = _VERDICT_PROMPT.format(claim="x", evidence="y")
    assert "{claim}" not in out
    assert '"status"' in out
