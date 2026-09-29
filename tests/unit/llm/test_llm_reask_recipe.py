"""The docs' recipe for answers that miss a format works as written (F-009).

The code between ``<!-- llm-reask:begin -->`` and ``<!-- llm-reask:end -->`` in
docs/patterns/llm.md is read from the page and run against the fake provider.
It checks what the page promises: only the failed prompts are asked again, at most
``tries`` more times; the extra calls count against the budget and are in the run's
call log; and a recorded run replays call for call, a repeated prompt included.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from ubunye import llm
from ubunye.core.errors import LLMBudgetError

PAGE = Path(__file__).resolve().parents[3] / "docs" / "patterns" / "llm.md"
BLOCK = re.compile(r"<!-- llm-reask:begin -->\s*```python\n(.*?)```\s*<!-- llm-reask:end -->", re.S)
REMINDER = "\n\nAnswer with one line: SENTIMENT | ASPECT."


@pytest.fixture(scope="module")
def recipe():
    (code,) = BLOCK.findall(PAGE.read_text(encoding="utf-8"))
    namespace: dict = {}
    exec(compile(code, str(PAGE), "exec"), namespace)
    return namespace


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ("UBUNYE_LLM_MODE", "UBUNYE_LLM_STORE", "UBUNYE_LLM_MAX_CALLS"):
        monkeypatch.delenv(name, raising=False)


def _says(provider, *texts):
    for text in texts:
        provider.answer(
            {
                "type": "message",
                "model": "c",
                "content": [{"type": "text", "text": text}],
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 10, "output_tokens": 3},
            }
        )


def _port(provider, **kwargs):
    return llm.port("anthropic", model="c", api_key="k", base_url=provider.url, **kwargs)


PROMPTS = ["review a", "review b", "review c", "review d"]


def test_only_the_failed_answers_are_asked_again_up_to_the_cap(provider, recipe):
    _says(
        provider,
        "positive | battery",  # a
        "it was fine I guess",  # b: bad
        "NEGATIVE | price",  # c
        "negative",  # d: bad
        "neutral | taste",  # b, second try
        "still no format",  # d, second try
        "no | idea | at all",  # d, third try: the cap
    )
    with llm.recording() as calls:
        values, failed = recipe["complete_parsed"](
            _port(provider),
            PROMPTS,
            recipe["sentiment_aspect"],
            tries=2,
            reminder=REMINDER,
            max_concurrency=1,
        )
    assert values == [("positive", "battery"), ("neutral", "taste"), ("negative", "price"), None]
    assert failed == [3]
    sent = [r["body"]["messages"][0]["content"] for r in provider.requests]
    assert sent == PROMPTS + ["review b" + REMINDER, "review d" + REMINDER, "review d" + REMINDER]
    assert len(calls) == 7 and all(c["status"] == "ok" for c in calls)


def test_the_extra_calls_count_against_the_budget(provider, recipe):
    _says(provider, "positive | a", "bad", "bad", "positive | d")
    with pytest.raises(LLMBudgetError, match="max_calls=5"):
        recipe["complete_parsed"](
            _port(provider, max_calls=5),
            PROMPTS,
            recipe["sentiment_aspect"],
            max_concurrency=1,
        )
    # 4 first asks, then 2 re-asks would make 6: the sixth is refused, never sent.
    assert len(provider.requests) == 5


def test_a_recorded_run_replays_call_for_call(provider, recipe, tmp_path):
    store = str(tmp_path / "replay.jsonl")
    # a, b, c; then a, c again; then a; then a: its fourth answer is the first good one.
    _says(provider, "bad", "positive | b", "bad", "bad", "negative | c", "bad", "neutral | a")

    def run(mode):
        with llm.recording():  # one run: repeated prompts are numbered within it
            return recipe["complete_parsed"](
                _port(provider, mode=mode, store=store),
                PROMPTS[:3],
                recipe["sentiment_aspect"],
                tries=3,  # no reminder: the very same prompt is sent again
                max_concurrency=1,
            )

    recorded = run("record")
    assert recorded == ([("neutral", "a"), ("positive", "b"), ("negative", "c")], [])
    assert len(provider.requests) == 7
    assert run("replay") == recorded
    assert len(provider.requests) == 7  # replay sent nothing
