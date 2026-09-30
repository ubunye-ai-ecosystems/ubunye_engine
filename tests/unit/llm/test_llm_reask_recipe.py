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
    assert list(failed) == [3] and isinstance(failed[3], ValueError)
    assert "no | idea | at all" in str(failed[3])  # the last reason is kept
    sent = [r["body"]["messages"][0]["content"] for r in provider.requests]
    assert sent == PROMPTS + ["review b" + REMINDER, "review d" + REMINDER, "review d" + REMINDER]
    assert len(calls) == 7 and all(c["status"] == "ok" for c in calls)


def test_the_extra_calls_count_against_the_budget_and_paid_answers_are_kept(provider, recipe):
    _says(provider, "positive | a", "bad", "bad", "positive | d")
    values, failed = recipe["complete_parsed"](
        _port(provider, max_calls=5),
        PROMPTS,
        recipe["sentiment_aspect"],
        max_concurrency=1,
    )
    # 4 first asks, then 2 re-asks would make 6: the sixth is refused, never sent.
    assert len(provider.requests) == 5
    assert values == [("positive", "a"), None, None, ("positive", "d")]
    assert list(failed) == [1, 2]
    assert all(isinstance(e, LLMBudgetError) for e in failed.values())


def test_a_budget_that_cannot_cover_the_first_pass_raises(provider, recipe):
    with pytest.raises(LLMBudgetError, match="max_calls=2"):
        recipe["complete_parsed"](
            _port(provider, max_calls=2), PROMPTS, recipe["sentiment_aspect"], max_concurrency=1
        )


def test_a_pandas_column_with_its_own_index_answers_by_position(provider, recipe):
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"text": ["alpha", "beta", "gamma", "delta"]}, index=[3, 2, 1, 0])
    _says(provider, "positive | alpha", "nope", "positive | gamma", "positive | delta")
    _says(provider, "positive | beta")
    values, failed = recipe["complete_parsed"](
        _port(provider), df["text"], recipe["sentiment_aspect"], max_concurrency=1
    )
    assert [v[1] for v in values] == ["alpha", "beta", "gamma", "delta"] and not failed
    sent = [r["body"]["messages"][0]["content"] for r in provider.requests]
    assert sent == ["alpha", "beta", "gamma", "delta", "beta"]


def test_any_error_in_parse_means_did_not_parse(provider, recipe):
    import json

    def parse_json(text):
        d = json.loads(text)
        return d["sentiment"], d["aspect"]

    _says(provider, '{"sentiment": "positive", "aspect": "a"}', '{"sentiment": "positive"}')
    _says(provider, '{"sentiment": "negative", "aspect": "b"}')
    values, failed = recipe["complete_parsed"](
        _port(provider), PROMPTS[:2], parse_json, max_concurrency=1
    )
    assert values == [("positive", "a"), ("negative", "b")] and not failed
    # And when it never parses, the KeyError is the reason given.
    _says(provider, '{"sentiment": "positive"}')
    values, failed = recipe["complete_parsed"](_port(provider), ["x"], parse_json, tries=0)
    assert values == [None] and isinstance(failed[0], KeyError)


@pytest.mark.parametrize("shape", ["text", "messages"])
def test_the_reminder_works_on_both_prompt_shapes(provider, recipe, shape):
    if shape == "text":
        prompts = ["review a"]
    else:
        prompts = [
            [
                {"role": "user", "content": "review a"},
                {"role": "assistant", "content": "ok"},
                {"role": "user", "content": "now label it"},
            ]
        ]
    before = repr(prompts)
    _says(provider, "bad", "positive | a")
    values, failed = recipe["complete_parsed"](
        _port(provider), prompts, recipe["sentiment_aspect"], reminder=REMINDER
    )
    assert values == [("positive", "a")] and not failed
    assert repr(prompts) == before  # the caller's prompts are not changed
    last = provider.requests[-1]["body"]["messages"][-1]["content"]
    assert last == ("review a" if shape == "text" else "now label it") + REMINDER


def test_negative_tries_are_refused(provider, recipe):
    with pytest.raises(ValueError, match="tries"):
        recipe["complete_parsed"](_port(provider), PROMPTS, recipe["sentiment_aspect"], tries=-1)
    assert provider.requests == []


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
    assert recorded == ([("neutral", "a"), ("positive", "b"), ("negative", "c")], {})
    assert len(provider.requests) == 7
    assert run("replay") == recorded
    assert len(provider.requests) == 7  # replay sent nothing
