"""The gate on model steps: replayed in CI, and no quiet growth in the bill.

A pull request's run should replay its model calls (no key, no spend, the same
answers), so a changed prompt shows up as a failed replay, never as a live call.
And a change that makes the task send more tokens is a cost regression the gate
can refuse, measured at list prices from the recorded tokens, so it works on
replayed runs too.
"""

from __future__ import annotations

import copy

from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.core.gate import FAIL, OK, Policy, evaluate, passed

from ..test_gate import _run

runner = CliRunner()


def _calls(n, source="replay", tokens_in=100, tokens_out=10, price=(1.0, 5.0)):
    return [
        {"backend": "anthropic", "model": "claude-haiku-4-5", "status": "ok", "source": source,
         "input_tokens": tokens_in, "output_tokens": tokens_out,
         "price_usd_per_mtok": list(price) if price else None,
         "cost_usd": 0.0 if source == "replay" else None}
        for _ in range(n)
    ]  # fmt: skip


def _pair(base_calls, cand_calls):
    base = _run(llm_calls=base_calls)
    cand = copy.deepcopy(base)
    cand.run_id = "bbbbbbbb-0000-4000-8000-000000000002"
    cand.llm_calls = cand_calls
    return base, cand


def _llm(findings):
    return [f for f in findings if f.rule == "llm"]


def test_model_calls_are_reported(tmp_path):
    base, cand = _pair(_calls(10), _calls(10))
    (f,) = _llm(evaluate(base, cand))
    assert f.status == OK
    assert "10 calls (10 replayed)" in f.detail
    assert "1100 tokens" in f.detail
    assert "$0.001500 at list prices" in f.detail  # 10 x (100 x $1 + 10 x $5) / 1M


def test_a_task_without_model_calls_gets_no_llm_finding():
    base, cand = _pair([], [])
    assert _llm(evaluate(base, cand)) == []


def test_require_replay_fails_a_run_that_called_out():
    base, cand = _pair(_calls(3), _calls(2) + _calls(1, source="live"))
    findings = evaluate(base, cand, Policy(require_replay=True))
    (f,) = [x for x in _llm(findings) if x.status == FAIL]
    assert "1 of 3 model calls went live" in f.detail
    assert not passed(findings)


def test_require_replay_passes_a_fully_replayed_run():
    base, cand = _pair(_calls(3), _calls(3))
    assert passed(evaluate(base, cand, Policy(require_replay=True)))


def test_more_tokens_past_the_limit_fail():
    base, cand = _pair(_calls(10), _calls(13))
    findings = evaluate(base, cand, Policy(max_llm_cost_increase=0.2))
    (f,) = [x for x in _llm(findings) if x.status == FAIL]
    assert "30%" in f.detail and "20%" in f.detail


def test_more_tokens_within_the_limit_pass():
    base, cand = _pair(_calls(10), _calls(11))
    assert passed(evaluate(base, cand, Policy(max_llm_cost_increase=0.2)))


def test_an_unpriced_model_is_compared_on_tokens():
    base, cand = _pair(_calls(10, price=None), _calls(13, price=None))
    findings = evaluate(base, cand, Policy(max_llm_cost_increase=0.2))
    (f,) = [x for x in _llm(findings) if x.status == FAIL]
    assert "tokens" in f.detail


def test_the_cli_takes_the_new_rules(tmp_path):
    base, cand = _pair(_calls(10), _calls(10, source="live"))
    for name, rec in (("base.json", base), ("cand.json", cand)):
        (tmp_path / name).write_text(__import__("json").dumps(rec.to_dict()), encoding="utf-8")
    result = runner.invoke(
        app,
        ["gate", "--baseline", str(tmp_path / "base.json"),
         "--candidate", str(tmp_path / "cand.json"), "--require-replay"],
    )  # fmt: skip
    assert result.exit_code == 1
    assert "went live" in result.output
