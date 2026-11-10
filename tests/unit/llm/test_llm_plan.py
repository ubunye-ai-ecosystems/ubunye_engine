"""``ubunye plan`` shows the bill before the run.

For a task that calls a model, the plan prices the task's recorded calls (its
replay file) at today's prices and sets that against the run's ceiling. It reads
no data and calls nothing.
"""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app

from .test_llm_port import _task

runner = CliRunner()


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (
        "UBUNYE_LLM_MODE",
        "UBUNYE_LLM_STORE",
        "UBUNYE_LLM_PRICES",
        "UBUNYE_LLM_MAX_USD",
        "UBUNYE_LLM_MAX_CALLS",
        "UBUNYE_LLM_MAX_SECONDS",
        "UBUNYE_LLM_CURRENCY",
        "UBUNYE_LLM_FX_ZAR",
        "UBUNYE_LLM_MAX_ZAR",
    ):
        monkeypatch.delenv(name, raising=False)


def _recorded(task, entries):
    """A replay file with (model, input_tokens, output_tokens) answers."""
    folder = task
    lines = [
        json.dumps({"key": f"sha256:{i:064x}", "backend": "anthropic", "model": model,
                    "recorded_at": "2026-09-24T00:00:00Z",
                    "response": {"text": "x", "model": model, "input_tokens": tin,
                                 "output_tokens": tout, "stop_reason": "end_turn"}})
        for i, (model, tin, tout) in enumerate(entries)
    ]  # fmt: skip
    (folder / "llm-replay.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _plan(tmp_path, *extra):
    return runner.invoke(
        app, ["plan", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "label", *extra]
    )


def _llm(tmp_path):
    result = _plan(tmp_path, "--json")
    return result, json.loads(result.output)["tasks"][0]["llm"]


def test_a_task_that_calls_a_model_gets_a_model_section(tmp_path):
    task = _task(tmp_path, "http://unused")
    _recorded(task, [("claude-haiku-4-5", 1_000_000, 100_000)] * 2)
    result, llm = _llm(tmp_path)
    assert result.exit_code == 0, result.output
    assert llm["uses_llm"] is True and llm["mode"] == "live"
    (group,) = llm["recorded"]
    assert group["model"] == "claude-haiku-4-5" and group["calls"] == 2
    # Each: 1M tokens in at $1/M plus 100k out at $5/M = $1.50.
    assert llm["estimated_usd"] == pytest.approx(3.0)
    assert llm["prices_as_of"]


def test_the_estimate_over_the_ceiling_is_a_warning(tmp_path, monkeypatch):
    task = _task(tmp_path, "http://unused")
    _recorded(task, [("claude-haiku-4-5", 1_000_000, 0)])
    monkeypatch.setenv("UBUNYE_LLM_MAX_USD", "0.5")
    result = _plan(tmp_path)
    assert result.exit_code == 0, result.output
    assert "$1.000000 of $0.5" in result.output
    assert "over UBUNYE_LLM_MAX_USD" in result.output


def test_replay_mode_with_nothing_recorded_is_a_problem(tmp_path, monkeypatch):
    _task(tmp_path, "http://unused")
    monkeypatch.setenv("UBUNYE_LLM_MODE", "replay")
    result = _plan(tmp_path)
    assert result.exit_code == 1
    assert "nothing recorded" in result.output


def test_replay_costs_nothing(tmp_path, monkeypatch):
    task = _task(tmp_path, "http://unused")
    _recorded(task, [("claude-haiku-4-5", 1_000_000, 0)])
    monkeypatch.setenv("UBUNYE_LLM_MODE", "replay")
    _, llm = _llm(tmp_path)
    assert llm["estimated_usd"] == 0.0


def test_a_dollar_ceiling_on_an_unpriced_recorded_model_is_a_problem(tmp_path, monkeypatch):
    task = _task(tmp_path, "http://unused")
    _recorded(task, [("mystery-model", 10, 10)])
    monkeypatch.setenv("UBUNYE_LLM_MAX_USD", "1")
    result = _plan(tmp_path)
    assert result.exit_code == 1
    assert "mystery-model has no price" in result.output


def test_no_ceiling_on_live_calls_is_a_warning(tmp_path):
    _task(tmp_path, "http://unused")
    result = _plan(tmp_path)
    assert result.exit_code == 0
    assert "no dollar ceiling" in result.output


def test_a_bad_limit_is_a_problem(tmp_path, monkeypatch):
    _task(tmp_path, "http://unused")
    monkeypatch.setenv("UBUNYE_LLM_MAX_CALLS", "many")
    result = _plan(tmp_path)
    assert result.exit_code == 1
    assert "UBUNYE_LLM_MAX_CALLS" in result.output


def test_a_task_without_model_calls_has_no_model_section(tmp_path):
    task = _task(tmp_path, "http://unused")
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\nclass T(Task):\n"
        "    def transform(self, sources):\n        return {'labelled': sources['raw']}\n",
        encoding="utf-8",
    )
    _, llm = _llm(tmp_path)
    assert llm is None


# --- in a display currency (ADR 015) -------------------------------------------------


def _rand(monkeypatch):
    monkeypatch.setenv("UBUNYE_LLM_CURRENCY", "ZAR")
    monkeypatch.setenv("UBUNYE_LLM_FX_ZAR", "18.40@2026-11-10")


def test_the_bill_shows_in_the_display_currency_with_the_rate_used(tmp_path, monkeypatch):
    task = _task(tmp_path, "http://unused")
    _recorded(task, [("claude-haiku-4-5", 1_000_000, 100_000)] * 2)  # $3.00
    _rand(monkeypatch)
    monkeypatch.setenv("UBUNYE_LLM_MAX_ZAR", "100")
    result = _plan(tmp_path)
    assert result.exit_code == 0, result.output
    assert "Model calls  mode live, max_zar=100" in result.output
    assert "2 recorded calls, 2000000 tokens in, 200000 out, ZAR 55.200000" in result.output
    assert "estimated ZAR 55.200000 of ZAR 100 (prices as of" in result.output
    assert "in ZAR at 18.4 per USD, rate as of 2026-11-10 (estimated $3.000000" in result.output
    assert "no dollar ceiling" not in result.output and "no cost ceiling" not in result.output
    _, llm = _llm(tmp_path)
    assert llm["currency"] == {"code": "ZAR", "per_usd": 18.40, "as_of": "2026-11-10"}
    assert llm["estimated_usd"] == pytest.approx(3.0)
    assert llm["estimated"] == pytest.approx(55.2)


def test_dollars_stay_the_default(tmp_path):
    task = _task(tmp_path, "http://unused")
    _recorded(task, [("claude-haiku-4-5", 1_000_000, 0)])
    _, llm = _llm(tmp_path)
    assert llm["currency"] is None and llm["estimated"] == llm["estimated_usd"]


def test_an_estimate_over_the_rand_ceiling_is_a_warning(tmp_path, monkeypatch):
    task = _task(tmp_path, "http://unused")
    _recorded(task, [("claude-haiku-4-5", 1_000_000, 0)])  # $1.00 = ZAR 18.40
    _rand(monkeypatch)
    monkeypatch.setenv("UBUNYE_LLM_MAX_ZAR", "10")
    result = _plan(tmp_path)
    assert "costs ZAR 18.400000, over UBUNYE_LLM_MAX_ZAR=ZAR 10" in result.output


def test_a_rand_ceiling_on_an_unpriced_recorded_model_is_a_problem(tmp_path, monkeypatch):
    task = _task(tmp_path, "http://unused")
    _recorded(task, [("mystery-model", 10, 10)])
    _rand(monkeypatch)
    monkeypatch.setenv("UBUNYE_LLM_MAX_ZAR", "10")
    result = _plan(tmp_path)
    assert result.exit_code == 1
    assert "UBUNYE_LLM_MAX_ZAR is set but anthropic/mystery-model has no price" in result.output


def test_a_currency_with_no_rate_is_a_problem(tmp_path, monkeypatch):
    _task(tmp_path, "http://unused")
    monkeypatch.setenv("UBUNYE_LLM_CURRENCY", "ZAR")
    result = _plan(tmp_path)
    assert result.exit_code == 1
    assert "UBUNYE_LLM_FX_ZAR is not" in result.output
    assert "Set UBUNYE_LLM_FX_ZAR=RATE@" in result.output  # and what to do about it


def test_live_calls_with_no_ceiling_in_either_currency_warn(tmp_path, monkeypatch):
    _task(tmp_path, "http://unused")
    _rand(monkeypatch)
    result = _plan(tmp_path)
    assert "no cost ceiling (UBUNYE_LLM_MAX_ZAR or UBUNYE_LLM_MAX_USD)" in result.output
