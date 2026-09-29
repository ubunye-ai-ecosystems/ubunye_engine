"""A long ``complete_many`` says how far it is (F-008); a short one says nothing.

The clock is a fake that moves one second each time the progress reads it (once
when the batch starts, once per finished call, under a lock), so the lines are
the same whatever order concurrent calls finish in.
"""

from __future__ import annotations

import io
import sys
import threading

import pytest

from ubunye import llm
from ubunye.core.errors import LLMBudgetError, LLMError
from ubunye.llm import progress


class FakeClock:
    def __init__(self, step: float) -> None:
        self.now = 0.0
        self.step = step
        self.lock = threading.Lock()

    def __call__(self) -> float:
        with self.lock:
            now = self.now
            self.now += self.step
            return now


@pytest.fixture
def clock(monkeypatch):
    def use(step: float) -> FakeClock:
        fake = FakeClock(step)
        monkeypatch.setattr(progress, "CLOCK", fake)
        return fake

    return use


@pytest.fixture(autouse=True)
def _no_progress_or_limit_settings(monkeypatch):
    for name in ("UBUNYE_LLM_PROGRESS", "UBUNYE_LLM_MAX_USD", "UBUNYE_LLM_MAX_CALLS"):
        monkeypatch.delenv(name, raising=False)


def _port(provider, **kwargs):
    return llm.port("anthropic", model="c", api_key="k", base_url=provider.url, **kwargs)


def _lines(capsys):
    return [x for x in capsys.readouterr().err.splitlines() if x.startswith("LLM ")]


@pytest.mark.parametrize("concurrency", [1, 8], ids=["sequential", "concurrent"])
def test_a_long_batch_reports_calls_done_of_total(provider, clock, capsys, concurrency):
    clock(1.0)  # one second per call: 300 calls take 5 minutes
    answers = _port(provider).complete_many(
        [f"review {i}" for i in range(300)], max_concurrency=concurrency
    )
    assert len(answers) == 300
    lines = _lines(capsys)
    # Every 10% (30 calls), since 30 s is more than 10 s; then one line at the end.
    assert [x.split(": ")[1].split(" calls")[0] for x in lines] == [
        f"{n}/300" for n in range(30, 301, 30)
    ]
    assert lines[0] == "LLM anthropic/c: 30/300 calls (10%), 30s"
    assert lines[-1] == "LLM anthropic/c: 300/300 calls (100%), 5m00s"


def test_the_line_carries_the_spend_when_a_budget_keeps_count(provider, clock, capsys):
    clock(1.0)
    _port(provider, max_usd=5, price=(3.0, 15.0)).complete_many([str(i) for i in range(40)])
    lines = _lines(capsys)
    assert lines and all(" spent of $5" in x for x in lines)
    # 40 calls, 11 tokens in and 4 out each, at $3 and $15 per million.
    assert lines[-1].endswith("$0.0037 spent of $5")


def test_a_fast_batch_writes_nothing(provider, clock, capsys):
    clock(0.001)  # 300 calls in a third of a second
    _port(provider).complete_many([str(i) for i in range(300)])
    assert _lines(capsys) == []


def test_a_handful_of_calls_writes_nothing_however_slow(provider, clock, capsys):
    clock(100.0)
    _port(provider).complete_many([str(i) for i in range(progress.MIN_CALLS - 1)])
    assert _lines(capsys) == []


def test_ten_seconds_bound_the_lines_when_ten_percent_come_faster(provider, clock, capsys):
    clock(0.5)  # 1000 calls, 10% is 100 calls = 50 s, so the share decides
    _port(provider).complete_many([str(i) for i in range(1000)])
    assert len(_lines(capsys)) == 10
    clock(1.0)  # 20 calls: 10% is 2 calls = 2 s, so the 10 s decide
    _port(provider).complete_many([str(i) for i in range(20)])
    done = [x.split(": ")[1].split(" calls")[0] for x in _lines(capsys)]
    assert done == ["10/20", "20/20"]


# --- after the skeptic's review ---------------------------------------------------


class BrokenStderr(io.TextIOBase):
    def write(self, s):
        raise BrokenPipeError(32, "Broken pipe")

    def flush(self):
        raise BrokenPipeError(32, "Broken pipe")

    def close(self):  # garbage collection closes a stream: stay quiet there
        pass


def test_a_broken_stderr_never_stops_a_batch_that_succeeded(provider, clock, monkeypatch):
    clock(1.0)
    monkeypatch.setattr(sys, "stderr", BrokenStderr())
    assert len(_port(provider).complete_many([str(i) for i in range(40)])) == 40


def test_a_broken_stderr_never_hides_the_calls_own_error(provider, clock, monkeypatch):
    clock(1.0)  # call 10 fails, and call 10 is when the first line is due
    for _ in range(9):
        provider.answer(None)
    provider.answer({"error": "no"}, status=400)
    monkeypatch.setattr(sys, "stderr", BrokenStderr())
    with pytest.raises(LLMError, match="HTTP 400"):
        _port(provider).complete_many([str(i) for i in range(40)], max_concurrency=1)


def test_no_stderr_means_no_line_and_never_stdout(provider, clock, capsys, monkeypatch):
    clock(1.0)
    monkeypatch.setattr(sys, "stderr", None)
    _port(provider).complete_many([str(i) for i in range(40)])
    assert "LLM " not in capsys.readouterr().out


def test_refused_calls_are_not_counted_as_answered(provider, clock, capsys):
    clock(1.0)
    with pytest.raises(LLMBudgetError):
        _port(provider, max_calls=50).complete_many([str(i) for i in range(300)], max_concurrency=8)
    assert _lines(capsys)[-1] == (
        "LLM anthropic/c: 300/300 calls (100%), 50 answered, 250 refused by the budget, 5m00s"
    )


def test_failed_calls_are_not_counted_as_answered(provider, clock, capsys):
    clock(1.0)
    for _ in range(40):
        provider.answer({"error": "no"}, status=400)
    with pytest.raises(LLMError):
        _port(provider).complete_many([str(i) for i in range(40)], max_concurrency=1)
    assert _lines(capsys)[-1] == "LLM anthropic/c: 40/40 calls (100%), 0 answered, 40 failed, 40s"


def test_no_spend_shows_without_a_dollar_limit(provider, clock, capsys):
    clock(1.0)
    _port(provider, max_calls=1000, price=(3.0, 15.0)).complete_many([str(i) for i in range(40)])
    lines = _lines(capsys)
    assert lines and not any("$" in x for x in lines)


def test_the_environment_dollar_limit_shows_outside_a_run(provider, clock, capsys, monkeypatch):
    clock(1.0)
    monkeypatch.setenv("UBUNYE_LLM_MAX_USD", "2")
    _port(provider, price=(3.0, 15.0)).complete_many([str(i) for i in range(40)])
    assert _lines(capsys)[-1].endswith(", $0.0037 spent of $2")


def test_a_run_limit_and_a_port_limit_both_show(provider, clock, capsys):
    from ubunye.llm.budget import Budget

    clock(1.0)
    with llm.recording(budget=Budget(max_usd=100.0)):
        _port(provider, max_usd=5, price=(3.0, 15.0)).complete_many([str(i) for i in range(40)])
    assert _lines(capsys)[-1].endswith(", run $0.0037 of $100, port $0.0037 of $5")


@pytest.mark.parametrize("value", ["0", "false", "No", " off "])
def test_the_environment_turns_it_off(provider, clock, capsys, monkeypatch, value):
    clock(1.0)
    monkeypatch.setenv("UBUNYE_LLM_PROGRESS", value)
    _port(provider).complete_many([str(i) for i in range(300)])
    assert _lines(capsys) == []
