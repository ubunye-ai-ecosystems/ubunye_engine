"""The bill is capped before the run: a call that could pass the ceiling is never sent.

Before each call the port reserves its worst case (the prompt's tokens counted
high, plus the whole ``max_tokens``); after it, the reservation becomes the real
cost. Limits come from ``UBUNYE_LLM_MAX_USD``, ``UBUNYE_LLM_MAX_CALLS`` and
``UBUNYE_LLM_MAX_SECONDS`` (one budget per run) or from the port's own arguments.
"""

from __future__ import annotations

import json

import pytest

import ubunye
from ubunye import llm
from ubunye.core.errors import LLMBudgetError, LLMError
from ubunye.lineage.storage import FileSystemLineageStore
from ubunye.llm import prices

from .test_llm_port import _task

LIMITS = ("UBUNYE_LLM_MAX_USD", "UBUNYE_LLM_MAX_CALLS", "UBUNYE_LLM_MAX_SECONDS")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in (*LIMITS, "UBUNYE_LLM_MODE", "UBUNYE_LLM_STORE", "UBUNYE_LLM_PRICES"):
        monkeypatch.delenv(name, raising=False)


def _port(provider, **kw):
    return llm.port("anthropic", model="claude-haiku-4-5-20251001", api_key="k",
                    base_url=provider.url, **kw)  # fmt: skip


# --- prices -------------------------------------------------------------------------


def test_the_table_is_dated_and_sourced():
    assert prices.AS_OF and prices.SOURCES["anthropic"].startswith("https://")


def test_a_dated_model_id_finds_its_family_price():
    assert prices.lookup("anthropic", "claude-haiku-4-5-20251001") == (1.0, 5.0)
    assert prices.lookup("anthropic", "claude-sonnet-5") == (2.0, 10.0)
    assert prices.lookup("anthropic", "no-such-model") is None


def test_more_prices_come_from_a_file(tmp_path, monkeypatch):
    extra = tmp_path / "prices.json"
    extra.write_text(json.dumps({"openai_compatible": {"llama3.2": [0, 0], "gpt-x": [2.5, 10]}}))
    monkeypatch.setenv("UBUNYE_LLM_PRICES", str(extra))
    assert prices.lookup("openai_compatible", "gpt-x-2026") == (2.5, 10.0)
    assert prices.lookup("openai_compatible", "llama3.2") == (0.0, 0.0)


def test_cost_is_tokens_times_price():
    assert prices.cost((1.0, 5.0), 1_000_000, 200_000) == pytest.approx(2.0)


# --- the ceiling --------------------------------------------------------------------


def test_a_call_that_could_pass_the_ceiling_is_never_sent(provider):
    # Worst case of one call: ~1 input token + 1000 output tokens at $5/M = ~$0.005.
    port = _port(provider, max_usd=0.004)
    with pytest.raises(LLMBudgetError, match="max_usd") as err:
        port.complete("hi", max_tokens=1000)
    assert provider.requests == []
    assert "0.004" in str(err.value)


def test_calls_within_the_ceiling_run_and_are_charged_at_their_real_cost(provider):
    port = _port(provider, max_usd=1.0)
    with llm.recording() as calls:
        port.complete("hi", max_tokens=100)
    (call,) = calls
    assert call["cost_usd"] == pytest.approx((11 * 1.0 + 4 * 5.0) / 1_000_000)
    assert call["estimated_usd"] >= call["cost_usd"]


def test_the_reservation_is_released_so_real_costs_add_up(provider):
    # Each worst case is ~$0.0005, each real call ~$0.00003: 20 fit under $0.002,
    # where 20 worst cases ($0.01) would not.
    port = _port(provider, max_usd=0.002)
    for _ in range(20):
        port.complete("hi", max_tokens=100)
    assert len(provider.requests) == 20


def test_max_calls_stops_the_next_call(provider):
    port = _port(provider, max_calls=2)
    port.complete("a")
    port.complete("b")
    with pytest.raises(LLMBudgetError, match="max_calls"):
        port.complete("c")
    assert len(provider.requests) == 2


def test_concurrent_calls_share_one_ceiling(provider):
    port = _port(provider, max_calls=5)
    with pytest.raises(LLMBudgetError):
        port.complete_many([f"p{i}" for i in range(12)], max_concurrency=6)
    assert len(provider.requests) == 5


def test_max_seconds_stops_calls_after_the_clock_runs_out(provider, monkeypatch):
    port = _port(provider, max_seconds=10)
    port.complete("a")
    clock = llm.budget.time.monotonic
    monkeypatch.setattr(llm.budget.time, "monotonic", lambda: clock() + 11)
    with pytest.raises(LLMBudgetError, match="max_seconds"):
        port.complete("b")


def test_a_dollar_ceiling_on_an_unpriced_model_fails_closed(provider):
    port = llm.port("anthropic", model="mystery", api_key="k", base_url=provider.url, max_usd=5)
    with pytest.raises(LLMBudgetError, match="no price"):
        port.complete("hi")
    assert provider.requests == []


def test_a_price_can_be_given_to_the_port(provider):
    port = llm.port("anthropic", model="mystery", api_key="k", base_url=provider.url,
                    max_usd=5, price=(3.0, 15.0))  # fmt: skip
    with llm.recording() as calls:
        port.complete("hi")
    assert calls[0]["cost_usd"] == pytest.approx((11 * 3 + 4 * 15) / 1_000_000)


def test_no_ceiling_means_no_price_is_needed(provider):
    port = llm.port("anthropic", model="mystery", api_key="k", base_url=provider.url)
    with llm.recording() as calls:
        port.complete("hi")
    assert calls[0]["cost_usd"] is None  # unknown, not zero


def test_replay_costs_nothing_and_is_never_blocked(provider, tmp_path):
    store = str(tmp_path / "r.jsonl")
    _port(provider, mode="record", store=store).complete("hi")
    port = _port(provider, mode="replay", store=store, max_usd=0.0, max_calls=0)
    with llm.recording() as calls:
        assert port.complete("hi").text == "echo: hi"
    assert calls[0]["cost_usd"] == 0.0


def test_a_bad_limit_is_refused(monkeypatch):
    monkeypatch.setenv("UBUNYE_LLM_MAX_USD", "lots")
    with pytest.raises(LLMError, match="UBUNYE_LLM_MAX_USD"):
        llm.budget.Budget.from_env()


# --- through the engine: one budget per run -----------------------------------------


def test_the_run_budget_covers_every_port_and_nothing_is_written(provider, tmp_path, monkeypatch):
    task = _task(tmp_path, provider.url)  # labels 2 rows: 2 calls
    monkeypatch.setenv("UBUNYE_LLM_MAX_CALLS", "1")
    lineage = tmp_path / "lineage"
    with pytest.raises(LLMBudgetError):
        ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    assert not (tmp_path / "out").exists()
    (record,) = FileSystemLineageStore(str(lineage)).list_runs("uc/pkg/label")
    assert record.status == "error"
    assert record.llm_budget["max_calls"] == 1
    assert [c["status"] for c in record.llm_calls].count("refused") == 1


def test_the_run_record_keeps_the_budget_and_the_spend(provider, tmp_path, monkeypatch):
    task = _task(tmp_path, provider.url, model="claude-haiku-4-5")
    monkeypatch.setenv("UBUNYE_LLM_MAX_USD", "1")
    lineage = tmp_path / "lineage"
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    (record,) = FileSystemLineageStore(str(lineage)).list_runs("uc/pkg/label")
    assert record.llm_budget["max_usd"] == 1.0
    assert record.llm_budget["calls"] == 2
    assert record.llm_budget["spent_usd"] == pytest.approx(2 * (11 * 1 + 4 * 5) / 1_000_000)
