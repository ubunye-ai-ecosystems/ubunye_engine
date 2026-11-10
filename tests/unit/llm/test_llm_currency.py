"""Model costs in a display currency (ADR 015).

Prices stay in US dollars. ``UBUNYE_LLM_CURRENCY`` names the currency the bill is
shown and capped in, and ``UBUNYE_LLM_FX_<CODE>`` gives the rate and the day it was
read. The engine never fetches or guesses a rate: a currency with no rate stops the
run. The run record keeps the dollar figures and the currency, rate and date used.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

import ubunye
from ubunye.cli.main import app
from ubunye.core.errors import LLMError
from ubunye.lineage.context import RunContext
from ubunye.lineage.storage import FileSystemLineageStore
from ubunye.llm import currency
from ubunye.llm.budget import Budget

from .test_llm_port import _task

ENV = (
    "UBUNYE_LLM_CURRENCY",
    "UBUNYE_LLM_FX_ZAR",
    "UBUNYE_LLM_FX_EUR",
    "UBUNYE_LLM_MAX_USD",
    "UBUNYE_LLM_MAX_CALLS",
    "UBUNYE_LLM_MAX_SECONDS",
    "UBUNYE_LLM_MODE",
    "UBUNYE_LLM_STORE",
    "UBUNYE_LLM_PRICES",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ENV:
        monkeypatch.delenv(name, raising=False)


def _zar(monkeypatch, rate="18.40@2026-11-10"):
    monkeypatch.setenv("UBUNYE_LLM_CURRENCY", "ZAR")
    monkeypatch.setenv("UBUNYE_LLM_FX_ZAR", rate)


# --- the setting -----------------------------------------------------------------


def test_no_currency_or_usd_means_dollars(monkeypatch):
    assert currency.from_env() is None
    monkeypatch.setenv("UBUNYE_LLM_CURRENCY", "USD")
    assert currency.from_env() is None


def test_a_currency_needs_a_dated_rate(monkeypatch):
    _zar(monkeypatch)
    zar = currency.from_env()
    assert zar == currency.Currency(code="ZAR", per_usd=18.40, as_of="2026-11-10")
    assert zar.amount(2.0) == pytest.approx(36.80)
    assert zar.to_dict() == {"code": "ZAR", "per_usd": 18.40, "as_of": "2026-11-10"}


def test_any_iso_code_works_in_any_case(monkeypatch):
    monkeypatch.setenv("UBUNYE_LLM_CURRENCY", "eur")
    monkeypatch.setenv("UBUNYE_LLM_FX_EUR", "0.86@2026-11-09")
    assert currency.from_env().code == "EUR"


def test_a_currency_without_a_rate_is_a_clear_error(monkeypatch):
    monkeypatch.setenv("UBUNYE_LLM_CURRENCY", "ZAR")
    with pytest.raises(LLMError, match="UBUNYE_LLM_FX_ZAR is not") as info:
        currency.from_env()
    assert "never fetches" in str(info.value)
    assert "UBUNYE_LLM_FX_ZAR=" in info.value.hint


@pytest.mark.parametrize(
    "rate", ["18.40", "lots@2026-11-10", "0@2026-11-10", "-1@2026-11-10", "18.40@last week"]
)
def test_a_rate_must_be_a_positive_number_and_a_date(monkeypatch, rate):
    _zar(monkeypatch, rate)
    with pytest.raises(LLMError, match="UBUNYE_LLM_FX_ZAR"):
        currency.from_env()


def test_a_currency_must_be_an_iso_code(monkeypatch):
    monkeypatch.setenv("UBUNYE_LLM_CURRENCY", "rand")
    with pytest.raises(LLMError, match="ISO 4217"):
        currency.from_env()


def test_the_run_budget_carries_the_currency(monkeypatch):
    _zar(monkeypatch)
    budget = Budget.from_env()
    assert budget.currency.code == "ZAR"
    assert budget.summary()["currency"] == {"code": "ZAR", "per_usd": 18.40, "as_of": "2026-11-10"}


def test_a_budget_in_dollars_has_the_same_summary_as_before():
    assert "currency" not in Budget(max_usd=1).summary()


# --- the run record --------------------------------------------------------------


def test_the_run_record_keeps_dollars_and_the_rate_used(provider, tmp_path, monkeypatch):
    task = _task(tmp_path, provider.url, model="claude-haiku-4-5")
    _zar(monkeypatch)  # no limit set: the currency is still recorded
    lineage = tmp_path / "lineage"
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    (record,) = FileSystemLineageStore(str(lineage)).list_runs("uc/pkg/label")
    b = record.llm_budget
    assert b["currency"] == {"code": "ZAR", "per_usd": 18.40, "as_of": "2026-11-10"}
    assert b["spent_usd"] == pytest.approx(2 * (11 * 1 + 4 * 5) / 1_000_000)
    assert all(c["cost_usd"] is not None for c in record.llm_calls)

    shown = CliRunner().invoke(
        app,
        ["lineage", "trace", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "label",
         "--lineage-dir", str(lineage)],
    )  # fmt: skip
    assert shown.exit_code == 0, shown.output
    assert "ZAR 0.001141" in shown.output  # $0.000062 at 18.40
    assert "ZAR at 18.4 per USD, rate as of 2026-11-10" in shown.output


def test_a_currency_with_no_rate_stops_the_run_before_any_call(provider, tmp_path, monkeypatch):
    task = _task(tmp_path, provider.url, model="claude-haiku-4-5")
    monkeypatch.setenv("UBUNYE_LLM_CURRENCY", "ZAR")
    with pytest.raises(LLMError, match="UBUNYE_LLM_FX_ZAR"):
        ubunye.run_task(str(task), backend="pandas")
    assert provider.requests == []


def test_an_old_record_without_a_currency_reads_as_dollars():
    old = {"run_id": "r", "task_path": "u/p/t", "usecase": "u", "package": "p",
           "task_name": "t", "started_at": "2026-01-01T00:00:00+00:00",
           "llm_budget": {"max_usd": 1.0, "spent_usd": 0.1}}  # fmt: skip
    record = RunContext.from_dict(old)
    assert currency.from_record(record) is None
