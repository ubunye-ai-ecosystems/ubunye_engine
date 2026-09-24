"""FOCUS 1.4 cost rows from the workload itself.

A run's model calls become rows a FinOps tool can load next to the cloud bill:
every mandatory FOCUS 1.4 column, one row per model and token direction, the cost
equal to quantity times unit price, times in UTC with a ``Z``. The costs are
tokens times the dated list price, and say so; the provider's invoice decides.
"""

from __future__ import annotations

import csv
import io
import json
import re

import pytest
from typer.testing import CliRunner

import ubunye
from ubunye.cli.main import app
from ubunye.lineage import focus
from ubunye.lineage.storage import FileSystemLineageStore

from .test_llm_port import _task

runner = CliRunner()
ZULU = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for name in ("UBUNYE_LLM_MODE", "UBUNYE_LLM_STORE", "UBUNYE_LLM_MAX_USD",
                 "UBUNYE_FOCUS_BILLING_ACCOUNT_ID", "UBUNYE_FOCUS_BILLING_ACCOUNT_NAME"):  # fmt: skip
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def record(provider, tmp_path):
    task = _task(tmp_path, provider.url, model="claude-haiku-4-5-20251001")
    lineage = tmp_path / "lineage"
    ubunye.run_task(str(task), backend="pandas", lineage=True, lineage_dir=str(lineage))
    (rec,) = FileSystemLineageStore(str(lineage)).list_runs("uc/pkg/label")
    return rec


def test_every_mandatory_column_is_filled(record):
    rows = focus.rows(record)
    assert len(rows) == 2  # one model: input tokens and output tokens
    for row in rows:
        for column in focus.MANDATORY:
            assert column in row, column
            if column not in focus.NULLABLE:
                assert row[column] is not None, column


def test_values_follow_the_spec(record):
    for row in focus.rows(record):
        assert row["ChargeCategory"] == "Usage"
        assert row["ChargeClass"] is None
        assert row["ChargeFrequency"] == "Usage-Based"
        assert row["ServiceCategory"] == "AI and Machine Learning"
        assert row["BillingCurrency"] == "USD"
        assert row["PricingUnit"] == "1000000 Tokens"
        assert row["ConsumedUnit"] == "Tokens"
        for column in ("ChargePeriodStart", "ChargePeriodEnd",
                       "BillingPeriodStart", "BillingPeriodEnd"):  # fmt: skip
            assert ZULU.match(row[column]), (column, row[column])
        assert row["BillingPeriodStart"].endswith("-01T00:00:00Z")
        for x in [c for c in row if c not in focus.MANDATORY and c not in focus.OPTIONAL]:
            assert x.startswith("x_"), x  # custom columns carry the x_ prefix


def test_cost_is_quantity_times_unit_price(record):
    by_direction = {r["x_TokenDirection"]: r for r in focus.rows(record)}
    inp, out = by_direction["input"], by_direction["output"]
    assert inp["ConsumedQuantity"] == 22 and out["ConsumedQuantity"] == 8  # 2 calls
    assert inp["ListUnitPrice"] == 1.0 and out["ListUnitPrice"] == 5.0
    for row in (inp, out):
        assert row["PricingQuantity"] == pytest.approx(row["ConsumedQuantity"] / 1_000_000)
        assert row["ListCost"] == pytest.approx(row["PricingQuantity"] * row["ListUnitPrice"])
        assert row["BilledCost"] == row["EffectiveCost"] == row["ContractedCost"] == row["ListCost"]


def test_rows_name_the_provider_the_run_and_the_basis(record):
    row = focus.rows(record)[0]
    assert row["ServiceProviderName"] == row["InvoiceIssuerName"] == "Anthropic"
    assert row["ServiceName"] == "Anthropic API"
    assert row["x_UbunyeRunId"] == record.run_id
    assert row["x_UbunyeTaskPath"] == "uc/pkg/label"
    assert row["x_CostBasis"].startswith("tokens x list price")
    assert row["x_PricesAsOf"]


def test_the_billing_account_can_be_named(record, monkeypatch):
    monkeypatch.setenv("UBUNYE_FOCUS_BILLING_ACCOUNT_ID", "acct-7")
    monkeypatch.setenv("UBUNYE_FOCUS_BILLING_ACCOUNT_NAME", "Data team")
    row = focus.rows(record)[0]
    assert (row["BillingAccountId"], row["BillingAccountName"]) == ("acct-7", "Data team")


def test_replayed_and_unpriced_calls_are_not_charges(record):
    record.llm_calls.append(dict(record.llm_calls[0], source="replay", cost_usd=0.0))
    record.llm_calls.append(
        dict(record.llm_calls[0], model="mystery", price_usd_per_mtok=None, cost_usd=None)
    )
    rows = focus.rows(record)
    assert {r["x_UbunyeModel"] for r in rows} == {"claude-haiku-4-5-20251001"}
    assert sum(r["ConsumedQuantity"] for r in rows if r["x_TokenDirection"] == "input") == 22
    assert focus.left_out(record) == {"replayed": 1, "unpriced": 1}


def test_the_cli_writes_csv_and_jsonl(record, tmp_path):
    args = ["lineage", "focus", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "label",
            "--lineage-dir", str(tmp_path / "lineage")]  # fmt: skip
    as_csv = runner.invoke(app, args)
    assert as_csv.exit_code == 0, as_csv.output
    rows = list(csv.DictReader(io.StringIO(as_csv.output)))
    assert len(rows) == 2 and rows[0]["ChargeCategory"] == "Usage"
    assert rows[0]["ChargeClass"] == ""  # null in CSV

    out = tmp_path / "focus.jsonl"
    written = runner.invoke(app, [*args, "--format", "jsonl", "-o", str(out)])
    assert written.exit_code == 0, written.output
    lines = [json.loads(x) for x in out.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 2 and lines[0]["ChargeClass"] is None
