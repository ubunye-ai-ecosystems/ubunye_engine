"""FOCUS 1.4 cost rows from a run record: the model bill, from the workload itself.

`FOCUS <https://focus.finops.org/>`_ is the FinOps Foundation's format for cost and
usage data, which FinOps tools load next to the cloud bill. :func:`rows` turns a
run record's model calls into FOCUS 1.4 Cost and Usage rows, with every mandatory
column (specification/datasets/cost_and_usage/columns, tag v1.4, of
github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS_Spec):

- one row per provider, model and token direction (input and output tokens have
  different prices), so each row's cost is its quantity times its unit price;
- ``ChargeCategory`` "Usage", ``ChargeFrequency`` "Usage-Based",
  ``ServiceCategory`` "AI and Machine Learning", ``PricingUnit`` "1000000 Tokens";
- the charge period is the run's start and end, the billing period its calendar
  month, all in UTC as ``YYYY-MM-DDTHH:mm:ssZ``;
- custom columns carry the ``x_`` prefix: the run, the task, the model, the token
  direction, the price table's date and the cost basis.

The costs are tokens times the dated list price the run used (``x_CostBasis``), so
BilledCost, EffectiveCost, ContractedCost and ListCost are equal: the engine cannot
see discounts or credits, and the provider's invoice is the authority. Replayed
calls are not charges and make no rows; calls with no price are left out and
counted by :func:`left_out`. ``UBUNYE_FOCUS_BILLING_ACCOUNT_ID`` and
``UBUNYE_FOCUS_BILLING_ACCOUNT_NAME`` name the account (default "unknown").
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

#: FOCUS 1.4 Cost and Usage columns at feature level Mandatory.
MANDATORY = (
    "BilledCost",
    "BillingAccountId",
    "BillingAccountName",
    "BillingCurrency",
    "BillingPeriodEnd",
    "BillingPeriodStart",
    "ChargeCategory",
    "ChargeClass",
    "ChargeDescription",
    "ChargePeriodEnd",
    "ChargePeriodStart",
    "ContractedCost",
    "EffectiveCost",
    "HostProviderName",
    "InvoiceIssuerName",
    "ListCost",
    "PricingQuantity",
    "PricingUnit",
    "ServiceCategory",
    "ServiceName",
    "ServiceProviderName",
)
#: Mandatory columns the specification lets be null in these rows: ChargeClass is
#: null unless the charge corrects a closed period; HostProviderName may be null
#: when the infrastructure cannot be told (an unknown OpenAI-compatible server).
NULLABLE = ("ChargeClass", "HostProviderName")
#: Recommended and conditional FOCUS columns these rows also fill.
OPTIONAL = (
    "ChargeFrequency",
    "ConsumedQuantity",
    "ConsumedUnit",
    "ListUnitPrice",
    "ContractedUnitPrice",
    "SkuPriceId",
)

FOCUS_VERSION = "1.4"
PER = 1_000_000


def _zulu(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def _fmt(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def _month(moment: datetime) -> Tuple[str, str]:
    start = moment.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end = (
        start.replace(year=start.year + 1, month=1)
        if start.month == 12
        else start.replace(month=start.month + 1)
    )
    return _fmt(start), _fmt(end)


def _charges(record: Any) -> List[Dict[str, Any]]:
    return [
        c
        for c in record.llm_calls or []
        if c.get("status") == "ok"
        and c.get("source", "live") != "replay"
        and c.get("price_usd_per_mtok")
    ]


def left_out(record: Any) -> Dict[str, int]:
    """Calls that made no row: replayed (no charge) and unpriced (unknown cost)."""
    ok = [c for c in record.llm_calls or [] if c.get("status") == "ok"]
    replayed = sum(1 for c in ok if c.get("source") == "replay")
    unpriced = sum(1 for c in ok if c.get("source") != "replay" and not c.get("price_usd_per_mtok"))
    return {"replayed": replayed, "unpriced": unpriced}


def rows(record: Any) -> List[Dict[str, Any]]:
    """FOCUS 1.4 rows for a run record's model calls (see the module docstring)."""
    start = _zulu(record.started_at) or datetime.now(timezone.utc)
    end = _zulu(record.ended_at) or start
    period_start, period_end = _month(start)
    account_id = os.environ.get("UBUNYE_FOCUS_BILLING_ACCOUNT_ID") or "unknown"
    account_name = os.environ.get("UBUNYE_FOCUS_BILLING_ACCOUNT_NAME") or "unknown"

    groups: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for c in _charges(record):
        unit_in, unit_out = (float(p) for p in c["price_usd_per_mtok"])
        for direction, tokens, unit in (
            ("input", int(c.get("input_tokens") or 0), unit_in),
            ("output", int(c.get("output_tokens") or 0), unit_out),
        ):
            key = (
                c.get("provider"),
                c.get("service"),
                c.get("backend"),
                c["model"],
                direction,
                unit,
                c.get("price_as_of") or "unknown",
            )
            g = groups.setdefault(key, {"tokens": 0, "calls": 0})
            g["tokens"] += tokens
            g["calls"] += 1

    out: List[Dict[str, Any]] = []
    for (provider, service, backend, model, direction, unit, as_of), g in sorted(
        groups.items(), key=lambda kv: tuple(str(k) for k in kv[0])
    ):
        quantity = g["tokens"] / PER
        cost = quantity * unit
        issuer = provider or "unknown"
        out.append(
            {
                "BilledCost": cost,
                "BillingAccountId": account_id,
                "BillingAccountName": account_name,
                "BillingCurrency": "USD",
                "BillingPeriodEnd": period_end,
                "BillingPeriodStart": period_start,
                "ChargeCategory": "Usage",
                "ChargeClass": None,
                "ChargeDescription": (
                    f"{model} {direction} tokens for Ubunye task {record.task_path}"
                ),
                "ChargePeriodEnd": _fmt(end),
                "ChargePeriodStart": _fmt(start),
                "ContractedCost": cost,
                "EffectiveCost": cost,
                "HostProviderName": provider,
                "InvoiceIssuerName": issuer,
                "ListCost": cost,
                "PricingQuantity": quantity,
                "PricingUnit": "1000000 Tokens",
                "ServiceCategory": "AI and Machine Learning",
                "ServiceName": service or "Language model API",
                "ServiceProviderName": issuer,
                "ChargeFrequency": "Usage-Based",
                "ConsumedQuantity": g["tokens"],
                "ConsumedUnit": "Tokens",
                "ListUnitPrice": unit,
                "ContractedUnitPrice": unit,
                "SkuPriceId": f"{backend}/{model}/{direction}",
                "x_UbunyeRunId": record.run_id,
                "x_UbunyeTaskPath": record.task_path,
                "x_UbunyeModel": model,
                "x_UbunyeCallCount": g["calls"],
                "x_TokenDirection": direction,
                "x_PricesAsOf": as_of,
                "x_CostBasis": (
                    "tokens x list price "
                    + ("given to the port" if as_of == "given" else "from the engine's table")
                    + "; the provider's invoice is authoritative"
                ),
                "x_FocusVersion": FOCUS_VERSION,
            }
        )
    return out
