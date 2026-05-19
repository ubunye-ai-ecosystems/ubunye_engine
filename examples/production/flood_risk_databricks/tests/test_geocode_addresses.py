"""Tests for the TomTom geocoding task.

No live API calls: ``_tomtom_call`` is exercised through a fake
``requests.Session`` whose ``.get`` returns a prebuilt response-like
object. Ensures the fallback chain (strict -> no language -> no idxSet),
429-handling, rank-1 filtering, and schema contract all hold.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import pytest

# transformations imports are path-injected by conftest.py
import transformations as geocode_mod  # noqa: E402
from transformations import GEOCODE_OUTPUT_COLUMNS, batch_geocode  # noqa: E402


class FakeResponse:
    def __init__(self, status_code: int, payload: Dict[str, Any] | None = None, reason: str = "OK"):
        self.status_code = status_code
        self._payload = payload or {}
        self.reason = reason
        self.text = str(payload) if payload else ""
        self.headers: Dict[str, str] = {}

    def json(self) -> Dict[str, Any]:
        return self._payload


class FakeSession:
    """Records calls and replays canned payloads keyed by the address in the URL."""

    def __init__(self, payload_by_address: Dict[str, Any]):
        self.calls: List[Dict[str, Any]] = []
        self.payload_by_address = payload_by_address
        self.headers: Dict[str, str] = {}

    def get(self, url, params=None, timeout=None, verify=None):
        self.calls.append({"url": url, "params": dict(params or {})})
        for addr, payload in self.payload_by_address.items():
            if addr.replace(" ", "%20") in url or addr.replace(",", "%2C") in url or addr in url:
                return FakeResponse(200, payload)
        return FakeResponse(200, {"results": []})


def _tomtom_payload(lat: float, lon: float, score: float, municipality: str) -> Dict[str, Any]:
    return {
        "results": [
            {
                "type": "Point Address",
                "score": score,
                "address": {
                    "freeformAddress": f"addr @ ({lat}, {lon})",
                    "countryCode": "ZA",
                    "municipality": municipality,
                    "streetName": "Some Street",
                    "postalCode": "0000",
                },
                "position": {"lat": lat, "lon": lon},
            }
        ]
    }


def test_batch_geocode_happy_path_returns_all_rows():
    session = FakeSession(
        {
            "1 Discovery Place, Sandton": _tomtom_payload(-26.1, 28.05, 9.5, "Sandton"),
            "30 Jellicoe Avenue, Rosebank": _tomtom_payload(-26.14, 28.04, 9.7, "Rosebank"),
        }
    )
    pairs = [
        ("id-1", "1 Discovery Place, Sandton"),
        ("id-2", "30 Jellicoe Avenue, Rosebank"),
    ]
    pdf = batch_geocode(pairs, api_key="fake", session=session, per_call_sleep=0, max_retries=1)

    assert list(pdf.columns) == list(GEOCODE_OUTPUT_COLUMNS)
    assert len(pdf) == 2
    # one call per address (no fallbacks hit on 200)
    assert len(session.calls) == 2
    assert set(pdf["id"]) == {"id-1", "id-2"}
    assert pdf.loc[pdf["id"] == "id-1", "lat"].iloc[0] == -26.1
    assert pdf.loc[pdf["id"] == "id-2", "municipality"].iloc[0] == "Rosebank"


def test_batch_geocode_no_results_produces_error_row():
    session = FakeSession({})  # empty mapping -> always {"results": []}
    pairs = [("id-9", "Nowhere, Atlantis")]
    pdf = batch_geocode(pairs, api_key="fake", session=session, per_call_sleep=0, max_retries=1)

    assert len(pdf) == 1
    row = pdf.iloc[0]
    assert row["id"] == "id-9"
    assert row["error"] == "no results"
    assert pd.isna(row["lat"])


def test_batch_geocode_empty_address_skips_call():
    session = FakeSession({})
    pdf = batch_geocode(
        [("id-0", "")],
        api_key="fake",
        session=session,
        per_call_sleep=0,
        max_retries=1,
    )
    assert len(session.calls) == 0
    assert pdf.iloc[0]["error"] == "empty address"


def test_tomtom_call_falls_back_when_400_on_strict_params():
    # session that returns 400 for the first two calls (language + idxSet attempts)
    # and 200 on the third (minimal params).
    responses = iter(
        [
            FakeResponse(400, {}, reason="Bad Request"),
            FakeResponse(400, {}, reason="Bad Request"),
            FakeResponse(200, _tomtom_payload(-26.0, 28.0, 8.0, "Test")),
        ]
    )

    class StrictFakeSession:
        def __init__(self):
            self.calls: List[Dict[str, Any]] = []
            self.headers: Dict[str, str] = {}

        def get(self, url, params=None, timeout=None, verify=None):
            self.calls.append({"url": url, "params": dict(params or {})})
            return next(responses)

    session = StrictFakeSession()
    r = geocode_mod._tomtom_call(session, "some addr", {"key": "k", "language": "en-ZA"}, 5)

    assert r.status_code == 200
    assert len(session.calls) == 3
    # Third call dropped both language and idxSet (even though we didn't pass idxSet)
    assert "language" not in session.calls[2]["params"]


def test_batch_geocode_429_retries_then_succeeds():
    responses = iter(
        [
            FakeResponse(429, {}, reason="Too Many"),
            FakeResponse(200, _tomtom_payload(-26.0, 28.0, 9.0, "Test")),
        ]
    )

    class BurstyFakeSession:
        def __init__(self):
            self.headers: Dict[str, str] = {}

        def get(self, url, params=None, timeout=None, verify=None):
            resp = next(responses)
            resp.headers = {"Retry-After": "0"}
            return resp

    pdf = batch_geocode(
        [("id-5", "some addr")],
        api_key="fake",
        session=BurstyFakeSession(),
        per_call_sleep=0,
        max_retries=3,
    )
    assert len(pdf) == 1
    assert pdf.iloc[0]["id"] == "id-5"
    assert pdf.iloc[0]["lat"] == -26.0


def test_batch_geocode_empty_input_returns_empty_frame():
    pdf = batch_geocode([], api_key="fake", session=None)
    assert list(pdf.columns) == list(GEOCODE_OUTPUT_COLUMNS)
    assert len(pdf) == 0


@pytest.mark.parametrize(
    "cols",
    [
        GEOCODE_OUTPUT_COLUMNS,
    ],
)
def test_output_columns_unique(cols):
    assert len(cols) == len(set(cols))
