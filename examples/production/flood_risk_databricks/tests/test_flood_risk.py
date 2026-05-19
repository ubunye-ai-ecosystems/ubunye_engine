"""Tests for the JBA flood-risk task.

Uses an injected ``post`` callable (no ``requests`` imports needed) to
stand in for the two endpoint calls. Covers: batching, merge on id,
column renaming, empty-input short-circuit, and missing-lat/lon filter.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

import transformations as flood_mod  # noqa: E402
from transformations import (  # noqa: E402
    COLUMN_MAPPING,
    _build_items,
    compute_flood_risk,
    jba_batch_request,
)


class FakePostResponse:
    def __init__(self, status_code: int, payload: Any = None, reason: str = "OK"):
        self.status_code = status_code
        self._payload = payload
        self.reason = reason
        self.text = str(payload) if payload else ""
        self.headers: Dict[str, str] = {}

    def json(self):
        return self._payload


def _make_post(endpoint_payloads: Dict[str, Any]):
    calls: List[Dict[str, Any]] = []

    def _post(url, headers=None, params=None, json=None, timeout=None, verify=None):
        calls.append({"url": url, "json": json, "headers": headers})
        return FakePostResponse(200, endpoint_payloads.get(url, []))

    _post.calls = calls  # type: ignore[attr-defined]
    return _post


def test_build_items_drops_rows_without_coords():
    items = _build_items(
        [
            {"id": "a", "lat": -26.0, "lon": 28.0},
            {"id": "b", "lat": None, "lon": 28.0},
            {"id": "c", "lat": -26.1, "lon": None},
            {"id": "d", "lat": -26.2, "lon": 28.05},
        ]
    )
    assert [i["id"] for i in items] == ["a", "d"]
    assert items[0]["wkt_geometry"] == "POINT(28.0 -26.0)"
    assert items[0]["buffer"] == 100


def test_build_items_rejects_oversized_buffer():
    import pytest

    with pytest.raises(ValueError, match="buffer"):
        _build_items([{"id": "a", "lat": -26.0, "lon": 28.0}], buffer_m=600)


def test_jba_batch_request_batches_by_size_and_normalises_payload():
    items = [
        {"id": f"id-{i}", "wkt_geometry": f"POINT(28 {-26 - i * 0.01})", "buffer": 100}
        for i in range(23)
    ]
    endpoint = "https://example/jba/floodscores"
    payload = [
        {"id": f"id-{i}", "stats": {"FLRF_U": {"sop": 100}}} for i in range(23)
    ]
    post = _make_post({endpoint: payload})

    df = jba_batch_request(endpoint, items, basic_auth="Basic x", batch_size=10, post=post)

    # 23 items -> 3 batches (10, 10, 3)
    assert len(post.calls) == 3
    assert [len(c["json"]["geometries"]) for c in post.calls] == [10, 10, 3]
    # pandas json_normalize flattens stats.FLRF_U.sop
    assert "stats.FLRF_U.sop" in df.columns
    assert len(df) == 23


def test_jba_batch_request_empty_items_returns_empty():
    post = _make_post({})
    df = jba_batch_request("https://example/jba/depths", [], basic_auth="Basic x", post=post)
    assert df.empty
    assert post.calls == []


def test_compute_flood_risk_merges_scores_and_depths_and_renames():
    geocoded = pd.DataFrame(
        [
            {"id": "a", "inputAddress": "1 A St", "lat": -26.0, "lon": 28.0},
            {"id": "b", "inputAddress": "2 B St", "lat": -26.1, "lon": 28.05},
            {"id": "c", "inputAddress": "3 C St", "lat": None, "lon": 28.1},  # dropped
        ]
    )
    scores_payload = [
        {"id": "a", "River_Floodscore_Def": 1, "FloodScore_UD": 2},
        {"id": "b", "River_Floodscore_Def": 3, "FloodScore_UD": 4},
    ]
    depths_payload = [
        {"id": "a", "stats": {"FLRF_U": {"sop": 100}}},
        {"id": "b", "stats": {"FLRF_U": {"sop": 50}}},
    ]
    post = _make_post(
        {
            flood_mod.JBA_FLOODSCORES_URL: scores_payload,
            flood_mod.JBA_FLOODDEPTHS_URL: depths_payload,
        }
    )

    out = compute_flood_risk(geocoded, basic_auth="Basic x", post=post)

    # c was filtered out (missing lat)
    assert set(out["address_id"]) == {"a", "b"}
    # Renames applied
    assert "river_flood_standard_of_protection_rp" in out.columns
    assert "overall_flood_score_undefended" in out.columns
    assert "river_flood_score_defended" in out.columns
    # Values land per id
    row_a = out.loc[out["address_id"] == "a"].iloc[0]
    assert row_a["river_flood_score_defended"] == 1
    assert row_a["overall_flood_score_undefended"] == 2
    assert row_a["river_flood_standard_of_protection_rp"] == 100


def test_compute_flood_risk_empty_input_returns_empty_frame():
    out = compute_flood_risk(pd.DataFrame(), basic_auth="Basic x", post=_make_post({}))
    assert out.empty


def test_compute_flood_risk_all_missing_coords_returns_empty():
    df = pd.DataFrame(
        [{"id": "a", "lat": None, "lon": None}, {"id": "b", "lat": None, "lon": 28}]
    )
    out = compute_flood_risk(df, basic_auth="Basic x", post=_make_post({}))
    assert out.empty


def test_column_mapping_is_injective_and_covers_source_keys():
    # Sanity check on the 60+ mapping: no two source keys collapse into the
    # same output name (would silently drop columns on rename).
    targets = list(COLUMN_MAPPING.values())
    assert len(targets) == len(set(targets))
