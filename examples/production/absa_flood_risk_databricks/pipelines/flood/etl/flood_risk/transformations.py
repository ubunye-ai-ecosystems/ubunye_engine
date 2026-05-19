"""JBA flood-risk task - geocoded address -> floodscores + flooddepths.

Reads the Task-1 output (``address_geocoded``), assembles a batched POST
body for the JBA ``/floodscores/ZA`` and ``/flooddepths/ZA`` endpoints,
calls each in chunks of 10, merges the two response sets on ``id``, and
renames the ~60 nested keys into snake-case analytics columns.

Secrets:
    ``JBA_BASIC_AUTH`` is the full ``Authorization`` header value
    (``"Basic <base64>"``). The notebook wrapper reads it from a
    Databricks secret scope and exports it before ``ubunye.run_pipeline``.

Testability:
    ``jba_batch_request`` takes an optional ``post`` callable so tests can
    inject a deterministic fake for both endpoints without touching the
    real JBA API.
"""

from __future__ import annotations

import math
import os
import time
from typing import Any, Callable, Dict, Iterable, List, Optional

from ubunye.core.interfaces import Task

JBA_FLOODSCORES_URL = "https://api.jbarisk.com/floodscores/ZA"
JBA_FLOODDEPTHS_URL = "https://api.jbarisk.com/flooddepths/ZA"

DEFAULT_BUFFER_M = 100
DEFAULT_BATCH_SIZE = 10
DEFAULT_MAX_BATCHES = 100
DEFAULT_TIMEOUT = (30, 120)
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_BASE = 1.5

# Maps the pandas-json_normalized JBA response keys onto analytics-friendly
# snake-case column names. Ported verbatim from the legacy script.
COLUMN_MAPPING: Dict[str, str] = {
    "id": "address_id",
    "stats.FLRF_U.rp_20.ppa20": "river_flood_rp20_affected_area_percentage",
    "stats.FLRF_U.rp_20.min20": "river_flood_rp20_min_depth_meters",
    "stats.FLRF_U.rp_20.max20": "river_flood_rp20_max_depth_meters",
    "stats.FLRF_U.rp_20.mean": "river_flood_rp20_mean_depth_meters",
    "stats.FLRF_U.rp_20.std_dev": "river_flood_rp20_std_dev_depth_meters",
    "stats.FLRF_U.rp_50.ppa50": "river_flood_rp50_affected_area_percentage",
    "stats.FLRF_U.rp_50.min50": "river_flood_rp50_min_depth_meters",
    "stats.FLRF_U.rp_50.max50": "river_flood_rp50_max_depth_meters",
    "stats.FLRF_U.rp_50.mean": "river_flood_rp50_mean_depth_meters",
    "stats.FLRF_U.rp_50.std_dev": "river_flood_rp50_std_dev_depth_meters",
    "stats.FLRF_U.rp_100.ppa100": "river_flood_rp100_affected_area_percentage",
    "stats.FLRF_U.rp_100.min100": "river_flood_rp100_min_depth_meters",
    "stats.FLRF_U.rp_100.max100": "river_flood_rp100_max_depth_meters",
    "stats.FLRF_U.rp_100.mean": "river_flood_rp100_mean_depth_meters",
    "stats.FLRF_U.rp_100.std_dev": "river_flood_rp100_std_dev_depth_meters",
    "stats.FLRF_U.rp_200.ppa200": "river_flood_rp200_affected_area_percentage",
    "stats.FLRF_U.rp_200.min200": "river_flood_rp200_min_depth_meters",
    "stats.FLRF_U.rp_200.max200": "river_flood_rp200_max_depth_meters",
    "stats.FLRF_U.rp_200.mean": "river_flood_rp200_mean_depth_meters",
    "stats.FLRF_U.rp_200.std_dev": "river_flood_rp200_std_dev_depth_meters",
    "stats.FLRF_U.rp_500.ppa500": "river_flood_rp500_affected_area_percentage",
    "stats.FLRF_U.rp_500.min500": "river_flood_rp500_min_depth_meters",
    "stats.FLRF_U.rp_500.max500": "river_flood_rp500_max_depth_meters",
    "stats.FLRF_U.rp_500.mean": "river_flood_rp500_mean_depth_meters",
    "stats.FLRF_U.rp_500.std_dev": "river_flood_rp500_std_dev_depth_meters",
    "stats.FLRF_U.rp_1500.ppa1500": "river_flood_rp1500_affected_area_percentage",
    "stats.FLRF_U.rp_1500.min1500": "river_flood_rp1500_min_depth_meters",
    "stats.FLRF_U.rp_1500.max1500": "river_flood_rp1500_max_depth_meters",
    "stats.FLRF_U.rp_1500.mean": "river_flood_rp1500_mean_depth_meters",
    "stats.FLRF_U.rp_1500.std_dev": "river_flood_rp1500_std_dev_depth_meters",
    "stats.FLRF_U.sop": "river_flood_standard_of_protection_rp",
    "stats.FLSW_U.rp_20.ppa20": "surface_water_flood_rp20_affected_area_percentage",
    "stats.FLSW_U.rp_20.min20": "surface_water_flood_rp20_min_depth_meters",
    "stats.FLSW_U.rp_20.max20": "surface_water_flood_rp20_max_depth_meters",
    "stats.FLSW_U.rp_20.mean": "surface_water_flood_rp20_mean_depth_meters",
    "stats.FLSW_U.rp_20.std_dev": "surface_water_flood_rp20_std_dev_depth_meters",
    "stats.FLSW_U.rp_50.ppa50": "surface_water_flood_rp50_affected_area_percentage",
    "stats.FLSW_U.rp_50.min50": "surface_water_flood_rp50_min_depth_meters",
    "stats.FLSW_U.rp_50.max50": "surface_water_flood_rp50_max_depth_meters",
    "stats.FLSW_U.rp_50.mean": "surface_water_flood_rp50_mean_depth_meters",
    "stats.FLSW_U.rp_50.std_dev": "surface_water_flood_rp50_std_dev_depth_meters",
    "stats.FLSW_U.rp_100.ppa100": "surface_water_flood_rp100_affected_area_percentage",
    "stats.FLSW_U.rp_100.min100": "surface_water_flood_rp100_min_depth_meters",
    "stats.FLSW_U.rp_100.max100": "surface_water_flood_rp100_max_depth_meters",
    "stats.FLSW_U.rp_100.mean": "surface_water_flood_rp100_mean_depth_meters",
    "stats.FLSW_U.rp_100.std_dev": "surface_water_flood_rp100_std_dev_depth_meters",
    "stats.FLSW_U.rp_200.ppa200": "surface_water_flood_rp200_affected_area_percentage",
    "stats.FLSW_U.rp_200.min200": "surface_water_flood_rp200_min_depth_meters",
    "stats.FLSW_U.rp_200.max200": "surface_water_flood_rp200_max_depth_meters",
    "stats.FLSW_U.rp_200.mean": "surface_water_flood_rp200_mean_depth_meters",
    "stats.FLSW_U.rp_200.std_dev": "surface_water_flood_rp200_std_dev_depth_meters",
    "stats.FLSW_U.rp_500.ppa500": "surface_water_flood_rp500_affected_area_percentage",
    "stats.FLSW_U.rp_500.min500": "surface_water_flood_rp500_min_depth_meters",
    "stats.FLSW_U.rp_500.max500": "surface_water_flood_rp500_max_depth_meters",
    "stats.FLSW_U.rp_500.mean": "surface_water_flood_rp500_mean_depth_meters",
    "stats.FLSW_U.rp_500.std_dev": "surface_water_flood_rp500_std_dev_depth_meters",
    "stats.FLSW_U.rp_1500.ppa1500": "surface_water_flood_rp1500_affected_area_percentage",
    "stats.FLSW_U.rp_1500.min1500": "surface_water_flood_rp1500_min_depth_meters",
    "stats.FLSW_U.rp_1500.max1500": "surface_water_flood_rp1500_max_depth_meters",
    "stats.FLSW_U.rp_1500.mean": "surface_water_flood_rp1500_mean_depth_meters",
    "stats.FLSW_U.rp_1500.std_dev": "surface_water_flood_rp1500_std_dev_depth_meters",
    "River_Floodscore_Def": "river_flood_score_defended",
    "River_Floodscore_UD": "river_flood_score_undefended",
    "Surfacewater_Floodscore_UD": "surface_water_flood_score_undefended",
    "FloodScore_Def": "overall_flood_score_defended",
    "FloodScore_UD": "overall_flood_score_undefended",
    "Floodability_Def": "floodability_defended",
    "Floodability_UD": "floodability_undefended",
}


def _chunks(seq: List[Any], n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _build_items(
    rows: Iterable[Dict[str, Any]],
    *,
    buffer_m: int = DEFAULT_BUFFER_M,
) -> List[Dict[str, Any]]:
    if buffer_m > 500:
        raise ValueError("JBA limit: point buffer must be <= 500 meters.")
    items: List[Dict[str, Any]] = []
    for row in rows:
        lat = row.get("lat")
        lon = row.get("lon")
        if lat is None or lon is None:
            continue
        items.append(
            {
                "id": str(row["id"]),
                "wkt_geometry": f"POINT({lon} {lat})",
                "buffer": int(buffer_m),
            }
        )
    return items


def jba_batch_request(
    endpoint_url: str,
    items: List[Dict[str, Any]],
    *,
    basic_auth: str,
    country_code: str = "ZA",
    include_version: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_batches: int = DEFAULT_MAX_BATCHES,
    timeout=DEFAULT_TIMEOUT,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base: float = DEFAULT_BACKOFF_BASE,
    verify_tls: Any = True,
    post: Optional[Callable[..., Any]] = None,
) -> "Any":
    """Batch-POST ``items`` to ``endpoint_url`` and return a normalised pandas DataFrame."""
    import pandas as pd
    import requests

    if not items:
        return pd.DataFrame()
    if len(items) > batch_size * max_batches:
        raise ValueError(
            f"Too many items: JBA limit ~{batch_size} per batch x {max_batches} batches."
        )

    headers = {"Authorization": basic_auth, "Content-Type": "application/json"}
    params = {"include_version": include_version}
    _post = post or requests.post

    all_results: List[Dict[str, Any]] = []
    batches = list(_chunks(items, batch_size))
    for b_idx, batch in enumerate(batches, start=1):
        body = {"country_code": country_code, "geometries": batch}
        for attempt in range(1, max_retries + 1):
            try:
                r = _post(
                    endpoint_url,
                    headers=headers,
                    params=params,
                    json=body,
                    timeout=timeout,
                    verify=verify_tls,
                )
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    time.sleep(float(ra) if ra else attempt * 2.0)
                    continue
                if r.status_code >= 400:
                    msg = f"[batch {b_idx}/{len(batches)}] {r.status_code} {r.reason}: {r.text[:500]}"
                    if attempt == max_retries:
                        raise RuntimeError(msg)
                    time.sleep(backoff_base * attempt)
                    continue
                payload = r.json() or []
                all_results.extend(payload)
                break
            except requests.exceptions.RequestException:
                if attempt == max_retries:
                    raise
                time.sleep(backoff_base * attempt)
        time.sleep(0.2)

    from pandas import json_normalize

    return json_normalize(all_results, sep=".")


def compute_flood_risk(
    geocoded_pdf: "Any",
    *,
    basic_auth: str,
    buffer_m: int = DEFAULT_BUFFER_M,
    scores_endpoint: str = JBA_FLOODSCORES_URL,
    depths_endpoint: str = JBA_FLOODDEPTHS_URL,
    post: Optional[Callable[..., Any]] = None,
    verify_tls: Any = True,
) -> "Any":
    """Orchestrate floodscores + flooddepths + merge + rename into one pandas DataFrame."""
    import pandas as pd

    if geocoded_pdf.empty:
        return pd.DataFrame()

    keep = geocoded_pdf.dropna(subset=["lat", "lon"]).copy()
    if keep.empty:
        return pd.DataFrame()

    items = _build_items(keep.to_dict(orient="records"), buffer_m=buffer_m)

    df_scores = jba_batch_request(
        scores_endpoint, items, basic_auth=basic_auth, post=post, verify_tls=verify_tls
    )
    df_depths = jba_batch_request(
        depths_endpoint, items, basic_auth=basic_auth, post=post, verify_tls=verify_tls
    )

    if df_scores.empty and df_depths.empty:
        return pd.DataFrame()
    if df_scores.empty:
        merged = df_depths
    elif df_depths.empty:
        merged = df_scores
    else:
        merged = df_depths.merge(df_scores, on="id", how="inner")

    merged = merged.drop_duplicates(subset=["id"], keep="first")
    merged = merged.rename(columns={k: v for k, v in COLUMN_MAPPING.items() if k in merged.columns})

    keep = keep.rename(columns={"id": "address_id"})
    return keep.merge(merged, on="address_id", how="inner")


class FloodRisk(Task):
    """Ubunye Task: merge JBA floodscores + flooddepths onto geocoded addresses."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        from pyspark.sql import functions as F

        basic_auth = os.environ.get("JBA_BASIC_AUTH")
        if not basic_auth:
            raise RuntimeError(
                "JBA_BASIC_AUTH is not set. The notebook wrapper must export it "
                "from the Databricks secret scope before invoking run_pipeline. "
                "Expected value format: 'Basic <base64-encoded user:pass>'."
            )

        geocoded = sources["address_geocoded"]
        spark = geocoded.sparkSession

        pdf = geocoded.toPandas()
        out_pdf = compute_flood_risk(pdf, basic_auth=basic_auth)

        if out_pdf.empty:
            # Preserve the schema shell so downstream readers don't trip on an
            # empty table being inferred as a single-column DataFrame.
            empty_cols = list(pdf.columns) + ["risk_computed_at"]
            out_pdf = out_pdf.reindex(columns=empty_cols)

        sdf = spark.createDataFrame(out_pdf).withColumn("risk_computed_at", F.current_timestamp())
        return {"address_flood_risk": sdf}
