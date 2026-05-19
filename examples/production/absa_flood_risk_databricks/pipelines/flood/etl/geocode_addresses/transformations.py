"""TomTom geocoding task - address string -> lat/lon + metadata.

Ports the batch-geocoding loop from the legacy flood-detection notebook into
a Ubunye ``Task``. The TomTom Search API is rate-limited (roughly 5 qps on
most tenants), so calls are driven serially from the driver with retry and
fallback on strict parameter sets. The returned pandas frame is filtered to
the top-1 candidate per ``inputAddress`` and converted back to Spark.

Secrets:
    ``TOMTOM_API_KEY`` is read from the environment at task init time. On
    Databricks the notebook wrapper pulls it from a secret scope and exports
    the env var before ``ubunye.run_pipeline()`` is invoked - it never
    lands in the config or in source.

Testability:
    All HTTP calls go through ``_tomtom_call``, which is easy to monkeypatch.
    ``batch_geocode`` accepts an optional ``session`` parameter so tests can
    inject a mock ``requests.Session``.
"""

from __future__ import annotations

import os
import time
import urllib.parse
from typing import Any, Dict, Iterable, List, Optional

import certifi

from ubunye.core.interfaces import Task

TOMTOM_BASE_URL = "https://api.tomtom.com/search/2/search"
DEFAULT_COUNTRY_SET = "ZA"
DEFAULT_MIN_FUZZY = 1
DEFAULT_MAX_FUZZY = 2
DEFAULT_LIMIT = 5
DEFAULT_VIEW = "Unified"
DEFAULT_LANGUAGE = "en-ZA"
DEFAULT_PER_CALL_SLEEP = 0.15

GEOCODE_OUTPUT_COLUMNS = (
    "id",
    "inputAddress",
    "score",
    "scoreRank",
    "freeformAddress",
    "countryCode",
    "municipality",
    "streetName",
    "postalCode",
    "lat",
    "lon",
    "matchType",
    "error",
)


def _tomtom_call(session, address: str, params_base: Dict[str, Any], timeout):
    """Single-address call with the three-step fallback from the legacy code."""
    import requests  # local import so the module imports without requests for schema-only tests

    q = urllib.parse.quote(address, safe="")
    url = f"{TOMTOM_BASE_URL}/{q}.json"

    r = session.get(url, params=params_base, timeout=timeout, verify=certifi.where())
    if r.status_code == 400:
        params_no_lang = {k: v for k, v in params_base.items() if k != "language"}
        r = session.get(url, params=params_no_lang, timeout=timeout, verify=certifi.where())
    if r.status_code == 400:
        params_min = {k: v for k, v in params_base.items() if k not in ("language", "idxSet")}
        r = session.get(url, params=params_min, timeout=timeout, verify=certifi.where())
    return r


def _candidate_rows(address_id: str, address: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    candidates = [c for c in (payload.get("results") or []) if isinstance(c, dict)]
    for rank, c in enumerate(candidates, start=1):
        a = c.get("address") or {}
        p = c.get("position") or {}
        rows.append(
            {
                "id": address_id,
                "inputAddress": address,
                "score": c.get("score"),
                "scoreRank": rank,
                "freeformAddress": a.get("freeformAddress"),
                "countryCode": a.get("countryCode"),
                "municipality": a.get("municipality"),
                "streetName": a.get("streetName"),
                "postalCode": a.get("postalCode"),
                "lat": p.get("lat"),
                "lon": p.get("lon"),
                "matchType": c.get("type"),
                "error": None,
            }
        )
    return rows


def _error_row(address_id: str, address: str, error: str) -> Dict[str, Any]:
    row = {col: None for col in GEOCODE_OUTPUT_COLUMNS}
    row["id"] = address_id
    row["inputAddress"] = address
    row["error"] = error
    return row


def batch_geocode(
    id_address_pairs: Iterable[tuple],
    *,
    api_key: str,
    country_set: str = DEFAULT_COUNTRY_SET,
    min_fuzzy: int = DEFAULT_MIN_FUZZY,
    max_fuzzy: int = DEFAULT_MAX_FUZZY,
    limit: int = DEFAULT_LIMIT,
    view: str = DEFAULT_VIEW,
    language: str = DEFAULT_LANGUAGE,
    timeout=(10, 20),
    max_retries: int = 3,
    backoff_base: float = 0.75,
    per_call_sleep: float = DEFAULT_PER_CALL_SLEEP,
    session: Optional[Any] = None,
) -> "Any":
    """Iterate over ``(id, address)`` pairs and return a pandas DataFrame.

    Every input row is represented in the output (a successful lookup
    produces one row per candidate; a hard failure produces a single row
    with ``error`` populated and the positional columns null). Downstream
    code filters to ``scoreRank == 1`` for the canonical match.
    """
    import pandas as pd
    import requests

    pairs = list(id_address_pairs)
    if not pairs:
        return pd.DataFrame(columns=list(GEOCODE_OUTPUT_COLUMNS))

    if session is None:
        session = requests.Session()
        session.headers.update(
            {"User-Agent": "ubunye-absa-geocoder/1.0", "Tracking-ID": "absa-flood-geo"}
        )

    base_params = {
        "countrySet": country_set,
        "minFuzzyLevel": min_fuzzy,
        "maxFuzzyLevel": max_fuzzy,
        "limit": limit,
        "view": view,
        "language": language,
        "key": api_key,
    }

    rows: List[Dict[str, Any]] = []
    for address_id, address in pairs:
        if not address:
            rows.append(_error_row(address_id, address, "empty address"))
            continue

        for attempt in range(1, max_retries + 1):
            try:
                r = _tomtom_call(session, address, base_params, timeout)

                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    time.sleep(float(ra) if ra else attempt * 2.0)
                    continue

                if r.status_code >= 400:
                    err = f"{r.status_code} {r.reason}: {r.text[:300]}"
                    if attempt == max_retries:
                        rows.append(_error_row(address_id, address, err))
                        break
                    time.sleep(backoff_base * attempt)
                    continue

                payload = r.json()
                candidates = _candidate_rows(address_id, address, payload)
                if candidates:
                    rows.extend(candidates)
                else:
                    rows.append(_error_row(address_id, address, "no results"))
                break

            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    rows.append(_error_row(address_id, address, str(e)))
                    break
                time.sleep(backoff_base * attempt)

        time.sleep(per_call_sleep)

    return pd.DataFrame(rows, columns=list(GEOCODE_OUTPUT_COLUMNS))


def geocode_to_spark_top1(id_address_pairs, *, api_key: str, spark, **kwargs) -> "Any":
    """Run ``batch_geocode`` and return a Spark DataFrame filtered to the top-1 match per id."""
    from pyspark.sql import functions as F

    pdf = batch_geocode(id_address_pairs, api_key=api_key, **kwargs)
    # Keep one row per id: prefer scoreRank == 1, fall back to the error row
    # for addresses that had no successful candidate.
    if pdf.empty:
        sdf = spark.createDataFrame(pdf, schema=None)
    else:
        # Ensure only one row per id: either the top candidate or the error row
        pdf = pdf.sort_values(["id", "scoreRank"], na_position="last").drop_duplicates(
            subset=["id"], keep="first"
        )
        sdf = spark.createDataFrame(pdf)
    return sdf.withColumn("geocoded_at", F.current_timestamp())


class GeocodeAddresses(Task):
    """Ubunye Task: geocode addresses via TomTom, write top-1 candidates."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        api_key = os.environ.get("TOMTOM_API_KEY")
        if not api_key:
            raise RuntimeError(
                "TOMTOM_API_KEY is not set. The notebook wrapper must export it "
                "from the Databricks secret scope before invoking run_pipeline."
            )

        address_source = sources["address_source"]
        required = {"id", "address"}
        missing = required - set(address_source.columns)
        if missing:
            raise ValueError(
                f"address_source must contain columns: {sorted(required)} (missing: {sorted(missing)})"
            )

        rows = address_source.select("id", "address").toPandas()
        pairs = [(str(r["id"]), r["address"]) for _, r in rows.iterrows()]

        spark = address_source.sparkSession
        geocoded = geocode_to_spark_top1(pairs, api_key=api_key, spark=spark)
        return {"address_geocoded": geocoded}
