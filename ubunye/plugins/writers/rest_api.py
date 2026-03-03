"""REST API writer plugin for Ubunye Engine.

Converts DataFrame rows to JSON payloads and POSTs them to a REST endpoint
in configurable batch sizes, with retry and exponential backoff.

Authentication, headers, and rate limiting follow the same config shape as
the RestApiReader (see ubunye/plugins/readers/rest_api.py).

Example config (config.yaml):
  outputs:
    risk_alerts:
      format: rest_api
      url: "https://api.example.com/v1/alerts"
      method: POST
      headers:
        Authorization: "Bearer {{ env.API_TOKEN }}"
      batch_size: 50
      rate_limit:
        requests_per_second: 5
        retry_on: [429, 500, 503]
        max_retries: 3

The writer POSTs each batch as: {"records": [<row>, <row>, ...]}

Success and failure counts are logged at INFO level.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

import requests
from requests.auth import HTTPBasicAuth

from ubunye.core.interfaces import Writer

log = logging.getLogger(__name__)

_DEFAULT_RETRY_ON = [429, 500, 503]
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BATCH_SIZE = 100
_DEFAULT_BACKOFF_BASE = 1.0  # seconds; doubles each attempt


def _build_session(cfg: Dict[str, Any]) -> requests.Session:
    """Build a requests.Session with auth and headers pre-configured.

    Mirrors the reader's session builder — kept self-contained so each plugin
    file is independent (matching the existing plugin style in this codebase).

    Supported auth types (cfg['auth']['type']):
      - bearer         — Authorization: Bearer <token>
      - api_key_header — custom header name + key
      - api_key_query  — key injected into query params per-request (not here)
      - basic          — HTTPBasicAuth(username, password)
    """
    session = requests.Session()

    for header, value in (cfg.get("headers") or {}).items():
        session.headers[header] = str(value)

    auth_cfg = cfg.get("auth") or {}
    auth_type = auth_cfg.get("type", "").lower()

    if auth_type == "bearer":
        session.headers["Authorization"] = f"Bearer {auth_cfg.get('token', '')}"

    elif auth_type == "api_key_header":
        header_name = auth_cfg.get("header", "X-Api-Key")
        session.headers[header_name] = str(auth_cfg.get("key", ""))

    elif auth_type == "api_key_query":
        pass  # injected per-request in _post_batch

    elif auth_type == "basic":
        session.auth = HTTPBasicAuth(
            auth_cfg.get("username", ""),
            auth_cfg.get("password", ""),
        )

    return session


def _post_batch(
    session: requests.Session,
    url: str,
    payload: Dict[str, Any],
    rate_cfg: Dict[str, Any],
    auth_cfg: Dict[str, Any],
) -> None:
    """POST a single batch payload with retry and rate limiting.

    Parameters
    ----------
    session:   pre-configured requests.Session
    url:       target URL
    payload:   JSON-serialisable dict to POST
    rate_cfg:  dict with optional keys requests_per_second, retry_on, max_retries
    auth_cfg:  dict; used to inject api_key_query param if needed

    Raises
    ------
    requests.HTTPError  on non-retryable errors or after all retries exhausted.
    """
    rps: float = float(rate_cfg.get("requests_per_second", 0) or 0)
    retry_on: List[int] = list(rate_cfg.get("retry_on") or _DEFAULT_RETRY_ON)
    max_retries: int = int(rate_cfg.get("max_retries") or _DEFAULT_MAX_RETRIES)

    params: Optional[Dict[str, Any]] = None
    if auth_cfg.get("type") == "api_key_query":
        params = {auth_cfg.get("param", "api_key"): auth_cfg.get("key", "")}

    for attempt in range(max_retries + 1):
        if rps > 0:
            time.sleep(1.0 / rps)

        resp = session.post(url, json=payload, params=params)

        if resp.status_code in retry_on:
            if attempt < max_retries:
                wait = _DEFAULT_BACKOFF_BASE * (2**attempt)
                log.warning(
                    "HTTP %s posting to %s — retrying in %.1fs (attempt %d/%d)",
                    resp.status_code,
                    url,
                    wait,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()

        resp.raise_for_status()
        return

    raise RuntimeError("Exhausted retries without returning or raising")  # pragma: no cover


def _row_to_dict(row) -> Dict[str, Any]:
    """Convert a PySpark Row to a plain dict."""
    return row.asDict(recursive=True)


class RestApiWriter(Writer):
    """Write a Spark DataFrame to a REST API endpoint in JSON batches.

    Iterates over DataFrame rows, groups them into batches of ``batch_size``,
    and POSTs each batch as ``{"records": [...]}``. Tracks and logs success/
    failure counts per batch.
    """

    def write(self, df: Any, cfg: Dict[str, Any], backend) -> None:
        """POST DataFrame rows to a REST endpoint in batches.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
            Source DataFrame.
        cfg : dict
            Writer configuration. Required key: ``url``.
            See module docstring for full reference.
        backend : SparkBackend
            Ubunye Spark backend (passed for interface consistency; not used directly).

        Raises
        ------
        ValueError  if ``url`` is missing from cfg.
        """
        if not cfg.get("url"):
            raise ValueError("RestApiWriter requires 'url' in config")

        url: str = cfg["url"]
        batch_size: int = int(cfg.get("batch_size") or _DEFAULT_BATCH_SIZE)
        rate_cfg: Dict[str, Any] = cfg.get("rate_limit") or {}
        auth_cfg: Dict[str, Any] = cfg.get("auth") or {}

        session = _build_session(cfg)
        success_count = 0
        failure_count = 0
        batch: List[Dict[str, Any]] = []

        try:
            for row in df.toLocalIterator():
                batch.append(_row_to_dict(row))

                if len(batch) >= batch_size:
                    success_count, failure_count = self._flush_batch(
                        session, url, batch, rate_cfg, auth_cfg, success_count, failure_count
                    )
                    batch = []

            # Flush remaining rows
            if batch:
                success_count, failure_count = self._flush_batch(
                    session, url, batch, rate_cfg, auth_cfg, success_count, failure_count
                )

        finally:
            session.close()

        log.info(
            "RestApiWriter: posted to %s — %d batches succeeded, %d failed",
            url,
            success_count,
            failure_count,
        )

        if failure_count > 0:
            raise RuntimeError(
                f"RestApiWriter: {failure_count} batch(es) failed posting to {url}. "
                "Check logs for details."
            )

    def _flush_batch(
        self,
        session: requests.Session,
        url: str,
        batch: List[Dict[str, Any]],
        rate_cfg: Dict[str, Any],
        auth_cfg: Dict[str, Any],
        success_count: int,
        failure_count: int,
    ):
        """POST a single batch and update counters.

        Returns updated (success_count, failure_count).
        """
        payload = {"records": batch}
        try:
            _post_batch(session, url, payload, rate_cfg, auth_cfg)
            success_count += 1
            log.debug("RestApiWriter: batch of %d rows posted successfully", len(batch))
        except requests.HTTPError as exc:
            failure_count += 1
            log.error("RestApiWriter: batch of %d rows failed — %s", len(batch), exc)
        return success_count, failure_count
