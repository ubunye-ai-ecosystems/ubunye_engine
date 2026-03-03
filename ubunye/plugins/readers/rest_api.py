"""REST API reader plugin for Ubunye Engine.

Reads records from a paginated HTTP API and returns a Spark DataFrame.

Supports:
- HTTP methods: GET, POST
- Pagination strategies: offset, cursor, next_link (or none for a single request)
- Authentication: bearer token, api_key (header or query param), basic auth
- Rate limiting with configurable requests_per_second
- Retry with exponential backoff on configurable status codes (default: 429, 503)
- Optional user-defined schema; otherwise infers from response
- Extraction of nested arrays via response.root_key

Example config (config.yaml):
  inputs:
    customer_data:
      format: rest_api
      url: "https://api.example.com/v1/customers"
      method: GET
      headers:
        Authorization: "Bearer {{ env.API_TOKEN }}"
      params:
        since: "{{ ds | default('2025-01-01') }}"
      pagination:
        type: offset          # offset | cursor | next_link
        page_size: 100
        max_pages: 50
      response:
        root_key: "data"      # extract records from response["data"]
      rate_limit:
        requests_per_second: 10
        retry_on: [429, 503]
        max_retries: 3
      schema:
        - name: customer_id
          type: string
        - name: created_at
          type: timestamp
        - name: email
          type: string
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Generator, List, Optional

import requests
from requests.auth import HTTPBasicAuth

from ubunye.core.interfaces import Reader

log = logging.getLogger(__name__)

# Maps simple type names from config schema to PySpark type strings.
_SPARK_TYPE_MAP: Dict[str, str] = {
    "string": "string",
    "str": "string",
    "integer": "integer",
    "int": "integer",
    "long": "long",
    "bigint": "long",
    "double": "double",
    "float": "float",
    "boolean": "boolean",
    "bool": "boolean",
    "timestamp": "timestamp",
    "date": "date",
    "binary": "binary",
}

_DEFAULT_RETRY_ON = [429, 503]
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BACKOFF_BASE = 1.0  # seconds; doubles each attempt


def _build_session(cfg: Dict[str, Any]) -> requests.Session:
    """Build a requests.Session with auth pre-configured.

    Supported auth types (configured under cfg['auth']):
      - bearer:         {'type': 'bearer', 'token': '<value>'}
      - api_key_header: {'type': 'api_key_header', 'header': 'X-Api-Key', 'key': '<value>'}
      - api_key_query:  {'type': 'api_key_query', 'param': 'api_key', 'key': '<value>'}
                        (key appended to every request's params at fetch time)
      - basic:          {'type': 'basic', 'username': '...', 'password': '...'}

    Headers declared at the top-level cfg['headers'] are also applied to the session.
    """
    session = requests.Session()

    # Apply top-level headers (e.g. Authorization: Bearer ... already templated by engine)
    for header, value in (cfg.get("headers") or {}).items():
        session.headers[header] = str(value)

    auth_cfg = cfg.get("auth") or {}
    auth_type = auth_cfg.get("type", "").lower()

    if auth_type == "bearer":
        token = auth_cfg.get("token", "")
        session.headers["Authorization"] = f"Bearer {token}"

    elif auth_type == "api_key_header":
        header_name = auth_cfg.get("header", "X-Api-Key")
        session.headers[header_name] = str(auth_cfg.get("key", ""))

    elif auth_type == "api_key_query":
        # Cannot pre-apply to session; stored for per-request injection.
        # _fetch_page reads cfg['auth'] directly for this case.
        pass

    elif auth_type == "basic":
        session.auth = HTTPBasicAuth(
            auth_cfg.get("username", ""),
            auth_cfg.get("password", ""),
        )

    return session


def _fetch_page(
    session: requests.Session,
    url: str,
    method: str,
    params: Dict[str, Any],
    body: Optional[Dict[str, Any]],
    rate_cfg: Dict[str, Any],
    auth_cfg: Dict[str, Any],
) -> Any:
    """Perform a single HTTP request with retry and rate limiting.

    Parameters
    ----------
    session:   pre-configured requests.Session
    url:       target URL
    method:    'GET' or 'POST'
    params:    query string parameters
    body:      JSON body (POST only)
    rate_cfg:  dict with optional keys requests_per_second, retry_on, max_retries
    auth_cfg:  dict; used to inject api_key_query param if needed

    Returns
    -------
    Parsed JSON response (dict or list).

    Raises
    ------
    requests.HTTPError  if all retries are exhausted on a retryable status code.
    requests.HTTPError  on non-retryable HTTP errors.
    """
    rps: float = float(rate_cfg.get("requests_per_second", 0) or 0)
    retry_on: List[int] = list(rate_cfg.get("retry_on") or _DEFAULT_RETRY_ON)
    max_retries: int = int(rate_cfg.get("max_retries") or _DEFAULT_MAX_RETRIES)

    # Inject api_key_query if configured
    if auth_cfg.get("type") == "api_key_query":
        params = dict(params or {})
        params[auth_cfg.get("param", "api_key")] = auth_cfg.get("key", "")

    for attempt in range(max_retries + 1):
        if rps > 0:
            time.sleep(1.0 / rps)

        resp = session.request(
            method=method.upper(),
            url=url,
            params=params or None,
            json=body or None,
        )

        if resp.status_code in retry_on:
            if attempt < max_retries:
                wait = _DEFAULT_BACKOFF_BASE * (2 ** attempt)
                log.warning(
                    "HTTP %s from %s — retrying in %.1fs (attempt %d/%d)",
                    resp.status_code, url, wait, attempt + 1, max_retries,
                )
                time.sleep(wait)
                continue
            resp.raise_for_status()

        resp.raise_for_status()
        return resp.json()

    # Should never reach here, but satisfy type checker
    raise RuntimeError("Exhausted retries without returning or raising")  # pragma: no cover


def _extract_records(response_json: Any, root_key: Optional[str]) -> List[Dict[str, Any]]:
    """Extract the list of records from a response.

    Parameters
    ----------
    response_json: the parsed JSON from the API (dict or list)
    root_key:      optional key to extract from a dict response (e.g. "data")

    Returns
    -------
    Flat list of record dicts.

    Raises
    ------
    ValueError  if root_key is specified but not found, or result is not a list.
    """
    if root_key:
        if not isinstance(response_json, dict) or root_key not in response_json:
            raise ValueError(
                f"Expected response dict with key '{root_key}', "
                f"got: {type(response_json).__name__}"
            )
        records = response_json[root_key]
    elif isinstance(response_json, list):
        records = response_json
    elif isinstance(response_json, dict):
        # No root_key specified and response is a dict — treat values as single record
        records = [response_json]
    else:
        raise ValueError(f"Cannot extract records from response of type {type(response_json)}")

    if not isinstance(records, list):
        raise ValueError(
            f"Extracted value at root_key='{root_key}' is not a list: {type(records)}"
        )
    return records


def _paginate(
    cfg: Dict[str, Any],
    session: requests.Session,
) -> Generator[List[Dict[str, Any]], None, None]:
    """Yield pages of records using the configured pagination strategy.

    Supported pagination types (cfg['pagination']['type']):
      - offset:    increments an 'offset' param (or 'page' when page_size is set)
      - cursor:    reads next cursor from response, passes as configured param
      - next_link: follows the 'next' URL in response until null

    No pagination config → single request, yielded once.
    """
    url: str = cfg["url"]
    method: str = (cfg.get("method") or "GET").upper()
    params: Dict[str, Any] = dict(cfg.get("params") or {})
    body: Optional[Dict[str, Any]] = cfg.get("body")
    rate_cfg: Dict[str, Any] = cfg.get("rate_limit") or {}
    auth_cfg: Dict[str, Any] = cfg.get("auth") or {}
    root_key: Optional[str] = (cfg.get("response") or {}).get("root_key")

    pag_cfg: Dict[str, Any] = cfg.get("pagination") or {}
    pag_type: str = (pag_cfg.get("type") or "").lower()

    # ---- No pagination: single request ----
    if not pag_type:
        resp = _fetch_page(session, url, method, params, body, rate_cfg, auth_cfg)
        yield _extract_records(resp, root_key)
        return

    # ---- Offset pagination ----
    if pag_type == "offset":
        page_size: int = int(pag_cfg.get("page_size") or 100)
        max_pages: int = int(pag_cfg.get("max_pages") or 0)
        offset_param: str = pag_cfg.get("offset_param", "offset")
        current_params = dict(params)
        current_params.setdefault(offset_param, 0)
        page_count = 0

        while True:
            resp = _fetch_page(session, url, method, current_params, body, rate_cfg, auth_cfg)
            records = _extract_records(resp, root_key)
            if not records:
                break
            yield records
            page_count += 1
            if max_pages and page_count >= max_pages:
                break
            current_params[offset_param] = int(current_params[offset_param]) + page_size

    # ---- Cursor pagination ----
    elif pag_type == "cursor":
        cursor_response_key: str = pag_cfg.get("cursor_response_key", "next_cursor")
        cursor_param: str = pag_cfg.get("cursor_param", "cursor")
        current_params = dict(params)
        max_pages = int(pag_cfg.get("max_pages") or 0)
        page_count = 0

        while True:
            resp = _fetch_page(session, url, method, current_params, body, rate_cfg, auth_cfg)
            records = _extract_records(resp, root_key)
            if records:
                yield records
            page_count += 1

            # Extract cursor from response metadata
            cursor: Optional[str] = None
            if isinstance(resp, dict):
                cursor = resp.get(cursor_response_key)
            if not cursor:
                break
            if max_pages and page_count >= max_pages:
                break
            current_params[cursor_param] = cursor

    # ---- Next-link pagination ----
    elif pag_type == "next_link":
        next_key: str = pag_cfg.get("next_key", "next")
        max_pages = int(pag_cfg.get("max_pages") or 0)
        page_count = 0
        current_url = url
        current_params: Dict[str, Any] = dict(params)

        while current_url:
            resp = _fetch_page(
                session, current_url, method, current_params, body, rate_cfg, auth_cfg
            )
            records = _extract_records(resp, root_key)
            if records:
                yield records
            page_count += 1
            if max_pages and page_count >= max_pages:
                break

            next_url: Optional[str] = None
            if isinstance(resp, dict):
                next_url = resp.get(next_key)
            if not next_url:
                break
            current_url = next_url
            current_params = {}  # next_link URLs already carry their own params

    else:
        raise ValueError(
            f"Unknown pagination type '{pag_type}'. "
            "Expected one of: offset, cursor, next_link"
        )


def _build_schema(schema_cfg: List[Dict[str, str]]):
    """Build a PySpark StructType from a list of {name, type} dicts.

    Lazy-imports pyspark so the module can be imported without Spark installed.
    """
    from pyspark.sql.types import StructType, StructField, StringType  # noqa: F401

    type_map = {
        "string": "StringType",
        "str": "StringType",
        "integer": "IntegerType",
        "int": "IntegerType",
        "long": "LongType",
        "bigint": "LongType",
        "double": "DoubleType",
        "float": "FloatType",
        "boolean": "BooleanType",
        "bool": "BooleanType",
        "timestamp": "TimestampType",
        "date": "DateType",
        "binary": "BinaryType",
    }

    import pyspark.sql.types as T

    fields = []
    for col in schema_cfg:
        name = col["name"]
        raw_type = col.get("type", "string").lower()
        type_class_name = type_map.get(raw_type)
        if not type_class_name:
            raise ValueError(
                f"Unsupported schema type '{raw_type}' for column '{name}'. "
                f"Supported: {sorted(type_map)}"
            )
        spark_type = getattr(T, type_class_name)()
        fields.append(T.StructField(name, spark_type, nullable=True))

    return T.StructType(fields)


class RestApiReader(Reader):
    """Read a Spark DataFrame from a REST API endpoint.

    Handles pagination (offset, cursor, next_link), authentication (bearer,
    api_key, basic), rate limiting, and retry with exponential backoff.
    """

    def read(self, cfg: Dict[str, Any], backend) -> Any:
        """Fetch all pages from the API and return a Spark DataFrame.

        Parameters
        ----------
        cfg : dict
            Reader configuration. Required key: ``url``.
            See module docstring for full reference.
        backend : SparkBackend
            Ubunye Spark backend exposing ``.spark``.

        Returns
        -------
        pyspark.sql.DataFrame
        """
        if not cfg.get("url"):
            raise ValueError("RestApiReader requires 'url' in config")

        session = _build_session(cfg)
        all_records: List[Dict[str, Any]] = []

        try:
            for page in _paginate(cfg, session):
                all_records.extend(page)
                log.debug("Fetched %d records (total so far: %d)", len(page), len(all_records))
        finally:
            session.close()

        log.info("RestApiReader: fetched %d total records from %s", len(all_records), cfg["url"])

        if not all_records:
            log.warning("RestApiReader: no records returned from %s", cfg["url"])

        return self._to_dataframe(all_records, cfg, backend)

    def _to_dataframe(
        self,
        records: List[Dict[str, Any]],
        cfg: Dict[str, Any],
        backend,
    ) -> Any:
        """Convert a list of record dicts to a Spark DataFrame.

        Uses an explicit schema if cfg['schema'] is provided; otherwise infers.
        """
        spark = backend.spark
        schema_cfg = cfg.get("schema")

        if schema_cfg:
            schema = _build_schema(schema_cfg)
            return spark.createDataFrame(records, schema=schema)

        if not records:
            # Return empty DataFrame with a single string column as placeholder
            from pyspark.sql.types import StructType
            return spark.createDataFrame([], StructType([]))

        return spark.createDataFrame(records)
