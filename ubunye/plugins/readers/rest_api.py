"""REST API reader plugin for Ubunye Engine.

Reads records from a paginated HTTP API and returns the backend's frame: a Spark
DataFrame on Spark, a pandas DataFrame on pandas, with the same rows and types.

Supports:
- HTTP methods: GET, POST
- Pagination strategies: offset, cursor, next_link (or none for a single request)
- Authentication: bearer token, api_key (header or query param), basic auth
- Rate limiting with configurable requests_per_second
- Retry with exponential backoff on configurable status codes (default: 429, 503)
- Optional user-defined schema; otherwise inferred from every record
- Extraction of nested arrays via response.root_key

The HTTP side (session, auth, retries, rate limit) is shared with the writer in
:mod:`ubunye.plugins.rest_http`. The last step, records to a frame, is the
backend's ``frame_from_records`` (F-015).

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
        - name: email
          type: string
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional

if TYPE_CHECKING:  # only for type-checkers; requests is an optional dep
    import requests

from ubunye.core.capabilities import RECORDS
from ubunye.core.errors import SourceReadError
from ubunye.core.interfaces import Reader
from ubunye.plugins import rest_http

log = logging.getLogger(__name__)

# The schema's type names, as Spark DDL types (the form every backend reads).
_SPARK_TYPE_MAP: Dict[str, str] = {
    "string": "STRING",
    "str": "STRING",
    "integer": "INT",
    "int": "INT",
    "long": "BIGINT",
    "bigint": "BIGINT",
    "double": "DOUBLE",
    "float": "FLOAT",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "date": "DATE",
    "binary": "BINARY",
}

_DEFAULT_RETRY_ON = [429, 503]


def _requests():
    """``requests``, or a clear error naming the extra that installs it."""
    return rest_http.requests_module(SourceReadError)


def _build_session(cfg: Dict[str, Any]) -> requests.Session:
    """A session with headers and auth set (see :func:`rest_http.build_session`)."""
    return rest_http.build_session(cfg, SourceReadError)


def _fetch_page(
    session: requests.Session,
    url: str,
    method: str,
    params: Dict[str, Any],
    body: Optional[Dict[str, Any]],
    rate_cfg: Dict[str, Any],
    auth_cfg: Dict[str, Any],
) -> Any:
    """One page: a request with rate limiting and retries, as parsed JSON.

    Raises ``requests.HTTPError`` on an error status, once the retries run out.
    """
    resp = rest_http.send(
        session,
        method,
        url,
        params=params,
        body=body or None,
        rate_cfg=rate_cfg,
        auth_cfg=auth_cfg,
        retry_on_default=_DEFAULT_RETRY_ON,
    )
    return resp.json()


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
            raise SourceReadError(
                f"Expected response dict with key '{root_key}'.",
                context={
                    "Format": "rest_api",
                    "root_key": root_key,
                    "Actual type": type(response_json).__name__,
                },
                hint=f"Check response.root_key in your config. The API returned a {type(response_json).__name__}.",
            )
        records = response_json[root_key]
    elif isinstance(response_json, list):
        records = response_json
    elif isinstance(response_json, dict):
        # No root_key specified and response is a dict — treat values as single record
        records = [response_json]
    else:
        raise SourceReadError(
            f"Cannot extract records from response of type {type(response_json).__name__}.",
            context={"Format": "rest_api", "Actual type": type(response_json).__name__},
            hint="The API response must be a JSON array or object.",
        )

    if not isinstance(records, list):
        raise SourceReadError(
            f"Extracted value at root_key='{root_key}' is not a list.",
            context={
                "Format": "rest_api",
                "root_key": root_key,
                "Actual type": type(records).__name__,
            },
            hint="The value under response.root_key must be a JSON array of records.",
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
        current_params: Dict[str, Any] = dict(params)
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
        current_params = dict(params)

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
        raise SourceReadError(
            f"Unknown pagination type '{pag_type}'.",
            context={"Format": "rest_api", "pagination.type": pag_type},
            hint="Expected one of: offset, cursor, next_link.",
        )


def _schema_ddl(schema_cfg: List[Dict[str, str]]) -> str:
    """The config's ``schema`` list as a Spark DDL string, the form every backend reads."""
    parts = []
    for col in schema_cfg:
        name = col["name"]
        raw_type = str(col.get("type", "string")).lower()
        ddl_type = _SPARK_TYPE_MAP.get(raw_type)
        if not ddl_type:
            raise SourceReadError(
                f"Unsupported schema type '{raw_type}' for column '{name}'.",
                context={"Format": "rest_api", "Column": name, "Type": raw_type},
                hint=f"Supported types: {', '.join(sorted(_SPARK_TYPE_MAP))}",
            )
        parts.append("`" + str(name).replace("`", "``") + "` " + ddl_type)
    return ", ".join(parts)


class RestApiReader(Reader):
    """Read records from a REST API endpoint into the backend's frame.

    Handles pagination (offset, cursor, next_link), authentication (bearer,
    api_key, basic), rate limiting, and retry with exponential backoff. The HTTP
    side is the same on every backend; the backend builds the frame
    (``frame_from_records``), typed as Spark's ``createDataFrame`` types it.
    """

    # The settings this connector reads (typos in any other key fail validation).
    CONFIG_KEYS = frozenset(
        {
            "url",
            "method",
            "headers",
            "params",
            "body",
            "auth",
            "pagination",
            "response",
            "rate_limit",
            "schema",
        }
    )

    # Needs a backend that builds a frame from records; checked before a run (ADR 002).
    REQUIRES = frozenset({RECORDS})

    @classmethod
    def validate_config(cls, cfg):
        return [] if cfg.get("url") else ["format 'rest_api' requires 'url'"]

    def read(self, cfg: Dict[str, Any], backend) -> Any:
        """Fetch all pages from the API and return the backend's frame.

        Parameters
        ----------
        cfg : dict
            Reader configuration. Required key: ``url``.
            See module docstring for full reference.
        backend : Backend
            Any backend with the ``records`` capability (spark, databricks, pandas).
        """
        if not cfg.get("url"):
            raise SourceReadError(
                "RestApiReader requires 'url' in config.",
                context={"Format": "rest_api"},
                hint="Set url: 'https://api.example.com/...' in your input config.",
            )
        # Refuse before any HTTP call: a backend that cannot hold records, or a
        # schema type no backend knows.
        rest_http.check_backend(backend, SourceReadError)
        schema_cfg = cfg.get("schema")
        schema = _schema_ddl(schema_cfg) if schema_cfg else None

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

        try:
            return backend.frame_from_records(all_records, schema=schema)
        except (TypeError, ValueError) as exc:
            # Spark's own type errors (PySparkTypeError is a TypeError,
            # PySparkValueError a ValueError) and the pandas backend's port of them.
            raise SourceReadError(
                f"The records from {cfg['url']} do not fit one table: {exc}",
                context={"Format": "rest_api", "Records": len(all_records)},
                hint="A field must hold one kind of value in every record, and not be "
                "null in all of them. Declare it under schema: as string to take any "
                "value as text, or fix the field in the API's response.",
            ) from exc
