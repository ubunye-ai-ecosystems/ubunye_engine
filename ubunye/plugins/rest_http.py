"""The HTTP side of the ``rest_api`` reader and writer, shared by both (F-015).

Sessions, auth, headers, rate limiting and retries have nothing to do with the
engine that holds the data, so they live here once, and the reader and the
writer on every backend use the same code. Only the last step is the backend's:
turning records into its frame (reader) or its frame into records (writer),
through ``Backend.frame_from_records`` and ``Backend.iter_records``.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Type

from ubunye.core.capabilities import RECORDS, Capabilities, provided
from ubunye.core.errors import UbunyeError

if TYPE_CHECKING:  # only for type checkers; requests is an optional dependency
    import requests

log = logging.getLogger(__name__)

DEFAULT_MAX_RETRIES = 3
BACKOFF_BASE = 1.0  # seconds; doubles each attempt


def requests_module(error: Type[UbunyeError]) -> Any:
    """Import ``requests``, and say something useful if it is not there.

    It used to be declared only in the ``dev`` extra, so ``pip install
    ubunye-engine[spark]`` gave a rest_api connector that could not make a
    request. On Databricks it worked by accident (the runtime preinstalls it).
    """
    try:
        import requests  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - exercised by the message, not the path
        raise error(
            "The 'rest_api' connector needs the `requests` library, which is not installed.",
            context={"Format": "rest_api"},
            hint="pip install 'ubunye-engine[rest]'  (or add requests to your environment). "
            "It is not installed by default because not every pipeline makes HTTP calls.",
        ) from exc
    return requests


def check_backend(backend: Any, error: Type[UbunyeError]) -> None:
    """Refuse, before any HTTP call, a backend that says it cannot hold records.

    The engine checks this before a run too (ADR 002); this covers a connector
    called directly. A backend that declares nothing is not refused.
    """
    caps_of = getattr(backend, "capabilities", None)
    if not callable(caps_of):
        return
    caps = caps_of()
    if isinstance(caps, Capabilities) and caps.declared and RECORDS not in provided(caps):
        kind = type(backend).__name__
        raise error(
            f"The 'rest_api' connector needs a backend that builds frames from records, "
            f"and {kind} does not.",
            context={"connector": "rest_api", "Backend": kind},
            hint="Run with --backend spark or --backend pandas.",
        )


def frame_of(backend: Any, records: List[Dict[str, Any]], schema: Optional[str]) -> Any:
    """The backend's frame from records; the Spark way for a backend that predates them.

    A plugin backend with a SparkSession (``backend.spark``) that does not
    implement ``frame_from_records`` gets ``createDataFrame``, as the connector
    did before F-015.
    """
    try:
        return backend.frame_from_records(records, schema=schema)
    except NotImplementedError:
        try:
            spark = getattr(backend, "spark", None)
        except Exception:  # noqa: BLE001 (a session property that needs start())
            spark = None
        if spark is None:
            raise
    from ubunye.adapters.spark import frame_io

    return frame_io.frame_from_records(spark, records, schema=schema)


def records_of(frame: Any, backend: Any) -> Iterator[Dict[str, Any]]:
    """The frame's rows as dicts: the backend's ``iter_records``, else Spark's way.

    A backend that cannot say (``None``, or one written before ``iter_records``
    with no SparkSession) gets what the writer did before F-015: the frame's own
    ``toLocalIterator``, each row as a dict.
    """
    if backend is not None:
        try:
            return iter(backend.iter_records(frame))
        except (NotImplementedError, AttributeError):
            pass
    from ubunye.adapters.spark import frame_io

    return frame_io.iter_records(frame)


# --------------------------------------------------------------------------- #
# Secrets out of logs and errors (F-050)
# --------------------------------------------------------------------------- #


def secrets_of(cfg: Dict[str, Any]) -> List[str]:
    """Every secret this config sends, longest first, to mask wherever it shows.

    The auth token, key and password (and basic auth's encoded form), the value
    of a header that is ``Authorization`` or has a secret-looking name, and the
    value of a query parameter with a secret-looking name.
    """
    import base64

    from ubunye.core.secrets import looks_secret

    found = set()
    auth_cfg = cfg.get("auth") or {}
    if isinstance(auth_cfg, dict):
        for name in ("token", "key", "password"):
            if auth_cfg.get(name):
                found.add(str(auth_cfg[name]))
        if auth_cfg.get("password"):
            pair = f"{auth_cfg.get('username', '')}:{auth_cfg['password']}"
            found.add(base64.b64encode(pair.encode()).decode())
    for header, value in (cfg.get("headers") or {}).items():
        if value and (str(header).lower() == "authorization" or looks_secret(header)):
            found.add(str(value))
            found.update(str(value).split()[1:])  # "Bearer <token>": the token alone too
    for param, value in (cfg.get("params") or {}).items():
        if value and looks_secret(param):
            found.add(str(value))
    return sorted((s for s in found if len(s) >= 4), key=len, reverse=True)


def redact(text: Any, secrets: List[str]) -> str:
    """``text`` with every secret masked, and secret-named URL parameters too.

    The core's :func:`ubunye.core.secrets.mask_text`: one implementation for the
    connector's logs and errors and the run record.
    """
    from ubunye.core.secrets import mask_text

    return mask_text(text, secrets)


class _Redacting(logging.Filter):
    """Masks secrets in the records a logger writes (urllib3 logs each URL)."""

    def __init__(self, secrets: List[str]) -> None:
        super().__init__()
        self.secrets = secrets

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage(), self.secrets)
        record.args = None
        return True


#: Loggers that write request URLs: urllib3 logs ``"GET /path?query HTTP/1.1"``.
_URL_LOGGERS = ("urllib3.connectionpool",)


class masked:
    """While open, secrets are masked in this module's and urllib3's log records."""

    def __init__(self, secrets: List[str], *also: str) -> None:
        self.filter = _Redacting(secrets)
        self.loggers = [log] + [logging.getLogger(n) for n in (*_URL_LOGGERS, *also)]

    def __enter__(self) -> "masked":
        for logger in self.loggers:
            logger.addFilter(self.filter)
        return self

    def __exit__(self, *exc: Any) -> None:
        for logger in self.loggers:
            logger.removeFilter(self.filter)


# --------------------------------------------------------------------------- #
# Rows as JSON (F-051)
# --------------------------------------------------------------------------- #


def jsonable(value: Any) -> Any:
    """One value as JSON can hold it, the same from every backend.

    NaN and the infinities are ``null`` (JSON has neither, and requests refuses
    them); a timestamp is ISO 8601 text, in UTC with ``Z`` when it is an instant,
    without an offset when it is wall clock time (``timestamp_ntz``); a date is
    ``yyyy-mm-dd``; a decimal is its exact text (a string, so no digit is lost to
    a float); binary is base64 text. Raises ``TypeError`` for anything else.
    """
    import base64
    import datetime as dt
    import decimal
    import math

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            text = value.astimezone(dt.timezone.utc).replace(tzinfo=None).isoformat()
            return text + "Z"
        return value.isoformat()
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [jsonable(v) for v in value]
    raise TypeError(f"a {type(value).__name__} value cannot be sent as JSON")


def json_body(records: List[Dict[str, Any]]) -> bytes:
    """A batch as the bytes to POST: ``{"records": [...]}``, every row checked first.

    The whole batch is encoded before anything is sent, so a row that cannot be
    sent stops the batch before its first byte leaves (F-051). Raises
    ``ValueError`` naming the row and the field.
    """
    import json

    rows = []
    for i, record in enumerate(records):
        try:
            rows.append(jsonable(record))
        except TypeError as exc:
            field = (
                next((k for k, v in record.items() if _unsendable(v)), "?")
                if isinstance(record, dict)
                else "?"
            )
            raise ValueError(f"row {i} of the batch, field '{field}': {exc}") from None
    return json.dumps({"records": rows}, allow_nan=False).encode("utf-8")


def _unsendable(value: Any) -> bool:
    try:
        jsonable(value)
        return False
    except TypeError:
        return True


AUTH_TYPES = ("bearer", "api_key_header", "api_key_query", "basic")

#: Old names the docs used to print (F-049), still read, with a warning.
PAGINATION_ALIASES = {"cursor_field": "cursor_response_key", "link_field": "next_key"}


def _auth_type(auth_cfg: Dict[str, Any]) -> Any:
    """The auth type, with the old ``api_key`` resolved; a problem text if it cannot be."""
    kind = str(auth_cfg.get("type", "") or "").lower()
    if kind == "api_key":
        has_header, has_param = "header" in auth_cfg, "param" in auth_cfg
        if has_header == has_param:
            return None, (
                "auth type 'api_key' needs exactly one of 'header' or 'param'; better, "
                "use type: api_key_header (with header) or api_key_query (with param)"
            )
        return ("api_key_header" if has_header else "api_key_query"), None
    if kind and kind not in AUTH_TYPES:
        return None, f"unknown auth type '{kind}'; use one of {', '.join(AUTH_TYPES)}"
    return kind, None


def config_problems(cfg: Dict[str, Any]) -> List[str]:
    """What is wrong with a rest_api config's auth, found before any request."""
    auth_cfg = cfg.get("auth") or {}
    if not isinstance(auth_cfg, dict):
        return ["'auth' must be a mapping"]
    _, problem = _auth_type(auth_cfg)
    return [problem] if problem else []


def normalized(cfg: Dict[str, Any], error: Type[UbunyeError]) -> Dict[str, Any]:
    """The config with the old option names turned into the ones the code reads.

    The docs once printed ``auth: {type: api_key}``, ``cursor_field`` and
    ``link_field``; the code read none of them, so an ``api_key`` config sent no
    key at all (F-049). They are still accepted, with a warning; an auth type
    nobody knows is refused instead of silently sending nothing.
    """
    problems = config_problems(cfg)
    if problems:
        raise error(
            f"rest_api: {problems[0]}.",
            context={"Format": "rest_api"},
            hint="See docs/connectors/rest_api.md, Authentication.",
        )
    out = dict(cfg)
    auth_cfg = dict(cfg.get("auth") or {})
    if str(auth_cfg.get("type", "")).lower() == "api_key":
        kind, _ = _auth_type(auth_cfg)
        log.warning("rest_api: auth type 'api_key' is an old name; use type: %s.", kind)
        auth_cfg["type"] = kind
        out["auth"] = auth_cfg
    pag_cfg = dict(cfg.get("pagination") or {})
    for old, new in PAGINATION_ALIASES.items():
        if old in pag_cfg:
            log.warning("rest_api: pagination '%s' is an old name; use '%s'.", old, new)
            pag_cfg.setdefault(new, pag_cfg.pop(old))
    if pag_cfg:
        out["pagination"] = pag_cfg
    return out


def build_session(cfg: Dict[str, Any], error: Type[UbunyeError]) -> "requests.Session":
    """A ``requests.Session`` with headers and auth set.

    Auth types (``cfg['auth']['type']``):

    - ``bearer``: ``Authorization: Bearer <token>``
    - ``api_key_header``: a header (``header``, default ``X-Api-Key``) with ``key``
    - ``api_key_query``: ``key`` as a query parameter, added per request by :func:`send`
    - ``basic``: HTTP basic auth with ``username`` and ``password``

    Headers in ``cfg['headers']`` are set too.
    """
    requests = requests_module(error)
    from requests.auth import HTTPBasicAuth  # noqa: PLC0415

    session = requests.Session()
    for header, value in (cfg.get("headers") or {}).items():
        session.headers[header] = str(value)

    auth_cfg = cfg.get("auth") or {}
    auth_type = str(auth_cfg.get("type", "")).lower()
    if auth_type == "bearer":
        session.headers["Authorization"] = f"Bearer {auth_cfg.get('token', '')}"
    elif auth_type == "api_key_header":
        session.headers[auth_cfg.get("header", "X-Api-Key")] = str(auth_cfg.get("key", ""))
    elif auth_type == "basic":
        session.auth = HTTPBasicAuth(auth_cfg.get("username", ""), auth_cfg.get("password", ""))
    return session


def send(
    session: "requests.Session",
    method: str,
    url: str,
    *,
    params: Optional[Dict[str, Any]],
    body: Any,
    rate_cfg: Dict[str, Any],
    auth_cfg: Dict[str, Any],
    retry_on_default: List[int],
    secrets: Optional[List[str]] = None,
) -> Any:
    """One request with rate limiting and retries; returns the response.

    ``rate_cfg`` may set ``requests_per_second``, ``retry_on`` (status codes)
    and ``max_retries``. A retryable status waits 1 s, 2 s, 4 s... between
    attempts. Raises ``requests.HTTPError`` for any other error status, or for a
    retryable one once the retries run out. The message of any error raised
    here has ``secrets`` (and secret-named URL parameters) masked: requests puts
    the full URL, query string and all, into it (F-050).
    """
    try:
        return _send(
            session, method, url, params, body, rate_cfg, auth_cfg, retry_on_default, secrets
        )
    except Exception as exc:
        requests = requests_module(UbunyeError)
        if not isinstance(exc, requests.RequestException):
            raise
        masked_exc = type(exc)(redact(exc, secrets or []), response=getattr(exc, "response", None))
        raise masked_exc from None


def _send(
    session: "requests.Session",
    method: str,
    url: str,
    params: Optional[Dict[str, Any]],
    body: Any,
    rate_cfg: Dict[str, Any],
    auth_cfg: Dict[str, Any],
    retry_on_default: List[int],
    secrets: Optional[List[str]],
) -> Any:
    rps = float(rate_cfg.get("requests_per_second", 0) or 0)
    retry_on = list(rate_cfg.get("retry_on") or retry_on_default)
    max_retries = int(rate_cfg.get("max_retries") or DEFAULT_MAX_RETRIES)

    if auth_cfg.get("type") == "api_key_query":
        params = dict(params or {})
        params[auth_cfg.get("param", "api_key")] = auth_cfg.get("key", "")

    for attempt in range(max_retries + 1):
        if rps > 0:
            time.sleep(1.0 / rps)
        payload: Dict[str, Any]
        if isinstance(body, bytes):  # already encoded (the writer: see json_body)
            payload = {"data": body, "headers": {"Content-Type": "application/json"}}
        else:
            payload = {"json": body}
        resp = session.request(method=method.upper(), url=url, params=params or None, **payload)
        if resp.status_code in retry_on and attempt < max_retries:
            wait = BACKOFF_BASE * (2**attempt)
            log.warning(
                "HTTP %s from %s %s, retrying in %.1fs (attempt %d/%d)",
                resp.status_code,
                method.upper(),
                redact(url, secrets or []),
                wait,
                attempt + 1,
                max_retries,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError("Exhausted retries without returning or raising")  # pragma: no cover
