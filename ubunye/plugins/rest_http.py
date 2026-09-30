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
) -> Any:
    """One request with rate limiting and retries; returns the response.

    ``rate_cfg`` may set ``requests_per_second``, ``retry_on`` (status codes)
    and ``max_retries``. A retryable status waits 1 s, 2 s, 4 s... between
    attempts. Raises ``requests.HTTPError`` for any other error status, or for a
    retryable one once the retries run out.
    """
    rps = float(rate_cfg.get("requests_per_second", 0) or 0)
    retry_on = list(rate_cfg.get("retry_on") or retry_on_default)
    max_retries = int(rate_cfg.get("max_retries") or DEFAULT_MAX_RETRIES)

    if auth_cfg.get("type") == "api_key_query":
        params = dict(params or {})
        params[auth_cfg.get("param", "api_key")] = auth_cfg.get("key", "")

    for attempt in range(max_retries + 1):
        if rps > 0:
            time.sleep(1.0 / rps)
        resp = session.request(method=method.upper(), url=url, params=params or None, json=body)
        if resp.status_code in retry_on and attempt < max_retries:
            wait = BACKOFF_BASE * (2**attempt)
            log.warning(
                "HTTP %s from %s %s, retrying in %.1fs (attempt %d/%d)",
                resp.status_code,
                method.upper(),
                url,
                wait,
                attempt + 1,
                max_retries,
            )
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp
    raise RuntimeError("Exhausted retries without returning or raising")  # pragma: no cover
