"""REST API writer plugin for Ubunye Engine.

Converts DataFrame rows to JSON payloads and POSTs them to a REST endpoint
in configurable batch sizes, with retry and exponential backoff.

Authentication, headers, and rate limiting follow the same config shape as
the RestApiReader, and use the same code (:mod:`ubunye.plugins.rest_http`).
The rows come from the backend's ``iter_records``, so the payloads are the same
on Spark and on pandas (F-015).

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
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:  # only for type-checkers; requests is an optional dep
    import requests

from ubunye.core import write_modes
from ubunye.core.capabilities import RECORDS
from ubunye.core.errors import SinkWriteError
from ubunye.core.interfaces import Writer
from ubunye.plugins import rest_http

log = logging.getLogger(__name__)

_DEFAULT_RETRY_ON = [429, 500, 503]
_DEFAULT_BATCH_SIZE = 100


def _build_session(cfg: Dict[str, Any]) -> requests.Session:
    """A session with headers and auth set (see :func:`rest_http.build_session`)."""
    return rest_http.build_session(cfg, SinkWriteError)


def _post_batch(
    session: requests.Session,
    url: str,
    payload: Dict[str, Any],
    rate_cfg: Dict[str, Any],
    auth_cfg: Dict[str, Any],
    secrets: Optional[List[str]] = None,
    body: Optional[bytes] = None,
) -> None:
    """POST one batch with rate limiting and retries.

    ``body`` is the batch already encoded (:func:`rest_http.json_body`); without
    it ``payload`` is encoded here. Raises ``requests.HTTPError`` on an error
    status, once the retries run out, with ``secrets`` masked in its message
    (F-050).
    """
    if body is None:
        body = rest_http.json_body(payload.get("records", []))
    rest_http.send(
        session,
        "POST",
        url,
        params=None,
        body=body,
        rate_cfg=rate_cfg,
        auth_cfg=auth_cfg,
        retry_on_default=_DEFAULT_RETRY_ON,
        secrets=secrets,
    )


def _check_mode(cfg: Dict[str, Any]) -> None:
    """POSTing rows to an endpoint is append-only — no save-mode semantics exist.

    The lakehouse modes are rejected outright: a config asking a REST sink to
    MERGE is a mistake, and silently POSTing instead would hide it. The native
    Spark modes only warn, since configs predating this check may set them.
    """
    mode = (cfg.get("mode") or "").strip().lower()
    if not mode or mode == "append":
        return

    if mode in write_modes.LAKEHOUSE_MODES:
        raise SinkWriteError(
            f"Write mode '{mode}' is not supported by the 'rest_api' connector.",
            context={"Format": "rest_api", "Mode": mode, "Supported": ["append"]},
            hint="A REST sink POSTs rows; it has no table to merge into. Use mode: append.",
        )

    log.warning(
        "RestApiWriter: mode '%s' has no meaning for a REST sink — rows are POSTed "
        "(append semantics) regardless. Remove the mode key or set mode: append.",
        mode,
    )


class RestApiWriter(Writer):
    """Write a frame to a REST API endpoint in JSON batches.

    Takes the rows from the backend (``iter_records``), groups them into batches
    of ``batch_size``, and POSTs each batch as ``{"records": [...]}``. Tracks and
    logs success/failure counts per batch.
    """

    # The settings this connector reads (typos in any other key fail validation).
    CONFIG_KEYS = frozenset({"url", "method", "headers", "auth", "batch_size", "rate_limit"})

    # Needs a backend that gives rows back as records; checked before a run (ADR 002).
    REQUIRES = frozenset({RECORDS})

    SUPPORTS_MERGE = False
    MERGE_FILE_FORMATS = frozenset()

    @classmethod
    def validate_config(cls, cfg):
        problems = [] if cfg.get("url") else ["format 'rest_api' requires 'url'"]
        return problems + rest_http.config_problems(cfg)

    def write(self, df: Any, cfg: Dict[str, Any], backend) -> None:
        """POST the frame's rows to a REST endpoint in batches.

        Parameters
        ----------
        df : frame
            The output, as the engine hands it to writers.
        cfg : dict
            Writer configuration. Required key: ``url``.
            See module docstring for full reference.
        backend : Backend
            Any backend with the ``records`` capability (spark, databricks, pandas).
        """
        if not cfg.get("url"):
            raise SinkWriteError(
                "RestApiWriter requires 'url' in config.",
                context={"Format": "rest_api"},
                hint="Set url: 'https://api.example.com/...' in your output config.",
            )

        _check_mode(cfg)
        rest_http.check_backend(backend, SinkWriteError)
        cfg = rest_http.normalized(cfg, SinkWriteError)

        url: str = cfg["url"]
        batch_size: int = int(cfg.get("batch_size") or _DEFAULT_BATCH_SIZE)
        rate_cfg: Dict[str, Any] = cfg.get("rate_limit") or {}
        auth_cfg: Dict[str, Any] = cfg.get("auth") or {}

        session = _build_session(cfg)
        success_count = 0
        failure_count = 0
        batch: List[Dict[str, Any]] = []
        self._secrets = rest_http.secrets_of(cfg)
        where = rest_http.redact(url, self._secrets)

        # No secret in any log line while the requests run (urllib3 logs URLs).
        with rest_http.masked(self._secrets, __name__):
            try:
                for record in rest_http.records_of(df, backend):
                    batch.append(record)

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
            where,
            success_count,
            failure_count,
        )

        if failure_count > 0:
            raise SinkWriteError(
                f"RestApiWriter: {failure_count} batch(es) failed posting to {where}.",
                context={"Format": "rest_api", "URL": where, "Failed batches": failure_count},
                hint="Check logs for per-batch HTTP errors.",
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

        The whole batch is encoded first, so a row that cannot be sent stops the
        write before any of this batch is posted (F-051). Returns updated
        (success_count, failure_count).
        """
        import requests as _requests

        payload = {"records": batch}
        try:
            body = rest_http.json_body(batch)
        except ValueError as exc:
            raise SinkWriteError(
                f"RestApiWriter: a row cannot be sent as JSON ({exc}). Nothing of this "
                f"batch was posted; {success_count} earlier batch(es) were.",
                context={"Format": "rest_api", "Posted batches": success_count},
                hint="Cast the column to a string, number or date in the transform.",
            ) from None
        try:
            _post_batch(
                session,
                url,
                payload,
                rate_cfg,
                auth_cfg,
                secrets=getattr(self, "_secrets", None),
                body=body,
            )
            success_count += 1
            log.debug("RestApiWriter: batch of %d rows posted successfully", len(batch))
        except _requests.HTTPError as exc:
            failure_count += 1
            log.error("RestApiWriter: batch of %d rows failed — %s", len(batch), exc)
        return success_count, failure_count
