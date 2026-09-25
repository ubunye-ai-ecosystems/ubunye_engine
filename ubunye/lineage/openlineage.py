"""OpenLineage events from Ubunye's run records.

Every recorded run emits a ``START`` event when it begins and a ``COMPLETE`` or
``FAIL`` event when it ends (OpenLineage 2-0-2), so the run lands in any catalogue
that reads OpenLineage: Marquez, DataHub, OpenMetadata, Google Dataplex, and
others. Nothing extra to install: the events are plain JSON sent with the
standard library.

Configured the way OpenLineage clients are, by environment variables:

``OPENLINEAGE_URL``          send over HTTP to this server (e.g. Marquez)
``OPENLINEAGE_ENDPOINT``     path on it, default ``api/v1/lineage``
``OPENLINEAGE_API_KEY``      sent as ``Authorization: Bearer <key>``
``OPENLINEAGE_NAMESPACE``    the job namespace, default ``ubunye``
``OPENLINEAGE_DISABLED``     ``true`` turns emission off
``UBUNYE_OPENLINEAGE_FILE``  also append every event, one JSON per line, to this file

What goes in each event:

- the job (``namespace``, ``usecase.package.task``) and the run (its UUID);
- every input and output as a dataset named by the OpenLineage naming
  conventions (``s3://bucket`` + key, ``gs://bucket`` + path, ``file`` + path,
  ``hive`` + ``db.table``, ...); credentials in a URL are removed;
- standard facets: ``outputStatistics`` (rows written), ``dataQualityMetrics``
  (rows read), ``dataQualityAssertions`` (each expectation, passed or not);
- a ``ubunye_evidence`` facet on the run and a ``ubunye_hash`` facet on each
  dataset, carrying the receipt: config, code and environment hashes, row hashes,
  and per-step timings.

Emission never fails a run: a server that is down is logged and skipped.
"""

from __future__ import annotations

import json
import logging
import os
import re
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlsplit, urlunsplit

from ubunye.lineage.context import RunContext, StepRecord

log = logging.getLogger(__name__)

SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
FACETS = "https://openlineage.io/spec/facets"
EVIDENCE_SCHEMA = (
    "https://raw.githubusercontent.com/ubunye-ai-ecosystems/ubunye_engine/main/"
    "docs/schemas/ubunye_evidence.json"
)
HASH_SCHEMA = (
    "https://raw.githubusercontent.com/ubunye-ai-ecosystems/ubunye_engine/main/"
    "docs/schemas/ubunye_hash.json"
)
TRUE = ("1", "true", "yes", "on")


def producer(version: str) -> str:
    """The OpenLineage ``producer``: this engine, at this version."""
    return f"https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/v{version or 'dev'}"


# --- dataset names (https://openlineage.io/docs/spec/naming) ------------------------

_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")


def _without_credentials(url: str) -> str:
    parts = urlsplit(url)
    if "@" not in parts.netloc:
        return url
    host = parts.netloc.rsplit("@", 1)[1]
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))


def dataset_id(step: StepRecord) -> Dict[str, str]:
    """``{"namespace", "name"}`` for an input or output, by the naming conventions."""
    loc = step.location or ""
    fmt = step.format
    if loc.startswith("secret://"):
        return {"namespace": "secret", "name": loc}
    if fmt == "hive":
        return {"namespace": "hive", "name": loc}
    if fmt == "unity":
        host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        host = host.split("://", 1)[-1]
        return {"namespace": f"databricks://{host}" if host else "databricks", "name": loc}
    if loc.startswith("jdbc:"):
        url = _without_credentials(loc[len("jdbc:") :])
        parts = urlsplit(url)
        return {
            "namespace": f"{parts.scheme}://{parts.netloc}",
            "name": parts.path.lstrip("/").replace("/", "."),
        }
    for scheme in ("s3a://", "s3n://", "s3://", "gs://", "abfss://", "wasbs://", "hdfs://"):
        if loc.startswith(scheme):
            bucket, _, key = loc[len(scheme) :].partition("/")
            canonical = "s3://" if scheme.startswith("s3") else scheme
            return {"namespace": f"{canonical}{bucket}", "name": key or "/"}
    if loc.startswith(("http://", "https://")):
        parts = urlsplit(_without_credentials(loc))
        return {"namespace": f"{parts.scheme}://{parts.netloc}", "name": parts.path or "/"}
    if loc.startswith("dbfs:"):
        return {"namespace": "dbfs", "name": loc[len("dbfs:") :]}
    if loc.startswith("file://"):
        loc = loc[len("file://") :]
        if _DRIVE.match(loc.lstrip("/")):
            loc = loc.lstrip("/")
    if loc.startswith("/") or _DRIVE.match(loc):
        return {"namespace": "file", "name": Path(loc).as_posix()}
    if loc and "/" not in loc and "." in loc:
        return {"namespace": fmt or "table", "name": loc}  # a catalogued table
    return {"namespace": "file", "name": Path(loc).resolve().as_posix() if loc else fmt}


# --- events ----------------------------------------------------------------------


def _facet(prod: str, schema_url: str, **fields: Any) -> Dict[str, Any]:
    return {"_producer": prod, "_schemaURL": schema_url, **fields}


def _hash_facet(prod: str, step: StepRecord) -> Dict[str, Any]:
    return _facet(
        prod,
        HASH_SCHEMA,
        format=step.format,
        location=step.location,
        rowCount=step.row_count,
        schemaHash=step.schema_hash,
        dataHash=step.data_hash,
        hashMethod=step.hash_method,
        hashError=step.hash_error,
    )


def _inputs(prod: str, ctx: RunContext, final: bool) -> List[Dict[str, Any]]:
    out = []
    for step in ctx.inputs:
        ds: Dict[str, Any] = dict(dataset_id(step))
        if final:
            ds["facets"] = {"ubunye_hash": _hash_facet(prod, step)}
            if step.row_count is not None:
                ds["inputFacets"] = {
                    "dataQualityMetrics": _facet(
                        prod,
                        f"{FACETS}/1-0-1/DataQualityMetricsInputDatasetFacet.json"
                        "#/$defs/DataQualityMetricsInputDatasetFacet",
                        rowCount=step.row_count,
                        columnMetrics={},
                    )
                }
        out.append(ds)
    return out


def _outputs(prod: str, ctx: RunContext, final: bool) -> List[Dict[str, Any]]:
    out = []
    for step in ctx.outputs:
        ds: Dict[str, Any] = dict(dataset_id(step))
        if final:
            facets: Dict[str, Any] = {"ubunye_hash": _hash_facet(prod, step)}
            checks = [e for e in ctx.expectations if e.get("output") == step.name]
            if checks:
                assertions = []
                for e in checks:
                    item: Dict[str, Any] = {"assertion": e["rule"], "success": bool(e["passed"])}
                    if e.get("column"):
                        item["column"] = e["column"]
                    assertions.append(item)
                facets["dataQualityAssertions"] = _facet(
                    prod,
                    f"{FACETS}/1-0-1/DataQualityAssertionsDatasetFacet.json"
                    "#/$defs/DataQualityAssertionsDatasetFacet",
                    assertions=assertions,
                )
            ds["facets"] = facets
            if step.row_count is not None:
                ds["outputFacets"] = {
                    "outputStatistics": _facet(
                        prod,
                        f"{FACETS}/1-0-1/OutputStatisticsOutputDatasetFacet.json"
                        "#/$defs/OutputStatisticsOutputDatasetFacet",
                        rowCount=step.row_count,
                    )
                }
        out.append(ds)
    return out


def events(ctx: RunContext, *, namespace: Optional[str] = None) -> List[Dict[str, Any]]:
    """The events for one run: START, and COMPLETE or FAIL if it has ended."""
    found = [run_event(ctx, "START", namespace=namespace)]
    if ctx.status in ("success", "error"):
        found.append(
            run_event(ctx, "COMPLETE" if ctx.status == "success" else "FAIL", namespace=namespace)
        )
    return found


def run_event(
    ctx: RunContext, event_type: str, *, namespace: Optional[str] = None
) -> Dict[str, Any]:
    """One OpenLineage RunEvent for a run record."""
    prod = producer(ctx.engine_version)
    final = event_type != "START"
    evidence: Dict[str, Any] = {
        "recordVersion": ctx.record_version,
        "backend": ctx.backend,
        "configHash": ctx.config_hash,
        "codeHash": ctx.code_hash,
        "environmentHash": ctx.environment_hash,
        "environment": ctx.environment,
        "variables": ctx.variables,
        "profile": ctx.profile,
        "model": ctx.model,
        "version": ctx.version,
    }
    if final:
        evidence.update(
            durationSec=ctx.duration_sec,
            timings=ctx.timings,
            expectations=ctx.expectations,
            error=ctx.error,
        )
    run_facets: Dict[str, Any] = {"ubunye_evidence": _facet(prod, EVIDENCE_SCHEMA, **evidence)}
    if final and ctx.status == "error" and ctx.error:
        run_facets["errorMessage"] = _facet(
            prod,
            f"{FACETS}/1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet",
            message=ctx.error,
            programmingLanguage="python",
        )
    return {
        "eventType": event_type,
        "eventTime": (ctx.ended_at if final and ctx.ended_at else ctx.started_at),
        "producer": prod,
        "schemaURL": SCHEMA_URL,
        "run": {"runId": ctx.run_id, "facets": run_facets},
        "job": {
            "namespace": namespace or os.environ.get("OPENLINEAGE_NAMESPACE") or "ubunye",
            "name": ctx.task_path.replace("/", "."),
        },
        "inputs": _inputs(prod, ctx, final),
        "outputs": _outputs(prod, ctx, final),
    }


# --- sending ------------------------------------------------------------------------


@dataclass
class Emitter:
    """Sends events where the environment says; failures are logged, never raised."""

    url: Optional[str] = None
    endpoint: str = "api/v1/lineage"
    api_key: Optional[str] = None
    file: Optional[str] = None
    namespace: Optional[str] = None
    timeout: float = 5.0

    @classmethod
    def from_env(cls) -> Optional["Emitter"]:
        if os.environ.get("OPENLINEAGE_DISABLED", "").lower() in TRUE:
            return None
        url = os.environ.get("OPENLINEAGE_URL")
        file = os.environ.get("UBUNYE_OPENLINEAGE_FILE")
        if not url and not file:
            return None
        return cls(
            url=url,
            endpoint=os.environ.get("OPENLINEAGE_ENDPOINT", "api/v1/lineage"),
            api_key=os.environ.get("OPENLINEAGE_API_KEY"),
            file=file,
            namespace=os.environ.get("OPENLINEAGE_NAMESPACE"),
        )

    def emit(self, ctx: RunContext, event_type: str) -> None:
        try:
            event = run_event(ctx, event_type, namespace=self.namespace)
        except Exception as exc:  # a malformed record must not stop the run
            log.warning("OpenLineage: could not build the %s event: %s", event_type, exc)
            return
        self.send(event)

    def send(self, event: Dict[str, Any]) -> None:
        body = json.dumps(event, default=str)
        if self.file:
            try:
                with open(self.file, "a", encoding="utf-8") as fh:
                    fh.write(body + "\n")
            except OSError as exc:
                log.warning("OpenLineage: could not append to %s: %s", self.file, exc)
        if self.url:
            target = self.url.rstrip("/") + "/" + self.endpoint.lstrip("/")
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            request = urllib.request.Request(
                target, data=body.encode("utf-8"), headers=headers, method="POST"
            )
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as answer:
                    answer.read()
            except Exception as exc:  # the server's problem, not the run's
                log.warning(
                    "OpenLineage: %s refused the %s event: %s", target, event["eventType"], exc
                )
