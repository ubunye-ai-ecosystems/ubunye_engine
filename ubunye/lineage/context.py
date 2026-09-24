"""Lineage context dataclasses for run tracking.

``RunContext`` represents one complete task execution; ``StepRecord`` represents
a single input or output within that run. Both serialise to/from plain dicts
(JSON-safe) so they can be stored as ``.json`` files without any external deps.

Usage
-----
    from ubunye.lineage.context import RunContext, StepRecord

    ctx = RunContext(
        run_id="abc123",
        task_path="fraud_detection/ingestion/claim_etl",
        usecase="fraud_detection",
        package="ingestion",
        task_name="claim_etl",
        profile="dev",
        model="etl",
        version="0.1.0",
        config_hash="sha256:...",
        started_at="2025-03-01T10:00:00Z",
    )
    d = ctx.to_dict()
    ctx2 = RunContext.from_dict(d)
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _location_from_io_cfg(io_cfg: Dict[str, Any]) -> str:
    """Derive a human-readable location string from an IOConfig dict."""
    fmt = io_cfg.get("format", "")
    if fmt in ("hive", "unity"):
        db = io_cfg.get("db_name", "")
        tbl = io_cfg.get("tbl_name", "")
        if db and tbl:
            return f"{db}.{tbl}"
        return io_cfg.get("table") or io_cfg.get("sql", "")[:80] or fmt
    if fmt == "jdbc":
        url = io_cfg.get("url", "")
        table = io_cfg.get("table", "")
        return f"{url}/{table}" if table else url
    if fmt in ("s3", "binary", "delta"):
        return io_cfg.get("path", "") or io_cfg.get("table", fmt)
    if fmt == "rest_api":
        return io_cfg.get("url", "")
    return io_cfg.get("path") or io_cfg.get("url") or fmt


# ---------------------------------------------------------------------------
# StepRecord
# ---------------------------------------------------------------------------


#: The run record layout this engine writes (``RunContext.record_version``).
RECORD_VERSION = 2


@dataclass
class StepRecord:
    """Captures lineage metadata for a single input or output step."""

    name: str  # logical name in config, e.g. "source", "sink"
    direction: str  # "input" | "output"
    format: str  # "hive", "s3", "jdbc", ...
    location: str  # human-readable pointer to the data (db.tbl, path, url)
    row_count: Optional[int] = None
    schema_hash: Optional[str] = None  # "sha256:<hex>" of the canonical schema
    data_hash: Optional[str] = None  # "sha256:<hex>" of every row (see hash_method)
    #: How data_hash was computed ("rows-v1": every row, order independent, the
    #: same on every engine). Records from before 0.6.0 have none: their hash was
    #: a sample and is not comparable.
    hash_method: Optional[str] = None
    #: Why there is no data_hash, when the rows could not be read.
    hash_error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "StepRecord":
        return StepRecord(
            name=d["name"],
            direction=d["direction"],
            format=d["format"],
            location=d["location"],
            row_count=d.get("row_count"),
            schema_hash=d.get("schema_hash"),
            data_hash=d.get("data_hash"),
            hash_method=d.get("hash_method"),
            hash_error=d.get("hash_error"),
        )

    @staticmethod
    def from_io_cfg(name: str, direction: str, io_cfg: Dict[str, Any]) -> "StepRecord":
        """Construct a StepRecord from an IOConfig dict (model_dump output)."""
        return StepRecord(
            name=name,
            direction=direction,
            format=io_cfg.get("format", ""),
            location=_location_from_io_cfg(io_cfg),
        )


# ---------------------------------------------------------------------------
# RunContext
# ---------------------------------------------------------------------------


@dataclass
class RunContext:
    """Full lineage record for one task execution."""

    run_id: str
    task_path: str  # "usecase/package/task_name"
    usecase: str
    package: str
    task_name: str
    profile: str
    model: str  # "etl" | "ml"
    version: str
    config_hash: str  # "sha256:<hex>" of the raw config.yaml bytes
    started_at: str  # ISO-8601 UTC timestamp

    # Populated at task_end
    ended_at: Optional[str] = None
    duration_sec: Optional[float] = None
    status: str = "running"  # "running" | "success" | "error"
    inputs: List[StepRecord] = field(default_factory=list)
    outputs: List[StepRecord] = field(default_factory=list)
    error: Optional[str] = None
    #: The Ubunye version that made this record.
    engine_version: str = ""
    #: The backend that ran the task ("spark", "pandas", ...).
    backend: str = ""
    #: The template variables the run was given (dt, dtf, mode, ...).
    variables: Dict[str, Any] = field(default_factory=dict)
    #: 2 since 0.7.0 (code, environment, input hashes, timings, expectations);
    #: a record without it is 1.
    record_version: int = RECORD_VERSION
    #: ``sha256:`` of the task's Python files (ubunye.lineage.evidence.code_hash).
    code_hash: Optional[str] = None
    #: Python, platform and the versions of the packages that can change a result.
    environment: Dict[str, Any] = field(default_factory=dict)
    environment_hash: Optional[str] = None
    #: One entry per read, transform and write: {"step", "input"|"output", "seconds"}.
    timings: List[Dict[str, Any]] = field(default_factory=list)
    #: Every expectation checked, passed or not (ubunye.core.expectations).
    expectations: List[Dict[str, Any]] = field(default_factory=list)
    #: Every language model call the task made (ubunye.llm): backend, model,
    #: tokens, seconds, status and the request's hash; never the prompt or answer.
    llm_calls: List[Dict[str, Any]] = field(default_factory=list)
    #: The run's limits and what was spent (ubunye.llm.budget); empty with no limits.
    llm_budget: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        # asdict already converts nested dataclasses to dicts
        return d

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "RunContext":
        inputs = [StepRecord.from_dict(s) for s in d.get("inputs", [])]
        outputs = [StepRecord.from_dict(s) for s in d.get("outputs", [])]
        return RunContext(
            run_id=d["run_id"],
            task_path=d["task_path"],
            usecase=d["usecase"],
            package=d["package"],
            task_name=d["task_name"],
            profile=d.get("profile", ""),
            model=d.get("model", ""),
            version=d.get("version", ""),
            config_hash=d.get("config_hash", ""),
            started_at=d["started_at"],
            ended_at=d.get("ended_at"),
            duration_sec=d.get("duration_sec"),
            status=d.get("status", "running"),
            inputs=inputs,
            outputs=outputs,
            error=d.get("error"),
            engine_version=d.get("engine_version", ""),
            backend=d.get("backend", ""),
            variables=dict(d.get("variables") or {}),
            record_version=int(d.get("record_version") or 1),
            code_hash=d.get("code_hash"),
            environment=dict(d.get("environment") or {}),
            environment_hash=d.get("environment_hash"),
            timings=list(d.get("timings") or []),
            expectations=list(d.get("expectations") or []),
            llm_calls=list(d.get("llm_calls") or []),
            llm_budget=dict(d.get("llm_budget") or {}),
        )
