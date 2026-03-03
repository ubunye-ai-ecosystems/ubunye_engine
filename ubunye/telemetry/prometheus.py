"""
Prometheus metrics for Ubunye (optional).

- Uses `prometheus_client` if installed; otherwise functions no-op.
- Exposes counters and histograms for task runs and step durations.
- Provides helpers to record rows/bytes and errors per step.

Recommended usage:
    from ubunye.telemetry.prometheus import start_http_server, observe_step

    start_http_server(8000)  # once per process (optional)
    observe_step(
        step="Reader:hive",
        task="fraud/claims/claim_etl",
        profile="prod",
        status="success",
        duration_sec=1.23,
        rows=250_000
    )
"""

from __future__ import annotations

from typing import Optional

_PROM_ENABLED = False
try:
    from prometheus_client import Counter, Histogram
    from prometheus_client import start_http_server as _prom_start

    _PROM_ENABLED = True
except Exception:  # pragma: no cover
    Counter = Histogram = object  # type: ignore


if _PROM_ENABLED:
    UBUNYE_TASK_RUNS = Counter(
        "ubunye_task_runs_total",
        "Total Ubunye task runs",
        ["task", "profile", "status"],
    )
    UBUNYE_STEP_DURATION = Histogram(
        "ubunye_step_duration_seconds",
        "Execution time for Ubunye steps",
        ["task", "profile", "step", "status"],
        buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300),
    )
    UBUNYE_ROWS = Counter(
        "ubunye_rows_total",
        "Rows processed per step",
        ["task", "profile", "step"],
    )
    UBUNYE_BYTES = Counter(
        "ubunye_bytes_total",
        "Bytes processed per step",
        ["task", "profile", "step"],
    )
    UBUNYE_ERRORS = Counter(
        "ubunye_errors_total",
        "Errors by task/step",
        ["task", "profile", "step"],
    )
else:
    UBUNYE_TASK_RUNS = UBUNYE_STEP_DURATION = UBUNYE_ROWS = UBUNYE_BYTES = UBUNYE_ERRORS = None  # type: ignore


def start_prometheus_http_server(port: int = 8000) -> None:
    """
    Start a Prometheus HTTP metrics endpoint (default :8000).
    Safe to call multiple times; no-ops if prometheus_client is unavailable.
    """
    if _PROM_ENABLED:
        try:
            _prom_start(port)
        except Exception:
            # Best effort only; avoid crashing the job for metrics endpoint issues.
            pass


def observe_task(task: str, profile: str, status: str) -> None:
    """Increment the task runs counter with status label."""
    if _PROM_ENABLED and UBUNYE_TASK_RUNS is not None:
        UBUNYE_TASK_RUNS.labels(task=task, profile=profile, status=status).inc()


def observe_step(
    *,
    task: str,
    profile: str,
    step: str,
    status: str,
    duration_sec: float,
    rows: Optional[int] = None,
    bytes_: Optional[int] = None,
) -> None:
    """
    Record a single step’s timing and optional throughput metrics.

    Parameters
    ----------
    task : str
        Task coordinates (e.g., "fraud_detection/claims/claim_etl")
    profile : str
        Active profile (dev/prod/staging)
    step : str
        Step name ("Reader:hive", "Transform:dedupe", "Writer:s3")
    status : str
        "success" or "error"
    duration_sec : float
        Step latency in seconds
    rows : int, optional
        Rows processed (if known)
    bytes_ : int, optional
        Bytes processed (if known)
    """
    if not _PROM_ENABLED:
        return
    try:
        UBUNYE_STEP_DURATION.labels(task=task, profile=profile, step=step, status=status).observe(duration_sec)  # type: ignore
        if rows is not None:
            UBUNYE_ROWS.labels(task=task, profile=profile, step=step).inc(rows)  # type: ignore
        if bytes_ is not None:
            UBUNYE_BYTES.labels(task=task, profile=profile, step=step).inc(bytes_)  # type: ignore
        if status != "success":
            UBUNYE_ERRORS.labels(task=task, profile=profile, step=step).inc()  # type: ignore
    except Exception:
        # Never let metrics crash the job
        pass
