"""OpenTelemetry for Ubunye: traces and metrics, configured the standard way.

Needs ``opentelemetry-sdk`` (``pip install 'ubunye-engine[otel]'``); without it
every function here does nothing. Configuration is OpenTelemetry's own
environment variables, so the engine fits whatever collector a platform runs:

``OTEL_EXPORTER_OTLP_ENDPOINT``            where to send (``..._TRACES_`` / ``..._METRICS_``
                                          variants win for one signal)
``OTEL_EXPORTER_OTLP_PROTOCOL``            ``http/protobuf`` (default) or ``grpc``
``OTEL_EXPORTER_OTLP_HEADERS``             e.g. an API key, read by the exporter
``OTEL_TRACES_EXPORTER``, ``OTEL_METRICS_EXPORTER``  ``otlp``, ``console`` or ``none``
``OTEL_SERVICE_NAME``, ``OTEL_RESOURCE_ATTRIBUTES``   the resource

With no endpoint and no exporter named, nothing is sent and nothing is printed.
If the host application already set up OpenTelemetry, its providers are used.

Traces: a span per task and per read, transform and write, with the run id,
backend and config hash as attributes; a failure is recorded on the span and
marks it as an error. Metrics:

====================================  ==========  ===============================
``ubunye.task.runs``                  counter     task, backend, status
``ubunye.task.duration``              histogram   task, backend, status (seconds)
``ubunye.step.duration``              histogram   task, step, status (seconds)
``ubunye.rows.read``                  counter     task, dataset
``ubunye.rows.written``               counter     task, dataset
====================================  ==========  ===============================

Everything is flushed when a task ends, so a short CLI run still exports.
"""

from __future__ import annotations

import contextlib
import logging
import os
from typing import Any, Dict, Iterator, List, Optional

log = logging.getLogger(__name__)

_state: Dict[str, Any] = {"ready": False, "tracer": None, "instruments": None, "owned": []}


def _exporter_name(signal: str) -> str:
    named = os.environ.get(f"OTEL_{signal.upper()}_EXPORTER")
    if named:
        return named.split(",")[0].strip().lower()
    endpoint = os.environ.get(f"OTEL_EXPORTER_OTLP_{signal.upper()}_ENDPOINT") or os.environ.get(
        "OTEL_EXPORTER_OTLP_ENDPOINT"
    )
    return "otlp" if endpoint else "none"


def _protocol(signal: str) -> str:
    return (
        os.environ.get(f"OTEL_EXPORTER_OTLP_{signal.upper()}_PROTOCOL")
        or os.environ.get("OTEL_EXPORTER_OTLP_PROTOCOL")
        or "http/protobuf"
    )


def _span_exporter() -> Any:
    name = _exporter_name("traces")
    if name == "console":
        from opentelemetry.sdk.trace.export import ConsoleSpanExporter

        return ConsoleSpanExporter()
    if name != "otlp":
        return None
    try:
        if _protocol("traces") == "grpc":
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        else:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    except ImportError:
        log.warning(
            "OTLP export needs opentelemetry-exporter-otlp: pip install 'ubunye-engine[otel]'"
        )
        return None
    return OTLPSpanExporter()


def _metric_exporter() -> Any:
    name = _exporter_name("metrics")
    if name == "console":
        from opentelemetry.sdk.metrics.export import ConsoleMetricExporter

        return ConsoleMetricExporter()
    if name != "otlp":
        return None
    try:
        if _protocol("metrics") == "grpc":
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        else:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    except ImportError:
        log.warning(
            "OTLP export needs opentelemetry-exporter-otlp: pip install 'ubunye-engine[otel]'"
        )
        return None
    return OTLPMetricExporter()


def _is_proxy(provider: Any) -> bool:
    return type(provider).__name__.startswith(("Proxy", "NoOp", "_Proxy"))


def setup(
    service_name: Optional[str] = None,
    *,
    tracer_provider: Any = None,
    meter_provider: Any = None,
) -> bool:
    """Make the tracer and the instruments; safe to call more than once.

    ``tracer_provider`` / ``meter_provider`` are for callers (and tests) that bring
    their own; otherwise the host's global providers are used if it set any, and
    else providers are built from the ``OTEL_*`` environment variables.
    """
    if _state["ready"] and tracer_provider is None and meter_provider is None:
        return _state["tracer"] is not None
    try:
        from opentelemetry import metrics, trace
        from opentelemetry.sdk.resources import Resource
    except ImportError:
        return False

    name = os.environ.get("OTEL_SERVICE_NAME") or service_name or "ubunye"
    resource = Resource.create({"service.name": name})
    owned: List[Any] = []

    if tracer_provider is None:
        tracer_provider = trace.get_tracer_provider()
        if _is_proxy(tracer_provider):
            exporter = _span_exporter()
            if exporter is not None:
                from opentelemetry.sdk.trace import TracerProvider
                from opentelemetry.sdk.trace.export import BatchSpanProcessor

                tracer_provider = TracerProvider(resource=resource)
                tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
                trace.set_tracer_provider(tracer_provider)
                owned.append(tracer_provider)
    if meter_provider is None:
        meter_provider = metrics.get_meter_provider()
        if _is_proxy(meter_provider):
            exporter = _metric_exporter()
            if exporter is not None:
                from opentelemetry.sdk.metrics import MeterProvider
                from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

                reader = PeriodicExportingMetricReader(exporter)
                meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
                metrics.set_meter_provider(meter_provider)
                owned.append(meter_provider)

    from ubunye import __version__ as version

    tracer = tracer_provider.get_tracer("ubunye", version)
    meter = meter_provider.get_meter("ubunye", version)
    _state.update(
        ready=True,
        tracer=tracer,
        owned=owned or [tracer_provider, meter_provider],
        instruments={
            "runs": meter.create_counter("ubunye.task.runs", unit="{run}", description="Task runs"),
            "task_s": meter.create_histogram(
                "ubunye.task.duration", unit="s", description="Task duration"
            ),
            "step_s": meter.create_histogram(
                "ubunye.step.duration", unit="s", description="Read, transform and write duration"
            ),
            "read": meter.create_counter("ubunye.rows.read", unit="{row}", description="Rows read"),
            "written": meter.create_counter(
                "ubunye.rows.written", unit="{row}", description="Rows written"
            ),
        },
    )
    return True


def reset() -> None:
    """Forget the tracer and instruments (tests)."""
    _state.update(ready=False, tracer=None, instruments=None, owned=[])


def _clean(attrs: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in (attrs or {}).items():
        if value is None:
            continue
        out[key] = value if isinstance(value, (str, bool, int, float)) else str(value)
    return out


@contextlib.contextmanager
def span(name: str, attrs: Optional[Dict[str, Any]] = None) -> Iterator[Any]:
    """A span around the block; an exception is recorded and marks it as an error."""
    tracer = _state["tracer"]
    if tracer is None:
        yield None
        return
    from opentelemetry.trace import Status, StatusCode

    with tracer.start_as_current_span(
        name, attributes=_clean(attrs), record_exception=False, set_status_on_exception=False
    ) as current:
        try:
            yield current
        except BaseException as exc:
            current.record_exception(exc)
            current.set_status(Status(StatusCode.ERROR, f"{type(exc).__name__}: {exc}"[:500]))
            raise


def record(instrument: str, value: float, attrs: Dict[str, Any]) -> None:
    """Add to a counter or a histogram; nothing without the SDK."""
    instruments = _state["instruments"]
    if not instruments:
        return
    metric = instruments[instrument]
    try:
        if hasattr(metric, "add"):
            metric.add(value, _clean(attrs))
        else:
            metric.record(value, _clean(attrs))
    except Exception:  # a metrics error must never fail a run
        log.debug("otel: could not record %s", instrument, exc_info=True)


def flush(timeout_millis: int = 5000) -> None:
    """Export what is buffered now; a short CLI run ends before the next batch."""
    for provider in _state["owned"]:
        force = getattr(provider, "force_flush", None)
        if callable(force):
            try:
                force(timeout_millis)
            except Exception:
                log.debug("otel: flush failed", exc_info=True)


def init_tracer(service_name: str = "ubunye") -> None:
    """Kept for code written against 0.5 and earlier; same as :func:`setup`."""
    setup(service_name)


def get_tracer() -> Any:
    """The tracer, or None without the SDK."""
    return _state["tracer"]
