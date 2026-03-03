"""
OpenTelemetry helpers for Ubunye.

- Optional dependency: if `opentelemetry-sdk` is not installed, functions no-op.
- Provides `get_tracer()` and a `span()` context manager to time steps.
- Adds attributes like task name, step type, and run_id to spans.

Usage:
    from ubunye.telemetry.otel import span, init_tracer

    init_tracer(service_name="ubunye")   # safe if called multiple times
    with span("Reader:hive", attrs={"task": "fraud/claims/claim_etl", "run_id": rid}):
        df = reader.read(cfg, backend)
"""
from __future__ import annotations

import contextlib
from typing import Dict, Optional

_TRACING_ENABLED = False
_tracer = None


def init_tracer(service_name: str = "ubunye") -> None:
    """
    Initialize OpenTelemetry tracing if the SDK is available.

    Parameters
    ----------
    service_name : str
        Logical service name for traces (shown in your tracing backend).
    """
    global _TRACING_ENABLED, _tracer
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

        if trace.get_tracer_provider().__class__.__name__ != "ProxyTracerProvider":
            # Already configured by host application; reuse it.
            _TRACING_ENABLED = True
            _tracer = trace.get_tracer(service_name)
            return

        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)
        # Default to console exporter; users can replace/augment outside
        processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(processor)

        trace.set_tracer_provider(provider)
        _tracer = trace.get_tracer(service_name)
        _TRACING_ENABLED = True
    except Exception:
        # Missing SDK or configuration error — keep tracing disabled.
        _TRACING_ENABLED = False
        _tracer = None


def get_tracer():
    """Return the OTel tracer if initialized; otherwise None."""
    return _tracer


@contextlib.contextmanager
def span(name: str, attrs: Optional[Dict[str, object]] = None):
    """
    Context manager that creates an OpenTelemetry span if tracing is enabled.

    Parameters
    ----------
    name : str
        Span name (e.g., "Reader:hive", "Transform:dedupe", "Writer:s3").
    attrs : dict
        Optional attributes to set on the span (task, run_id, profile, step, …).
    """
    if not _TRACING_ENABLED or _tracer is None:
        yield
        return

    from opentelemetry import trace

    current_span = None
    try:
        current_span = _tracer.start_span(name)
        if attrs:
            for k, v in attrs.items():
                try:
                    current_span.set_attribute(k, v)  # attributes must be simple types
                except Exception:
                    pass
        token = trace.use_span(current_span, end_on_exit=True)
        token.__enter__()
        yield
    finally:
        try:
            if current_span is not None:
                # end handled by end_on_exit True, but be defensive
                current_span.end()
        finally:
            try:
                token.__exit__(None, None, None)  # type: ignore
            except Exception:
                pass
