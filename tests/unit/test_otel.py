"""OpenTelemetry: spans per task and step, the run's metrics, the standard config.

Uses the SDK's in-memory exporter and reader, so what is checked is exactly what
an OTLP collector would receive.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("opentelemetry.sdk")
pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from opentelemetry.sdk.metrics import MeterProvider  # noqa: E402
from opentelemetry.sdk.metrics.export import InMemoryMetricReader  # noqa: E402
from opentelemetry.sdk.trace import TracerProvider  # noqa: E402
from opentelemetry.sdk.trace.export import SimpleSpanProcessor  # noqa: E402
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (  # noqa: E402
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode  # noqa: E402

import ubunye  # noqa: E402
from ubunye.telemetry import otel  # noqa: E402
from ubunye.telemetry.hooks.otel import OTelHook, _cheap_to_count  # noqa: E402


@pytest.fixture
def collector():
    """Fresh providers wired to in-memory sinks, for one test."""
    spans = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
    reader = InMemoryMetricReader()
    meter_provider = MeterProvider(metric_readers=[reader])
    otel.reset()
    otel.setup(tracer_provider=tracer_provider, meter_provider=meter_provider)
    yield spans, reader
    otel.reset()


def _metrics(reader):
    """{metric name: [(attributes, value), ...]}"""
    found = {}
    data = reader.get_metrics_data()
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                for p in m.data.data_points:
                    value = getattr(p, "value", None)
                    if value is None:
                        value = p.count  # a histogram: how many were recorded
                    found.setdefault(m.name, []).append((dict(p.attributes), value))
    return found


TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"out": sources["src"]}
"""


def _task(root: Path, expectations: str = "") -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id,qty\n1,2\n2,0\n3,5\n", encoding="utf-8")
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
CONFIG:
  inputs:
    src: {{format: s3, path: "{(root / 'in.csv').as_posix()}", file_format: csv, options: {{header: "true", inferSchema: "true"}}}}
  outputs:
    out: {{format: s3, path: "{(root / 'out').as_posix()}", file_format: parquet, mode: overwrite}}
{expectations}""",
        encoding="utf-8",
    )
    return task


def test_a_run_is_one_task_span_with_a_child_per_step(collector, tmp_path):
    spans, _ = collector
    ubunye.run_task(str(_task(tmp_path)), backend="pandas", hooks=[OTelHook()])
    finished = spans.get_finished_spans()
    [task] = [s for s in finished if s.name.startswith("ubunye.task")]
    steps = [s for s in finished if s is not task]
    assert [s.name.split(":")[0] for s in steps] == ["Reader", "Transform", "Writer"]
    assert all(s.parent.span_id == task.context.span_id for s in steps)
    assert task.attributes["ubunye.backend"] == "pandas"
    assert task.attributes["ubunye.config_hash"].startswith("sha256:")
    assert steps[0].attributes["ubunye.input"] == "src"
    assert task.status.status_code is not StatusCode.ERROR


def test_a_run_records_runs_durations_and_rows(collector, tmp_path):
    _, reader = collector
    ubunye.run_task(str(_task(tmp_path)), backend="pandas", hooks=[OTelHook()])
    m = _metrics(reader)
    [(attrs, runs)] = m["ubunye.task.runs"]
    assert runs == 1 and attrs["status"] == "success" and attrs["backend"] == "pandas"
    assert m["ubunye.rows.read"] == [({"task": attrs["task"], "dataset": "src"}, 3)]
    assert m["ubunye.rows.written"] == [({"task": attrs["task"], "dataset": "out"}, 3)]
    assert sum(count for _, count in m["ubunye.step.duration"]) == 3
    assert m["ubunye.task.duration"][0][1] == 1


def test_a_failed_run_is_an_error_span_and_an_error_count(collector, tmp_path):
    spans, reader = collector
    task = _task(
        tmp_path,
        "  expectations:\n    out:\n      rules:\n        - between: {column: qty, min: 1}\n",
    )
    with pytest.raises(Exception, match="Expectations failed"):
        ubunye.run_task(str(task), backend="pandas", hooks=[OTelHook()])
    [task_span] = [s for s in spans.get_finished_spans() if s.name.startswith("ubunye.task")]
    assert task_span.status.status_code is StatusCode.ERROR
    assert any(e.name == "exception" for e in task_span.events)
    m = _metrics(reader)
    assert [a["status"] for a, _ in m["ubunye.task.runs"]] == ["error"]
    assert "ubunye.rows.written" not in m  # nothing was written


# --- the standard configuration -------------------------------------------------------


@pytest.mark.parametrize(
    "env, expected",
    [
        ({}, "none"),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318"}, "otlp"),
        ({"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://collector:4318/v1/traces"}, "otlp"),
        ({"OTEL_TRACES_EXPORTER": "console"}, "console"),
        ({"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318", "OTEL_TRACES_EXPORTER": "none"}, "none"),
    ],
)
def test_the_exporter_follows_the_otel_variables(monkeypatch, env, expected):
    for var in (
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_TRACES_EXPORTER",
    ):
        monkeypatch.delenv(var, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert otel._exporter_name("traces") == expected


def test_nothing_is_set_up_to_send_without_an_endpoint(monkeypatch):
    for var in ("OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_TRACES_EXPORTER", "OTEL_METRICS_EXPORTER"):
        monkeypatch.delenv(var, raising=False)
    assert otel._span_exporter() is None
    assert otel._metric_exporter() is None


def test_rows_are_counted_only_where_counting_is_free(monkeypatch):
    monkeypatch.delenv("UBUNYE_OTEL_COUNT_ROWS", raising=False)
    assert _cheap_to_count("pandas") is True
    assert _cheap_to_count("spark") is False  # a count is a job on a cluster
    monkeypatch.setenv("UBUNYE_OTEL_COUNT_ROWS", "1")
    assert _cheap_to_count("spark") is True


def test_the_telemetry_switch_is_read_when_the_run_starts(monkeypatch):
    """It was read at import: setting it in a notebook after `import ubunye` did nothing."""
    from ubunye.core import runtime

    monkeypatch.setenv("UBUNYE_TELEMETRY", "0")
    assert not runtime._telemetry_enabled()
    monkeypatch.setenv("UBUNYE_TELEMETRY", "1")
    assert runtime._telemetry_enabled()
