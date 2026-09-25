# OpenTelemetry

Ubunye sends traces and metrics to any OpenTelemetry collector (Grafana,
Honeycomb, Datadog, New Relic, Google Cloud, AWS X-Ray through ADOT, a
self-hosted OTel Collector), configured by OpenTelemetry's own variables.

```bash
pip install 'ubunye-engine[otel]'
export UBUNYE_TELEMETRY=1
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_SERVICE_NAME=shop-pipelines
ubunye run -d pipelines -u shop -p orders -t clean
```

`UBUNYE_TELEMETRY=1` turns on the telemetry hooks (it is read when a run starts,
so setting it in a notebook works). Or pass the hook yourself:
`Engine(extra_hooks=[OTelHook()])`, `run_task(..., hooks=[OTelHook()])`.

## Configuration

The standard variables; nothing Ubunye specific.

| Variable | Meaning |
|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | the collector (`..._TRACES_ENDPOINT` / `..._METRICS_ENDPOINT` win for one signal) |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` (default) or `grpc` |
| `OTEL_EXPORTER_OTLP_HEADERS` | e.g. `x-honeycomb-team=...` |
| `OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER` | `otlp`, `console` or `none` |
| `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | the resource |

With no endpoint and no exporter named, nothing is sent and nothing is printed.
If your application already set up OpenTelemetry, Ubunye uses its providers.
Everything buffered is flushed when a task ends, so a short CLI run still exports.

## Traces

One span per task (`ubunye.task <usecase/package/task>`) with a child per read,
transform and write. Attributes: `ubunye.run_id` (the same id as `ubunye
lineage`), `ubunye.backend`, `ubunye.profile`, `ubunye.config_hash`,
`ubunye.input` / `ubunye.output`. A failure is recorded on the span as an
exception and marks it as an error.

## Metrics

| Metric | Type | Attributes |
|---|---|---|
| `ubunye.task.runs` | counter | task, backend, status |
| `ubunye.task.duration` | histogram, s | task, backend, status |
| `ubunye.step.duration` | histogram, s | task, step, status |
| `ubunye.rows.read` | counter | task, dataset |
| `ubunye.rows.written` | counter | task, dataset |

Rows are counted where it is free: on a single-machine backend (pandas) the
frame is already in memory. On Spark a count is a job over the data, so rows are
counted only with `UBUNYE_OTEL_COUNT_ROWS=1`; the run record's content hash
(`--lineage`) counts every row anyway.
