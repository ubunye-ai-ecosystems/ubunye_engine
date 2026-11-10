# Hooks: Observing Task Runs

For anyone who wants to log, time, alert on or audit a task run without
changing the task or the engine.

- **The problem:** you want to know when a task starts, ends or fails, and how
  long each step took, but you don't want that code inside every task.
- **When to use it:** logging, metrics, tracing, alerts, audit records, drift
  checks: anything that watches a run but does not change it.
- **The one command:** `run_task("pipelines/shop/orders/clean", hooks=[MyHook()])`
  from Python, or ship the hook as a package and set `UBUNYE_TELEMETRY=1` so
  `ubunye run` loads it.

Every task, and every step in it (each read, the transform, each write), is
wrapped in a hook. Your hook sees the start, the end, and any error that escaped.

---

## The contract

```python
from contextlib import contextmanager
from ubunye.core.hooks import Hook

class MyHook(Hook):
    @contextmanager
    def task(self, ctx, cfg, state):
        # before the task
        try:
            yield
            # task succeeded
        except Exception:
            # task failed: re-raise or you'll swallow the error
            raise

    @contextmanager
    def step(self, ctx, name, meta):
        # before each Reader/Transform/Writer step
        try:
            yield
            # step succeeded
        except Exception:
            raise
```

Both methods are optional: inherit from `Hook` and only override what you need.

### What your hook sees

| Arg | Type | Notes |
|---|---|---|
| `ctx` | `EngineContext` | `run_id`, `task_name`, `profile` |
| `cfg` | `dict` | Full resolved task config |
| `state` | `dict` | Shared scratchpad. On task exit, `state["outputs"]` holds the outputs map (success only). |
| `name` | `str` | Step name, e.g. `"Reader:s3"`, `"Transform:_ubunye_user_task"` (your `transformations.py`), `"Writer:s3"` |
| `meta` | `dict \| None` | Step-specific metadata, e.g. `{"input": "claims_raw"}` |

### Rules

1. **Never raise in `__exit__`.** Wrap all your hook code in `try/except`.
   The engine does not swallow hook errors; a crashing hook crashes the task.
2. **Always re-raise step failures.** If you catch an exception in `step()`,
   re-raise it so the engine can fail fast.
3. **Hooks must be cheap.** They run inline with every step.

---

## Registering hooks

There are two ways in. Pick by how you run the task.

| You run the task with | Use | Run record written? |
|---|---|---|
| Python: `ubunye.run_task(...)` | `hooks=[...]` | No, today (see the warning) |
| The CLI: `ubunye run ...` | A package that registers the hook, plus `UBUNYE_TELEMETRY=1` | Yes, with `--lineage` |

### From Python: `run_task(hooks=[...])`

```python
from ubunye import run_task

outputs = run_task(
    "pipelines/shop/orders/clean",
    backend="pandas",
    hooks=[MyHook(), AnotherHook()],
)
```

`hooks=` **adds** your hooks to the default set; it never replaces it. The
`CONFIG.monitors` hook and the run record (`lineage=True`) still run. The same
goes for `run_pipeline` and `notebook`.

### From the CLI: ship the hook as a package

Register the hook class under the `ubunye.hooks` entry point group in your
package (an entry point is a name in `pyproject.toml` that tells Python where
to find your class; see [Writing a Plugin](../connectors/plugin_guide.md)):

```toml
# your-package/pyproject.toml
[project.entry-points."ubunye.hooks"]
slack = "your_package.hooks:SlackAlertHook"
```

`pip install your-package`, then turn hook loading on and run as usual:

```bash
export UBUNYE_TELEMETRY=1
ubunye run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl --lineage
```

- Entry point hooks load **only** when `UBUNYE_TELEMETRY` is set (to `1`, or
  any value other than `0`, `false`, `no`, `off` or empty). Without it they are
  ignored. No engine or config changes are needed.
- Each class is built with no arguments, so read settings from environment
  variables (as `SlackAlertHook` below does with `SLACK_WEBHOOK_URL`). A hook
  that needs arguments goes through `run_task(hooks=[...])`.
- These hooks are added to the default set, so `CONFIG.monitors` still runs and
  `--lineage` still writes the run record.
- The built-in Prometheus hook reads `UBUNYE_PROM_PORT` (for example `8000`) to
  serve its metrics.

??? note "Under the hood: the `Engine` class"
    `run_task` builds an `Engine` from `ubunye.core.runtime`. `Engine(hooks=[...])`
    replaces the default set; `Engine(extra_hooks=[...])` adds to it. `Engine.run(cfg)`
    takes an already rendered config dict and runs config transforms, not your
    `transformations.py`, so it is not the way to run a task. Use `run_task`.

---

## Example: Slack alert on failure

```python
import os
from contextlib import contextmanager

import requests

from ubunye.core.hooks import Hook


class SlackAlertHook(Hook):
    def __init__(self, webhook_url: str | None = None):
        self.url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")

    @contextmanager
    def task(self, ctx, cfg, state):
        try:
            yield
        except Exception as e:
            self._post(f":red_circle: `{ctx.task_name}` failed: `{e!r}`")
            raise

    def _post(self, text: str) -> None:
        if not self.url:
            return
        try:
            requests.post(self.url, json={"text": text}, timeout=5)
        except Exception:
            pass
```

Wire it in:

```python
run_task("pipelines/shop/orders/clean", hooks=[SlackAlertHook()])
```

Or register it as an entry point (above) so every `ubunye run` uses it.

---

## Example: Row-count audit

```python
from contextlib import contextmanager
from ubunye.core.hooks import Hook


class RowCountHook(Hook):
    def __init__(self):
        self.counts: dict[str, int] = {}

    @contextmanager
    def task(self, ctx, cfg, state):
        yield
        outputs = state.get("outputs") or {}
        for name, df in outputs.items():
            try:
                self.counts[name] = df.count()
            except Exception:
                pass
        print(f"[{ctx.task_name}] row counts: {self.counts}")
```

The `state["outputs"]` dict is populated **only on success**, before the hook's
`__exit__` runs.

---

## Example: Per-step timing

```python
import time
from contextlib import contextmanager
from ubunye.core.hooks import Hook


class TimingHook(Hook):
    @contextmanager
    def step(self, ctx, name, meta):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            dur = time.perf_counter() - t0
            print(f"[{ctx.run_id[:8]}] {name} took {dur:.2f}s")
```

Run it on a task that reads one CSV and writes one Parquet file:

```python
from ubunye import run_task

run_task("shop/orders/clean", backend="pandas", hooks=[TimingHook()])
```

What success looks like:

```
[018d570d] Reader:s3 took 0.26s
[018d570d] Transform:_ubunye_user_task took 0.00s
[018d570d] Writer:s3 took 0.01s
```

`Transform:_ubunye_user_task` is your task's `transformations.py`.

---

## Composing hooks

`HookChain` combines multiple hooks. The engine uses it internally, but you can
use it yourself for complex setups:

```python
from ubunye.core.hooks import HookChain
from ubunye.telemetry.hooks import EventLoggerHook, PrometheusHook

chain = HookChain([EventLoggerHook(), PrometheusHook(), MyHook()])
```

- **Enter order**: as listed.
- **Exit order**: reverse (last in, first out), so the outermost hook sees inner hooks' work.
- **Broken hooks**: if one raises on `__enter__`, the others still run.

---

## When NOT to use a hook

Hooks observe. They do **not**:

- Mutate the config or outputs mid-run.
- Replace a Reader/Transform/Writer. Write a plugin for that.
- Decide whether a step runs. That belongs in config.

If you need to change pipeline behavior, write a plugin
([Connectors → Writing a Plugin](../connectors/plugin_guide.md)) or a custom
`Transform`. Hooks are strictly for observability.

---

## Source reference

??? note "Under the hood: where the code lives"
    | File | What it contains |
    |---|---|
    | `ubunye/core/hooks.py` | `Hook` base class, `HookChain` multiplexer |
    | `ubunye/telemetry/hooks/events.py` | JSON event logger hook |
    | `ubunye/telemetry/hooks/otel.py` | OpenTelemetry spans and metrics ([OpenTelemetry](opentelemetry.md)) |
    | `ubunye/telemetry/hooks/prometheus.py` | Prometheus metrics |
    | `ubunye/telemetry/hooks/monitors.py` | Legacy `CONFIG.monitors` bridge |
    | `tests/unit/test_hooks.py` | Reference tests: recording hook, failure propagation, dry-run |
