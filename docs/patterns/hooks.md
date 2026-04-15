# Hooks: Observing Task Runs

Hooks let you watch a task run without changing Engine code. Every task and
every step (Reader / Transform / Writer) is wrapped in a hook context
manager — so you see the start, the end, and any exception that escaped.

Use hooks to add logging, metrics, tracing, alerts, audit records, drift
checks — anything that observes but does not change the pipeline.

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
            # task failed — re-raise or you'll swallow the error
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

Both methods are optional — inherit from `Hook` and only override what you need.

### What your hook sees

| Arg | Type | Notes |
|---|---|---|
| `ctx` | `EngineContext` | `run_id`, `task_name`, `profile` |
| `cfg` | `dict` | Full resolved task config |
| `state` | `dict` | Shared scratchpad. On task exit, `state["outputs"]` holds the outputs map (success only). |
| `name` | `str` | Step name, e.g. `"Reader:hive"`, `"Transform:noop"`, `"Writer:s3"` |
| `meta` | `dict \| None` | Step-specific metadata, e.g. `{"input": "claims_raw"}` |

### Rules

1. **Never raise in `__exit__`.** Wrap all your hook code in `try/except`.
   The engine does not swallow hook errors; a crashing hook crashes the task.
2. **Always re-raise step failures.** If you catch an exception in `step()`,
   re-raise it so the engine can fail fast.
3. **Hooks must be cheap.** They run inline with every step.

---

## Registering hooks

### Per-engine (explicit)

```python
from ubunye.core.runtime import Engine

engine = Engine(hooks=[MyHook(), AnotherHook()])
engine.run(cfg)
```

Explicit hooks **replace** the default set. Add `LegacyMonitorsHook(cfg)`
yourself if you still want user monitors from `CONFIG.monitors`.

### Environment (default set)

With no `hooks=` argument, the engine loads:

- `EventLoggerHook`, `OTelHook`, `PrometheusHook` — only if `UBUNYE_TELEMETRY=1`
- `LegacyMonitorsHook(cfg)` — always (reads `CONFIG.monitors`)

```bash
export UBUNYE_TELEMETRY=1
export UBUNYE_PROM_PORT=8000
ubunye run -d ./pipelines -u fraud_detection -p ingestion -t claim_etl
```

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
engine = Engine(hooks=[SlackAlertHook()])
engine.run(cfg)
```

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

Output during a run:

```
[5b3f91aa] Reader:hive took 0.42s
[5b3f91aa] Transform:noop took 0.01s
[5b3f91aa] Writer:s3 took 1.37s
```

---

## Composing hooks

`HookChain` combines multiple hooks. Engine uses it internally, but you can
use it yourself for complex setups:

```python
from ubunye.core.hooks import HookChain
from ubunye.telemetry.hooks import EventLoggerHook, PrometheusHook

chain = HookChain([EventLoggerHook(), PrometheusHook(), MyHook()])
```

- **Enter order** — as listed.
- **Exit order** — reverse (LIFO), so outermost hook sees inner hooks' work.
- **Broken hooks** — if one raises on `__enter__`, the others still run.

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

| File | What it contains |
|---|---|
| `ubunye/core/hooks.py` | `Hook` base class, `HookChain` multiplexer |
| `ubunye/telemetry/hooks/events.py` | JSON event logger hook |
| `ubunye/telemetry/hooks/otel.py` | OpenTelemetry spans |
| `ubunye/telemetry/hooks/prometheus.py` | Prometheus metrics |
| `ubunye/telemetry/hooks/monitors.py` | Legacy `CONFIG.monitors` bridge |
| `tests/unit/test_hooks.py` | Reference tests — recording hook, failure propagation, dry-run |
