# Execution backends

A backend is the engine that runs your task. The task folder never names one,
so the same folder runs on any backend that can do what it asks.

## See what you have

```bash
ubunye backends
```

```text
databricks
  features:     catalog, partitioned_writes, path_io, remote_paths, spark
  file formats: any
  write modes:  any
  distributed:  yes   needs Java: yes
pandas
  features:     path_io
  file formats: csv, json, parquet
  write modes:  append, errorifexists, ignore, overwrite
  distributed:  no   needs Java: no
spark (default)
  features:     catalog, partitioned_writes, path_io, remote_paths, spark
  ...
```

Add `--json` for a machine readable list.

## Choose one

| Where | How |
|---|---|
| CLI | `ubunye run ... --backend pandas` (also `test run` and `validate`) |
| Python | `ubunye.run_task(task_dir, backend="pandas")` (also `run_pipeline` and `notebook`) |

With no choice, Ubunye uses the platform's session if there is one (on
Databricks, the notebook's), and otherwise starts Spark.

## Check before you run

```bash
ubunye validate -d pipelines -u sales -p etl -t daily --backend pandas
```

This checks every input and output against what the backend can do, without
starting anything, and lists every problem at once:

```text
  [FAIL] daily (on the pandas backend)
         - input 'orders' uses the 'hive' connector, which needs spark; the pandas backend does not provide it.
         - output 'report' uses mode 'merge'; the pandas backend can do append, errorifexists, ignore, overwrite.
```

`ubunye run` makes the same check before it starts, so a task that cannot run
stops in the first second, not halfway through.

## The shipped backends

| Backend | Use it for | Install |
|---|---|---|
| `spark` | Local Spark, Kubernetes, EMR, Dataproc, spark-submit | `pip install 'ubunye-engine[spark]'` |
| `databricks` | Attaching to a session that already exists (chosen for you on Databricks) | included in the runtime |
| `pandas` | Small data on a laptop, CI, teaching: no Spark and no Java | `pip install 'ubunye-engine[pandas]'` |

The pandas backend reads and writes exactly as Spark does: see
[Anywhere with spark-submit](deployment/anywhere.md#no-spark-at-all-the-pandas-backend).

## Write your own

A backend is a subclass of `ubunye.core.interfaces.Backend` registered as an
entry point. It says what it can do, and the engine does the rest.

```python
from ubunye.core.capabilities import PATH_IO, Capabilities
from ubunye.core.interfaces import Backend


class MyBackend(Backend):
    name = "mine"
    CAPABILITIES = Capabilities(
        features=frozenset({PATH_IO}),
        file_formats=frozenset({"parquet"}),
        write_modes=frozenset({"append", "overwrite"}),
    )

    def __init__(self, app_name="ubunye", conf=None):
        self.conf = dict(conf or {})

    def start(self): ...
    def stop(self): ...
    def read_frame(self, file_format, path, *, options=None, schema=None): ...
    def execute_write(self, df, resolved, *, connector, file_format, **kw): ...
```

```toml
[project.entry-points."ubunye.backends"]
mine = "my_package.backend:MyBackend"
```

After `pip install`, `ubunye backends` lists it and `--backend mine` runs on it.
If your constructor takes other arguments, override the `create` class method.
If your engine should claim a session the platform already started, override
`from_platform`. See the [architecture decisions](architecture/index.md) for why
it works this way.
