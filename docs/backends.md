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

## One transform for every engine

The config runs anywhere; the transform runs where its code does. A transform
that calls the Spark API (`F.col`, `groupBy`) needs a Spark backend, and one
written with pandas needs the pandas backend. To run the same task on both,
write the transform once with [Narwhals](https://narwhals-dev.github.io/narwhals/)
(`pip install narwhals`):

```python
import narwhals as nw

from ubunye.core.interfaces import Task


class SurvivalByGroup(Task):
    def transform(self, sources):
        people = nw.from_native(sources["titanic"])  # pandas or Spark, as given
        summary = (
            people.with_columns(
                age_group=nw.when(nw.col("Age") < 18).then(nw.lit("child")).otherwise(nw.lit("adult")),
                # Spark widens every sum to a 64 bit integer; pandas keeps the
                # column's type. Cast first and both give the same type.
                Survived=nw.col("Survived").cast(nw.Int64),
            )
            .group_by("Pclass", "age_group")
            .agg(nw.len().alias("passengers"), nw.col("Survived").sum().alias("survivors"))
            .sort("Pclass", "age_group")
        )
        return {"summary": summary}  # returning the Narwhals frame is fine
```

Run it with `--backend pandas` and on Spark, and the two run records carry the
same data hash: the same rows, values and types (the engine's own tests check
this against Spark). The engine hands your transform its own frames and
unwraps what you return, so writers, hooks and lineage never see Narwhals.

Narwhals gives one API, not one set of engine rules. Two differences are known:

- **A sum of whole numbers** is `bigint` on Spark and keeps the column's type on
  pandas (`int32` for small numbers read from CSV). Cast the column before the
  group by, as above. (Casting inside `agg` works too, but pandas then runs the
  sum the slow way, and Narwhals warns.)
- **Rounding a value exactly halfway**: Spark rounds half up and pandas rounds
  half to even, so `round(2)` makes 0.125 into 0.13 on Spark and 0.12 on pandas.
  Other values round the same. Round for presentation, or where ties cannot
  occur.

The run record's data hash covers every value and type, so comparing the two
runs' hashes (`ubunye lineage compare`) shows whether a transform really gives
the same result on both.

You do not declare which engines a transform supports. `ubunye plan` reads the
imports in `transformations.py` and says what it is written for:

```text
  Transform  SurvivalByGroup  (transformations.py), written for narwhals
```

and with `--backend pandas` on a transform written for pyspark it warns before
the run instead of failing on the first line. Why it works this way, and why
SQL is not the answer yet: [ADR 005](architecture/adr-005-portable-transforms.md).

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

### Prove it: the conformance suite

The tests every backend must pass ship with the engine, and the Spark and pandas
backends run them too. Subclass them in your backend's tests:

```python
import pytest

from ubunye.testing.backend_conformance import BackendConformance


class TestMyBackend(BackendConformance):
    @pytest.fixture
    def backend(self):
        backend = MyBackend(conf={"spark.sql.session.timeZone": "UTC"})
        backend.start()
        yield backend
        backend.stop()
```

They check that it is registered under its name and declares what it can do;
that transforms get its own frames and the engine a port whose `count()` means
rows; that it reads a CSV file exactly as Spark does, **leaving the same run
record hash as every other engine** for the same data; that what it writes it
reads back unchanged; and that the write modes it claims behave as Spark's do.
Anything your backend does not claim (a format, a write mode) is skipped, with
the reason. For the hash to match, a port that is not Spark or pandas reports
its column types by the run record's names (`int32`, `int64`, `float64`,
`bool`, `string`, `date`, `timestamp`, ...; see [ADR 006](architecture/adr-006-run-record.md)).
