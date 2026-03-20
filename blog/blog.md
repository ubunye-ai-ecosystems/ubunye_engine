# Ubunye Engine: From a Broken Pip Install to an Automated AI Ecosystem

*A Technical Memoir*

---

> This is not a polished tutorial. This is the real story — the broken imports, the silent
> CI failures at 2am, the bug that lived inside a single-line fallback, and the slow
> accumulation of something that actually works. If you've ever built a framework from
> scratch, you'll recognise every one of these moments.

---

## The Idea

Every data team eventually hits the same wall. You have notebooks that work locally.
You have Spark jobs that work on the cluster. You have ML experiments scattered across
a dozen different scripts, each trained differently, saved differently, versioned not at all.
Moving anything to production means a week of archaeology — figuring out which version of
which script produced which model, what data it saw, and why the schema looks different
today than it did last Tuesday.

The idea behind Ubunye Engine was simple: **one framework, config-first, that owns the
full lifecycle from raw data ingestion through to versioned model deployment.** Users write
their business logic. The engine handles everything else — I/O, monitoring, lineage, model
registry, CLI, documentation.

Simple idea. Complicated execution.

---

> **[Image: A whiteboard sketch with boxes and arrows — "config.yaml → Engine.run() →
> Readers → Task.transform() → Writers → Monitors". Some arrows are crossed out and
> redrawn. A coffee cup ring stains one corner. This is what day one looked like.]**

---

## Phase 1–4: Building the Foundation (And Discovering What "Done" Means)

The first four phases were about building the core contracts:

- **Phase 1** — Config loading: YAML + Jinja2 rendering before Pydantic validation.
  The key insight was that `{{ dt | default('1970-01-01') }}` needs to render *before*
  the schema sees it, not after. Getting that order right took longer than it should have.

- **Phase 2** — Lineage tracking: every run writes a structured JSON record — run ID,
  task path, input/output hashes, row counts, duration, status. Small files. Big value.
  When something breaks in production at 3am, the first question is always
  *"what data did this run see?"* The lineage file answers it without a Slack thread.

- **Phase 3** — Test infrastructure: unit tests that run without Spark, integration tests
  that spin up a real local SparkSession. The matrix was Python 3.9, 3.10, 3.11 × unit
  + integration. GitHub Actions. `pytest-cov`, `hypothesis`, `pytest-timeout`. Standard
  stuff, except none of these were in `pyproject.toml`'s `dev` extras yet, which meant
  CI was failing silently with `unrecognized arguments: --cov=ubunye` for weeks before
  anyone noticed.

- **Phase 4** — Access control: role-based config guards so not every pipeline can write
  to production targets. Not glamorous. Essential.

Each phase felt finished. None of them were.

---

> **[Image: A GitHub Actions run log. The left column shows a green checkmark on "Lint",
> a red X on "Unit Tests (Python 3.9)", and a spinning circle on "Integration Tests".
> The error visible in the log reads: "unrecognized arguments: --cov=ubunye". The
> timestamp reads 02:14 UTC.]**

---

## Phase 5: The Model Registry — The One That Changed Everything

The model registry was where the project went from "interesting framework" to
"something a team could actually use in production."

The design principle was strict: **the engine must never import sklearn, PyTorch,
XGBoost, or any ML library.** It interacts with models only through an abstract
contract — `UbunyeModel` — four methods: `train`, `predict`, `save`, `load`.
The engine calls these. It doesn't care what's inside them.

```python
class UbunyeModel(ABC):
    @abstractmethod
    def train(self, df: Any) -> Dict[str, Any]: ...

    @abstractmethod
    def predict(self, df: Any) -> Any: ...

    @abstractmethod
    def save(self, path: str) -> None: ...

    @classmethod
    @abstractmethod
    def load(cls, path: str) -> "UbunyeModel": ...
```

Simple. But getting the storage layout right, the version auto-increment, the
`development → staging → production → archived` lifecycle, and the promotion gates
took weeks of iteration.

The promotion gates were the most satisfying piece:

```python
PromotionGate({
    "min_accuracy": 0.85,
    "min_f1":       0.80,
    "require_drift_check": True,
})
```

A model cannot advance to production unless every gate passes. If it fails, the
error tells you exactly which metric missed and by how much. No more "I thought
it was good enough" production deployments.

The filesystem storage layout ended up clean:

```
.ubunye/model_store/
  fraud_detection/
    FraudRiskModel/
      registry.json          ← all version metadata
      versions/
        1.0.0/
          model/             ← opaque artifact (pkl, joblib, ONNX, anything)
          metadata.json
          metrics.json
        1.0.1/
          ...
```

Promoting a new version to production automatically archives the previous one.
One line of registry JSON update. No orphaned artifacts. No ambiguity about
what's live.

---

> **[Image: A terminal showing `ubunye models list` output. A table with columns:
> Version, Stage, Registered, Key Metrics. Row 1.0.0 is coloured green (production).
> Row 0.0.2 is white (archived). Row 0.0.1 is white (archived). Each row shows
> accuracy and F1 score. Clean, readable, one command.]**

---

## The Design Principle Worth Stealing (And Why It's Rare)

The model registry phase established one rule that turned out to be the most
significant architectural decision in the entire project: **the engine must never
import sklearn, PyTorch, XGBoost, or any ML library.** It interacts with models
only through the `UbunyeModel` contract — four abstract methods.

This has a name. It is called **hexagonal architecture**, also known as
**ports and adapters**. First described by Alistair Cockburn in 2005. The idea:
define abstract ports (interfaces) at the system boundary; everything outside
connects through adapters it provides. The engine core never depends on the
outside world — it defines the shape of the connection and lets adapters fill it.

What makes this interesting in Ubunye's context is that it wasn't deliberately
applied as "hexagonal architecture." It emerged from one practical constraint:
*we don't want to force users to install sklearn just to use the engine.* The
architectural pattern appeared as a consequence of a pragmatic decision. That's
how the best patterns usually arrive — not from a textbook, but from a constraint
that turns out to be the right one.

Most ML frameworks do the opposite. They own the ML layer. sklearn's `Pipeline`.
PyTorch Lightning's `Trainer`. Hugging Face's `Trainer`. Excellent tools —
all tightly coupled. If your model isn't sklearn-compatible, you're working
against the framework. If you want to swap PyTorch for JAX, you're rewriting.

Ubunye's model layer doesn't have this problem. A user can implement `UbunyeModel`
with sklearn today, ONNX tomorrow, and a custom C++ inference server next quarter.
The registry doesn't know or care. It calls `save()`, stores the artifact, calls
`load()`, and hands it to `predict()`. The internals are the user's business.

### The inconsistency this POC reveals — and the improvement

The model layer is correctly designed. But the pattern is incomplete — it only
exists in one layer. The rest of the engine has a consistency problem:

```
Engine (current state):
├── Reads   → pyspark.sql.DataFrame  ← coupled to Spark
├── Writes  → pyspark.sql.DataFrame  ← coupled to Spark
├── Transforms receive Spark DataFrame  ← coupled
└── UbunyeModel.train(df: Any)  ← decoupled ✓
```

The `Any` type annotation on `train(df: Any)` is a symptom. The engine passes a
Spark DataFrame because that's all it knows how to produce — but it annotates it
`Any` because it doesn't want to import PySpark into the model contract.

**The full hexagonal improvement is a `DataFramePort`:**

```python
# ubunye/core/ports.py
from typing import Any, Dict, List, Protocol, runtime_checkable

@runtime_checkable
class DataFramePort(Protocol):
    """Abstract port for any tabular data structure.

    Anything that satisfies this Protocol can flow through the engine.
    Spark DataFrames, pandas DataFrames, Polars DataFrames — all qualify
    without modification, because they already have these methods.
    """

    def schema(self) -> Dict[str, str]:
        """Return column names mapped to type strings."""
        ...

    def count(self) -> int:
        """Return number of rows."""
        ...

    def collect(self) -> List[Dict[str, Any]]:
        """Return all rows as list of dicts."""
        ...
```

Then lightweight adapters for cases where the native object doesn't satisfy the
Protocol natively:

```python
# ubunye/adapters/pandas_adapter.py
class PandasDataFrameAdapter:
    def __init__(self, df): self._df = df
    def schema(self): return {c: str(t) for c, t in self._df.dtypes.items()}
    def count(self): return len(self._df)
    def collect(self): return self._df.to_dict("records")

# ubunye/adapters/polars_adapter.py
class PolarsDataFrameAdapter:
    def __init__(self, df): self._df = df
    def schema(self): return {f.name: str(f.dtype) for f in self._df.schema}
    def count(self): return self._df.height
    def collect(self): return self._df.to_dicts()
```

**What this unlocks:**

**1. Spark-free unit tests with real data.** Currently, engine tests that avoid
Spark use mock objects — fake DataFrames with hard-coded return values. With
`PandasDataFrameAdapter`, those same tests run on real data, real schema, real
row counts. No SparkSession. No JVM. No Java install on the CI runner.

**2. Polars support in 30 lines.** Add `PolarsDataFrameAdapter`. No engine
changes needed. This is exactly what ports and adapters is for: adding a new
implementation behind an existing interface without touching the code that uses it.

**3. Local development on a laptop.** `ubunye run --backend pandas` uses pandas
as the execution engine. Same `transform()` code, same config YAML, same CLI —
running entirely without Spark. Experiment locally, deploy to the cluster when
ready. No environment gap.

**4. `UbunyeModel.train()` becomes consistent.** Instead of `train(df: Any)`,
it becomes `train(df: DataFramePort)`. The model knows exactly what interface
it will receive. If it needs the underlying native object (for sklearn, which
needs a numpy array), it calls `df.collect()` and builds from there.

**Why this isn't in the current version:**

It's a migration. Every `transform()` function currently receives a
`pyspark.sql.DataFrame`. Every `Reader.read()` returns one. Adding `DataFramePort`
as the official interface requires a v0.2.0 with a clear migration path.

The right approach for the next phase:
1. Ship `DataFramePort` as a `runtime_checkable` Protocol
2. Verify that Spark DataFrames already satisfy it via duck-typing (they already
   have `.schema`, `.count()`, `.collect()` — the Protocol check is structural)
3. Ship `PandasDataFrameAdapter` as the local/test backend
4. Add `--backend pandas` to `ubunye run`
5. Let `UbunyeModel.train(df)` accept `DataFramePort` in the contract

**Who else does this:**

- **Ibis Project** — a Python expression layer over DuckDB, Spark, BigQuery, Polars,
  pandas. One expression language, any backend. The closest equivalent in the query
  space.
- **Narwhals** — a lightweight compatibility layer between dataframe libraries, letting
  library authors write code that works on pandas, Polars, cuDF, Modin.
- **SQLGlot** — the same idea applied to SQL dialects: write one SQL, transpile to
  any backend.

The pattern is established. It is not new. What would be new in Ubunye's context
is applying it inside an ETL/ML engine that already has a correctly-designed model
layer — extending hexagonal architecture consistently from models down to the data
transport layer. That would make Ubunye the only config-driven ETL/ML engine with
a fully backend-agnostic data plane. That's a real differentiator.

---

> **[Image: A layered architecture diagram. At the centre: "Engine Core" in a solid
> box. Surrounding it, connected by dotted lines labelled "port": "SparkBackend",
> "PandasBackend", "PolarsBackend". On the right, connected to "port": "SklearnModel",
> "XGBoostModel", "CustomInferenceModel". Everything touches the engine through
> the interface. Nothing bypasses it. Caption: "Ports and adapters: the engine defines
> the shape. Backends fill it."]**

---

## The Documentation Detour

Every framework eventually needs documentation. The plan was to spend a weekend
on it. It took much longer.

**MkDocs** with the Material theme looked great. The `mkdocstrings` plugin would
auto-generate API docs from docstrings. `git-revision-date-localized` would show
when each page was last updated. `mkdocs build --strict` would catch any warnings
before deploy. Simple.

The first run of `mkdocs serve` produced this:

```
ERROR - mkdocstrings: ubunye.plugins.readers.rest_api.RestApiReader could not be found
```

Two hours of investigation found two separate root causes — both had to be fixed:

**Cause 1:** `import requests` at the top of `rest_api.py` failed when `requests`
wasn't installed in the docs build environment. The fix was lazy imports — move
`import requests` inside the functions that actually use it, guarded by
`TYPE_CHECKING` for type annotations:

```python
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import requests  # only for type checkers; not loaded at import time
```

**Cause 2:** `ubunye/plugins/readers/`, `ubunye/plugins/writers/`,
`ubunye/plugins/transforms/`, and three other sub-packages had no `__init__.py`.
The `griffe` AST engine that powers `mkdocstrings` couldn't traverse them —
it raised a silent `KeyError: 'readers'` and reported the class as not found.

Six empty files fixed it. Six. Empty. Files.

```
ubunye/cli/__init__.py
ubunye/compat/__init__.py
ubunye/plugins/ml/__init__.py
ubunye/plugins/readers/__init__.py
ubunye/plugins/transforms/__init__.py
ubunye/plugins/writers/__init__.py
```

---

> **[Image: A split screen. Left side: terminal output showing "ERROR - mkdocstrings:
> RestApiReader could not be found" repeated six times with different class names.
> Right side: a file tree showing six new empty __init__.py files highlighted in
> green. Caption: "Six bytes of solution for two hours of debugging."]**

---

## The Subtle Bug: When Sampling Returns Nothing

Between the documentation work, a unit test caught something genuinely tricky.

The test `test_different_input_different_data_hash` was failing — two DataFrames
with different data were producing identical hashes. The `hash_dataframe` function
sampled rows before hashing them:

```python
sample_rows = df.sample(fraction=0.01, seed=42).collect()
```

On a 2-row or 3-row DataFrame, `fraction=0.01` returns zero rows. The function
then fell back to `hash_schema(df)` — hashing only the column names and types.
Since both test DataFrames had the same schema but different data, the hashes
were identical.

The fix was a single fallback:

```python
sample_rows = df.sample(fraction=0.01, seed=42).collect()

if not sample_rows:
    # DataFrame too small for fractional sampling — collect all rows instead
    sample_rows = df.collect()

if not sample_rows:
    return hash_schema(df)
```

Three lines. But the regression test needed to document *why* this exists —
because otherwise someone will "clean it up" in six months and reintroduce the bug:

```python
def test_empty_sample_falls_back_to_collect_not_schema():
    """
    Regression: df.sample(0.01) returns empty on 2-3 row DataFrames.
    Without the collect() fallback, both DataFrames hash to hash_schema()
    — identical — even though their data differs.
    """
```

---

> **[Image: A diagram showing two small DataFrames (2 rows each, same schema,
> different values). An arrow from each points to "sample(0.01)" which both
> produce an empty list. Both empty lists point to "hash_schema()" producing
> the same hash "a3f7...". A red X is drawn over this path. A green arrow shows
> the corrected path: empty sample → collect() → hash row data → different hashes.]**

---

## CI/CD: The Perpetual Game of Whack-a-Mole

GitHub Actions was supposed to be set-and-forget. It was not.

**Problem 1: Missing dev dependencies.**
The test workflow ran `pytest --cov=ubunye --timeout=300`. Both flags required
packages that weren't in `pyproject.toml`'s `dev` extras:

```toml
# Before — silently broken
dev = ["pytest", "black", "ruff", "build"]

# After — actually works
dev = ["pytest", "pytest-cov>=4", "pytest-timeout", "hypothesis>=6",
       "requests>=2.28", "black", "ruff", "build"]
```

**Problem 2: setuptools flat-layout refusing to build.**
The build backend auto-discovers packages in a "flat layout" (packages at the
repo root). The repo had both `ubunye/` and `pipelines/` at the root. setuptools
refused:

```
Multiple top-level packages discovered in a flat-layout: ['ubunye', 'pipelines']
```

The `pipelines/` directory contains example pipeline tasks — it's not a Python
package, but setuptools didn't know that. Fix:

```toml
[tool.setuptools.packages.find]
include = ["ubunye*"]
```

**Problem 3: GitHub Pages 404.**
The docs workflow was building successfully locally but the deployed site was
returning 404. The `git-revision-date-localized` plugin requires full git history
to calculate when pages were last modified. The default `actions/checkout` does a
shallow clone (`fetch-depth: 1`). With `--strict` mode, the plugin warning became
an error, the build silently succeeded with empty output, and `gh-pages` branch
was never updated.

```yaml
# The one line that fixed it
- uses: actions/checkout@v4
  with:
    fetch-depth: 0   # full history required by git-revision-date-localized
```

**Problem 4: Dead import caught by ruff.**
After moving `import requests` to lazy imports inside `_build_session()`, the
`HTTPBasicAuth` import was still sitting in the `TYPE_CHECKING` block — imported
but never used as a type annotation (it was only used locally inside the function):

```
F401: 'requests.auth.HTTPBasicAuth' imported but unused
```

Two lines deleted. CI green.

---

> **[Image: A GitHub Actions workflow dashboard. Four jobs are shown. "Lint" has a
> green checkmark. "Unit Tests (Python 3.9)" has a green checkmark. "Unit Tests
> (Python 3.11)" has a green checkmark. "Integration Tests (Spark)" has a green
> checkmark. The commit message below reads: "fix: add fetch-depth: 0 to docs
> workflow". The timestamp is 23:47 UTC. It took all day to get here.]**

---

## The PyPI Publish Workflow

Publishing to PyPI should be the easy part. A version tag, a workflow file, done.

The question was authentication method. The older approach uses a `PYPI_API_TOKEN`
secret and the `twine` tool:

```yaml
- name: Upload to PyPI
  run: twine upload dist/*
  env:
    TWINE_USERNAME: __token__
    TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}
```

The modern approach uses OIDC Trusted Publishers — no secret to rotate, no token
to leak, no expiry to forget:

```yaml
environment: pypi
permissions:
  contents: read
  id-token: write          # OIDC token for PyPA Trusted Publisher

- name: Publish to PyPI
  uses: pypa/gh-action-pypi-publish@release/v1
  # No password needed — OIDC handles authentication
```

The trigger is a version tag. Nothing runs until:

```bash
git tag v0.1.1
git push origin v0.1.1
```

That's the moment it becomes real. A public package. Importable by anyone.
`pip install ubunye-engine`.

---

> **[Image: PyPI package page for "ubunye-engine". Shows version 0.1.1, description
> "Config-first, Spark-native ETL/ML engine with a modular plugin system", install
> command "pip install ubunye-engine", and optional extras listed: spark, ml, dev.
> The release date reads today's date.]**

---

## The Version Problem (And Its Elegant Solution)

`ubunye/__init__.py` had this:

```python
__version__ = "0.1.1"
```

Every time the version changed in `pyproject.toml`, someone had to remember to
update `__init__.py` too. Someone always forgot. The `ubunye version` CLI command
would show the wrong version. Users would open issues.

The fix is one of Python's most underused standard library features:

```python
from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("ubunye-engine")
except PackageNotFoundError:
    __version__ = "unknown"
```

`importlib.metadata` reads the installed package metadata — which comes directly
from `pyproject.toml`. One source of truth. The `PackageNotFoundError` guard
handles the case where someone runs the code directly from a cloned repo without
installing it first.

---

## Proving It Works: Kaggle

After all the unit tests and integration tests and CI pipelines, the question
remained: *does it actually work on real data?*

The Titanic dataset on Kaggle became the proving ground. No Hive metastore. No S3.
No Databricks. Just Python, pandas, and the engine contracts.

The journey started well with config loading and CLI commands. Then the first real
mistake:

```python
# My example code said:
recorder = LineageRecorder(store_object)
recorder.start_run("titanic_pipeline", "1.0.0", {"env": "kaggle"})

# The actual API is:
recorder = LineageRecorder(store="filesystem", base_dir="...")
recorder.task_start(context=ctx, config=config)
```

The error came immediately:

```
AttributeError: 'LineageRecorder' object has no attribute 'record_step'
```

Honest mistake. The documentation said one thing, the example code said another.
Fixed, documented, moved on.

Then the config validation error:

```
ValidationError: MODEL
  Input should be 'etl' or 'ml'
```

`MODEL: "titanic_etl"` — of course. `MODEL` is a job type classifier (`JobType`
enum), not a human-readable pipeline name. The human name lives in the folder
structure. `MODEL: "etl"`. Fixed.

Then the lineage inspection:

```python
for fname in os.listdir("/kaggle/working/.ubunye/lineage"):
    with open(f".../{fname}") as f: ...

# IsADirectoryError: [Errno 21] Is a directory: '.../lineage/titanic'
```

The lineage store doesn't write flat files in the root — it writes them under
`{usecase}/{package}/{task}/`. Use `os.walk`. Three-line fix.

---

> **[Image: A Kaggle notebook cell showing successful output. The cell ran
> `ubunye models list --use-case titanic --model TitanicSurvivalModel`.
> The output shows a formatted table: version 1.0.0 in green (production),
> version 1.0.1 in white (archived). Metrics show accuracy=0.8342, f1=0.7891.
> Below the cell, the execution time reads "0.8s".]**

---

## The End-to-End Notebook

The final artefact of the whole journey is a single Jupyter notebook:
`examples/titanic_end_to_end.ipynb`. It covers, in order:

1. `ubunye init` — scaffold the use-case folder structure
2. Three config files with Jinja2 templating and dev/prod profiles
3. `ubunye validate` and `ubunye plan` before touching data
4. `RawIngestTask` — clean raw passenger records, record lineage
5. `FeatureEngineeringTask` — engineer survival features, log to MLflow
6. `TitanicSurvivalModel(UbunyeModel)` — sklearn RF, library-independent contract
7. `ModelTransform(action=train)` — train, register, auto-promote via gates
8. `PromotionGate` — enforce quality thresholds before production
9. `ModelTransform(action=predict)` — load from registry by stage, score test set
10. `ubunye lineage list/show/trace/compare/search` — full audit trail
11. Train v2, compare versions, rollback, archive — full maintenance cycle

From `pip install` to a production-ready, versioned, monitored, lineage-tracked
ML pipeline. In one notebook. On a free Kaggle GPU.

---

> **[Image: The notebook's final summary table. Two columns: "Engine Feature" and
> "Key Class / Command". Rows include: Config templating → load_config() + Jinja2,
> Model training → UbunyeModel + ModelTransform, Quality gates → PromotionGate,
> Lineage → ubunye lineage trace, Rollback → registry.rollback(). Clean, complete,
> no Spark required.]**

---

## What Kaggle Doesn't Prove — And What I Still Need to Find Out

The Titanic dataset has 891 rows. Spark is overhead at 891 rows. The notebook
proves the contracts work and the engine runs end-to-end. It does not prove
anything about the environment where Ubunye is actually meant to operate.

Production looks different: 50 million rows with schema drift between runs.
Five pipelines running concurrently on a shared cluster. An engineer who didn't
build the framework trying to write their first `transformations.py` at 4pm on
a Friday. A model that passed all promotion gates but started degrading three
weeks after go-live because the upstream feature engineering changed.

I intend to test this in my current role — on actual production-scale data, with
an actual team, against real SLA pressure. That's a different test from Kaggle.
It's the test that matters.

I'll come back with that feedback. The framework passed its own tests. Whether it
survives contact with a real data team, over time, with engineers who didn't build
it — that's what I'll find out.

---

## What "Done" Actually Means

The repository now has:

- **261 tests** — unit and integration, Spark-free and Spark-full
- **Full CI/CD** — lint, unit matrix (3.9/3.10/3.11), integration (Spark + Java 17),
  docs build, PyPI publish on tag
- **MkDocs documentation site** — auto-deployed to GitHub Pages on every push to main
- **Model Registry** — filesystem-backed, versioned, lifecycle-managed
- **Lineage recording** — every run is an auditable JSON record
- **MLflow integration** — opt-in telemetry, zero coupling to the core engine
- **CLI** — `ubunye init`, `validate`, `plan`, `run`, `plugins`, `version`,
  `lineage *`, `models *`, `test run`
- **End-to-end example** — Titanic, real data, all features exercised

There is still no magic. The engine doesn't write your business logic for you.
It doesn't decide what features to engineer or what model to use. It doesn't
guess your schema or predict your data drift.

What it does is make everything around your business logic reliable, observable,
and repeatable. Your `transform()` function stays pure. The engine handles the
rest.

---

## The Honest Retrospective

If I had to do it again, the things I'd change:

**Start CI earlier.** The missing dev dependencies lived undetected for weeks
because the first version of the CI only ran `pytest tests/unit/` without
coverage or timeout flags. A proper CI setup on day one would have caught this
in the first commit.

**Add `__init__.py` files at scaffold time.** The griffe traversal failure
was entirely preventable. Any sub-package that contains importable Python code
should have `__init__.py`. This is not a new rule. It's been true since Python 2.

**Write the docs as you build, not after.** Documentation written after the fact
is archaeology. You have to re-excavate design decisions you made six weeks ago
and try to explain them to a stranger. Documentation written alongside the code
captures the *why* while it's still fresh.

**Test your own examples.** The incorrect `LineageRecorder` API in the Kaggle
example was embarrassing. If I had run the code before publishing it, that would
never have shipped. Always run the example. Always.

---

## Human + Agent: The Real Numbers

This project was built with an AI coding agent as a collaborator throughout.
That fact has been implicit in the whole story — the error messages, the corrections,
the back-and-forth. It's worth making it explicit and honest, because the numbers
tell a story that the AI industry mostly avoids having.

### How long would this have taken alone?

The honest estimate for a senior data engineer building this solo, no AI assistance:

| Phase | Solo estimate |
|---|---|
| Config system (YAML + Jinja2 + Pydantic v2 + profiles) | 4–5 days |
| Lineage tracking (RunContext, hasher, store, recorder, CLI) | 3–4 days |
| Test infrastructure (261 tests, unit + integration, fixtures) | 4–6 days |
| Access control | 1–2 days |
| Model Registry (base, loader, registry, gates, transform, 40 tests) | 8–12 days |
| Readers/Writers (REST API, S3, lazy imports) | 2–3 days |
| MkDocs documentation site (warnings, nav, mkdocstrings) | 2–3 days |
| CI/CD (3 workflows + all the debugging) | 2–3 days |
| Kaggle example notebook | 1–2 days |
| Blog | 0.5 days |
| **Total** | **27–40 days** |

That's 5–8 weeks of full-time engineering work. Conservative estimate.

### How long did it actually take with the agent?

Counting actual human hours spent — architecture decisions, reading generated code,
reviewing tests, directing the next step, catching errors, re-explaining context
after session resets:

| Phase | With agent (human hours) |
|---|---|
| Config system | 3–4 hours |
| Lineage tracking | 2–3 hours |
| Test infrastructure | 3–4 hours |
| Access control | 1–2 hours |
| Model Registry | 5–6 hours |
| Readers/Writers | 1–2 hours |
| Documentation | 3–4 hours |
| CI/CD debugging | 2–3 hours |
| Kaggle notebook | 2–3 hours |
| Blog | 1 hour |
| **Total** | **~23–31 hours** |

Roughly 3–4 full working days of human effort.

**The speedup is approximately 8–12× on elapsed time.**

---

> **[Image: A horizontal bar chart comparing the two approaches. Solo approach:
> a long bar spanning ~35 days. With agent: a short bar spanning ~3.5 days.
> A bracket on the right labels the gap "8–12× faster". Below the chart,
> a footnote reads: "Human hours, not wall-clock time. Excludes time spent
> waiting for CI runs."]**

---

### What a team would have cost — and whether agents replace developers

The honest comparison isn't solo-vs-agent. It's team-vs-agent-augmented-solo.

A realistic team to build this from scratch: one senior data/ML engineer
and two mid-level data engineers.

South African market rates (2025):

| Role | Annual salary | Daily rate (22 days/month) |
|---|---|---|
| Senior Data/ML Engineer | R850,000/year | ~R3,200/day |
| Mid-level Data Engineer | R520,000/year | ~R2,000/day |

For 35 working days of active development (the low end of the solo estimate):

| Cost item | Amount |
|---|---|
| Senior engineer × 35 days | R112,000 |
| Mid-level × 2 × 35 days | R140,000 |
| Team overhead (standups, PRs, code reviews, coordination — 20%) | R50,400 |
| **Total** | **~R302,000** |

Agent-augmented solo total: **R1,520**.

That is a **200× cost reduction** on the development of this specific framework.

Before concluding that agents replace developers, read that number carefully.

**The R302,000 team would have produced something the agent-augmented solo didn't:**
a codebase understood by three people. Three people who can maintain it, extend it,
and debug it at 3am without the original author present. The agent doesn't stay.
When the session ends, it forgets everything. The bus factor of agent-built code
is 1 — the human who directed it.

**Does this mean agents replace developers?** No. It means something more specific:
*one engineer who knows how to direct an agent can deliver what previously required
a team, for a fraction of the cost, in a fraction of the time.* The tradeoff is
that all the domain knowledge lives in one head instead of three. For a greenfield
POC that needs to prove itself before a team is justified, that tradeoff is correct.
For a production system that needs to outlive its creator, that tradeoff needs to
be actively managed — through documentation, tests, and the kind of CI discipline
this project demonstrates.

The real question is not "agents or engineers?" It's "what stage is this at, and
what does this stage need?" Early stage: agent-augmented solo is dramatically
more efficient. At scale: a team that uses agents collectively is more efficient
than a team that doesn't.

---

> **[Image: A cost comparison bar chart. Left bar: "3-person team, 35 days" —
> R302,000. Right bar: "Agent-augmented solo" — R1,520. The scale makes the right
> bar almost invisible. A note at the bottom: "The cost difference is real.
> So is the bus factor. Neither cancels the other."]**

---

### The experience compression — and why seniors benefit more than juniors

This is the part nobody in the AI industry talks about directly.

The skills required to build Ubunye Engine solo, without an agent:

| Skill domain | Experience needed |
|---|---|
| Apache Spark (production ETL, partitioning, shuffle tuning) | 3–5 years |
| Python packaging (pyproject.toml, entry points, OIDC PyPI) | 2–3 years |
| Pydantic v2 (released 2023; many engineers still on v1) | 6 months–1 year |
| MkDocs + mkdocstrings (griffe AST, strict mode, nav config) | 6 months |
| GitHub Actions (multi-job matrix, OIDC auth, `fetch-depth`) | 1–2 years |
| ML lifecycle management (versioning, gates, promotion, rollback) | 3–5 years |
| Abstract interface design (ports-and-adapters, plugin entry points) | 5–8 years |
| **Total (with realistic overlap)** | **8–12+ years of diverse, production experience** |

With an agent: roughly **5–7 years** of experience to use effectively. Not because
the agent does the easy parts — it does the volumetric parts. You still need enough
experience to design the architecture, evaluate what the agent produced, catch
hallucinated APIs, and know when the output is wrong in a non-obvious way.

**This is the counterintuitive result:** AI coding agents give more leverage to
senior engineers than to juniors. Not less.

A junior engineer with an agent generates code at a rate they cannot verify.
They cannot catch the `LineageRecorder.record_step` hallucination because they
don't know the actual API. They cannot evaluate whether a `DataFramePort` design
is architecturally sound. They cannot spot the empty-sample fallback bug because
they don't have the mental model of how Spark's fractional sampling behaves on
small DataFrames.

A senior engineer with an agent generates code at a rate they *can* verify — and
the agent handles everything they would otherwise have to type themselves. The
amplification is real because the verification capacity exists to match it.

This doesn't make agents useless for junior engineers. It means the value they
extract is lower, and the risk they carry is higher, until their verification
capacity catches up. The path for juniors is: use agents to learn faster, not to
skip learning.

---

> **[Image: A graph with "Engineering experience" on the X-axis and "Value extracted
> from agent" on the Y-axis. The line curves upward — low experience extracts little
> value (high hallucination risk, low verification capacity); high experience extracts
> high value. A note points to the middle of the curve: "The sweet spot is 5–8 years —
> enough experience to direct and verify, enough time to have the patience for it."]**

---

### Where the agent genuinely accelerated things

The agent was fastest on work that is **structurally clear but volumetrically large**:

- Writing 261 tests across 30 files once the testing patterns were established
- Implementing the 7 model registry methods once the storage layout was designed
- Writing 6 lineage CLI commands once the first one existed as a template
- Generating the full MkDocs navigation once the doc structure was planned
- The entire Kaggle notebook once the section structure was agreed

In all of these cases, the agent understood the pattern from one or two examples
and could replicate it at scale without degradation. The work that would have taken
a human a full day took an hour of direction and review.

### Where the agent failed or slowed things down

This is the part that doesn't make it into AI company marketing materials.

**1. API hallucination.**
The agent wrote Kaggle example code using APIs that don't exist:

```python
# Agent's example (wrong):
recorder = LineageRecorder(store_object)
recorder.start_run("pipeline", "1.0.0", {})

# Actual API:
recorder = LineageRecorder(store="filesystem", base_dir="...")
recorder.task_start(context=ctx, config=config)
```

The code looked plausible. It compiled. It failed at runtime with `AttributeError`.
The agent had read the `LineageRecorder` class earlier in the session, then
invented a simpler API that felt right. This cost a debugging round.

**2. Context degradation across sessions.**
The project spanned multiple long conversations. At the start of each session,
the agent had to re-read files to reconstruct what existed. Design decisions made
three sessions ago were occasionally re-invented differently. A human working
continuously carries that context in their head for free.

**3. The "plausible but wrong" problem.**
The hash_dataframe empty-sample bug was subtle — the code was correct for large
DataFrames and silently wrong for small ones. The agent wrote the regression test
correctly once the bug was explained, but didn't catch the bug during initial
implementation. Neither would most humans on first read. The point is: the agent
produces code that *looks right* at a rate that exceeds a human's ability to
*verify right*.

**4. Over-generation.**
Asked to add a section, the agent adds the section plus related error handling
plus a helper function plus a docstring. Asked to fix a bug, it sometimes
refactors surrounding code that didn't need touching. Every unrequested addition
is work you have to review and potentially undo. Discipline about scope is a
human responsibility — the agent defaults to more.

**5. The verification tax.**
261 tests generated quickly still need to be read. 30 files of code still need
to be understood. The agent compresses the *writing* time dramatically but the
*review* time is irreducible — and it's harder to review code you didn't write
than code you did. You're always slightly behind the output.

---

> **[Image: A split bar showing "time saved" vs "overhead introduced" for
> each phase. Most phases show large savings and small overhead. The
> documentation phase shows moderate savings with notable overhead (back-and-forth
> on warnings). The Kaggle notebook shows large savings but a visible "debugging
> wrong API" overhead band in red. The overall balance is strongly positive but
> not uniformly so.]**

---

### What this means for "vibe coding"

Vibe coding — the practice of generating code with AI and accepting it without
deeply understanding it — works fine for throwaway scripts and prototypes where
the cost of being wrong is low.

It does not work for a framework with a public API, 261 tests, and users who
will `pip install` it.

The reason is simple: **you cannot debug a codebase you don't understand.**
When the hash_dataframe bug appeared, identifying it required knowing exactly
how the hasher was supposed to work, what the fallback chain was, and why a
2-row DataFrame would behave differently than a 200-row one. That understanding
came from the architectural decisions made before the code was written — decisions
that were human, not agent.

Vibe coding transfers the typing to the agent. It cannot transfer the
understanding. And when production breaks at 3am, understanding is the only
thing that matters.

### The real shift

The role of the human engineer changes, but it doesn't diminish. It shifts from:

> *"I type the code"*

to:

> *"I decide what to build, verify what was built, catch what's wrong,
> and direct what comes next"*

That is harder than it sounds. Reviewing 261 tests across 30 files — asking
whether each test is testing the right thing, not just whether it passes — is
genuinely skilled work. Architectural decisions about promotion gate design or
lineage storage layout are not things an agent can make for you. Catching an
API hallucination requires knowing the actual API.

The engineer who can do those things well, and who uses an AI coding agent to
handle the volumetric work, is dramatically more productive than one who does
either alone.

The engineer who accepts agent output without verification is not coding.
They are accumulating liability.

---

> **[Image: A simple diagram with two columns. Left column: "Human does"
> — Architecture, Direction, Verification, Debugging, Judgement. Right column:
> "Agent does" — Pattern replication, Boilerplate, Volume, First drafts,
> Consistency. An arrow between the columns is labelled "collaboration".
> No column is labelled "replaced".]**

---

## The Bill: What This Actually Cost in Rands

These are not estimates. The session JSONL file was parsed to get the exact numbers.

### Raw token usage (one primary session)

| Metric | Count |
|---|---|
| API turns (back-and-forth exchanges) | 1,008 |
| Input tokens (fresh user messages) | 66,399 |
| Output tokens (agent-generated code, explanations) | 421,557 |
| **Cache read tokens** | **95,038,923** |
| Cache write tokens | 6,877,864 |
| Total non-cache tokens | 487,956 |

That cache read number — 95 million — is not a typo.

### What it cost

Using claude-sonnet-4-6 API rates (input: $3/M, output: $15/M,
cache read: $0.30/M, cache write: $3.75/M):

| Category | Tokens | Cost (USD) | Cost (ZAR @ R18.8) |
|---|---|---|---|
| Input (fresh) | 66,399 | $0.20 | R3.76 |
| Output | 421,557 | $6.32 | R118.82 |
| Cache reads | 95,038,923 | $28.51 | R536.00 |
| Cache writes | 6,877,864 | $25.79 | R484.86 |
| **Total** | | **$60.83** | **R1,143.53** |

Plus the Claude Pro subscription: $20/month = **R376/month**.

**Total cost of this project: approximately R1,520** — subscription plus API value consumed.
For 3–4 days of equivalent engineering output that would have taken 5–8 weeks alone,
that is an extraordinarily good deal. But only if you understand where the money went.

---

> **[Image: A pie chart of the R1,143 API cost. Two large slices dominate: "Cache reads
> R536 (47%)" and "Cache writes R485 (42%)". A small slice: "Output R119 (10%)".
> A tiny sliver: "Input R4 (1%)". Caption: "90% of the cost is context, not code."]**

---

### Where 90% of the cost went — and why

The pie chart tells the story. **90% of the bill is cache operations — not code generation.**

Here is what happened: every time the agent responded, it received the entire conversation
history plus all previously-read files as cached context. By turn 500, a single API call
was carrying the entire codebase, every error message, every correction, every file read
from the previous 499 turns — even the ones that were no longer relevant.

The session lasted 1,008 turns. Across those turns, **95 million tokens were read from
cache** — the same accumulated context, re-sent on every exchange, growing heavier
with each one.

This is how Claude Code's prompt caching works: it is very fast (cache reads are 10×
cheaper than fresh input) but it accumulates. A long single session with heavy file
reading is structurally expensive.

---

### Where tokens were specifically wasted

**1. Re-reading the same files across phases (cache write waste)**

`main.py` (450 lines), `schema.py`, `recorder.py`, `registry.py` (458 lines) were
read multiple times — once for context, again after a session reset, again when
debugging a related issue. Each new session starts with a cold cache. Prior cache
writes from the previous session are worthless. Every file re-read in a new session
is a fresh cache write billed at $3.75/M tokens.

**2. The mkdocstrings investigation**

Finding the two-cause root of the `RestApiReader could not be found` error required
testing multiple hypotheses. Each hypothesis involved reading files, generating a fix,
and discovering it was incomplete. Three rounds before both causes (lazy imports +
missing `__init__.py`) were identified. This generated output tokens for two wrong
fixes and one correct one — plus the accumulated cache re-reads between each attempt.

**3. The Kaggle example rewrites**

The wrong `LineageRecorder` API example was written, deployed to the user, tested,
found broken, explained back, and rewritten. Output tokens paid twice for the same
functionality. Then the `MODEL: "titanic_etl"` enum error. Then the `os.listdir`
directory error. Three separate Kaggle errors, each requiring a correction cycle.

**4. Long error logs pasted into chat**

When CI failed, full stack traces and GitHub Actions logs were pasted into the
conversation. Each paste added thousands of tokens to the running context — which
then got re-sent as cache reads on every subsequent turn for the rest of the session.
The agent only needed the last five lines of most of those errors.

**5. The plan file carried everywhere**

The Phase 5 model registry plan was a detailed ~200-line specification that sat in the
context for the entire implementation session. After the first two phases of
implementation it was no longer being referenced — but it was still being cache-read
on every turn.

---

> **[Image: A timeline of the session broken into phases. Each phase is a horizontal
> band. The band gets progressively darker from left to right, representing growing
> cache size. Phase 1 (config) is light. Phase 5 (model registry) is medium. The
> Kaggle debugging and blog sections are darkest. An annotation reads: "context
> size compounds — every turn in a late session is more expensive than a turn in
> an early session doing the same work."]**

---

### What could have been improved — agent side

**Pre-check APIs before writing examples.**
Before writing Kaggle example code for `LineageRecorder`, the agent should have
re-read the actual class definition. It had read it earlier in the session, but
relied on memory across many intervening turns. The cost of one `Read` tool call
(cheap) is far lower than the cost of a wrong example, a user debugging it, an
explanation, and a rewrite.

**Warn when context is ballooning.**
By turn 600, the session was carrying the weight of 500 previous turns. The agent
could have flagged: *"This session is getting large — consider starting a fresh
session for the next phase to reduce cache costs."* It didn't. The human had no
visibility into this.

**Consolidate file reads.**
Multiple files were read one at a time in separate tool calls when they could have
been batched. Each sequential read adds to the context before the next one is needed.
Reading 5 related files in parallel takes the same context space but doesn't
inflate the turn count.

**Proactively surface related issues.**
When fixing the `HTTPBasicAuth` lazy import, the agent could have predicted:
*"This same pattern exists in `rest_api_writer.py` — want me to fix both now?"*
Instead, the writer file surfaced as a separate ruff error in the next CI run —
another correction cycle, more turns, more cache.

---

### What could have been improved — human side

**Break work into smaller sessions by phase.**
One session per phase would have kept each session's cache footprint small.
By doing everything in one long session, the later phases were paying the cache
cost of all earlier phases on every single turn. Session hygiene is a real
cost lever.

**Use CLAUDE.md more aggressively.**
The project's `CLAUDE.md` exists. It could have contained the key API contracts —
`LineageRecorder` constructor signature, `ModelRegistry.get_model()` return type,
the `MODEL` enum values. If those facts were in `CLAUDE.md`, the agent reads
them at session start and doesn't hallucinate simpler versions three hours later.

**Don't paste full error logs.**
Paste the relevant five lines, not the full 200-line stack trace. The agent reads
the relevant part and the rest sits in context for hundreds of turns costing money.

**Start new sessions when a phase is done.**
The instinct is to keep going while momentum is there. The cost is that every
subsequent turn carries the entire previous phase as dead weight. The disciplined
move is: phase complete, new session, fresh cache.

**Tighter prompts.**
Several prompts were open-ended — *"fix this"*, *"add examples"*. Open-ended
prompts invite open-ended responses. Longer responses. More output tokens. More
to review. More cache written. Specific prompts get specific responses.
*"Fix only the `_build_session` function to use a lazy import"* costs less than
*"fix the import issue"* and produces less to review.

---

> **[Image: Two columns side by side. Left: "What happened" — one long session,
> files read repeatedly, full error logs pasted, open-ended prompts. Right:
> "What to do instead" — session per phase, CLAUDE.md API contracts, paste 5 lines
> not 500, specific prompts. An estimate at the bottom: "Estimated saving: 40-60%
> of cache cost with better session hygiene."]**

---

## A Framework for Collaborative Agent Development

This is not a section about Ubunye Engine. It's about what building it taught
me about how to work with AI agents effectively — stated simply, so it's
actually useful.

**Five things that work:**

**1. One session per phase, always.**
When a phase is complete, start a new session. The cache footprint stays small.
Every turn in a new session costs less because it carries less history. The
discipline is counterintuitive — you want to keep going while momentum is there —
but the cost difference is measurable. Estimate: 40–60% reduction in cache spend
with strict session boundaries.

**2. `CLAUDE.md` as a living API contract.**
Any interface the agent will need to call or reference should be in `CLAUDE.md`
with its actual signature, not prose description. Not "the `LineageRecorder`
records task runs" — but the actual constructor and method signatures, typed.
The agent reads `CLAUDE.md` at session start. If the API is there, it won't
invent a cleaner-sounding version three hours later.

**3. Bounded prompts, not open-ended ones.**
"Fix the import issue in `rest_api.py`" generates a targeted response.
"Fix the import issue" generates a response that may touch three files you
didn't ask about and add error handling you didn't need. Scope in the prompt
produces scope in the output. Scope in the output reduces review time.

**4. Paste five lines, not five hundred.**
When something breaks, find the relevant line of the error and share that.
Full stack traces and full GitHub Actions logs paste thousands of tokens into
the context, which then get re-sent as cache reads on every subsequent turn
for the rest of the session. The agent reads the relevant part. The rest is
pure cost.

**5. Run examples before publishing them.**
If you ask the agent to write example code, run it before you use it. API
hallucinations pass static analysis. They fail at runtime. The ten seconds
it takes to run the example is worth less than the debugging round when
someone else runs it and finds it broken.

---

**Three things the tools should do better:**

**Surface context size.** By turn 600 of a 1,008-turn session, the cache
footprint was enormous. The agent had no way to signal this and didn't.
A token budget indicator — visible, real-time, costed — would change how
humans manage sessions. This doesn't exist yet in any tool I've used.

**Re-verify APIs before writing examples.** If the agent is about to write
code that calls an API it read 400 turns ago, it should re-read the source
before writing. The cost of a `Read` tool call is orders of magnitude less
than the cost of an API hallucination correction cycle. This is a tool design
problem, not a user discipline problem.

**Persistent project memory across sessions.** The biggest structural weakness
of current AI coding tools is that every session starts from scratch. A human
engineer working continuously for two weeks carries two weeks of context in
their head. The agent starts fresh every morning. `CLAUDE.md` is a manual
workaround for a problem that should have a first-class solution: structured,
persistent, queryable project memory that the agent can read and write,
survives session boundaries, and gets more useful over time.

---

## The Question This Blog Doesn't Answer (But Should)

Before getting to the name, there's an honest gap in this entire post worth naming.

Reading it back, it answers *how* this was built well. It does not answer *why
you should use it*. That's a different question — and the harder one.

**Who is this actually for?**

Not a solo data scientist running notebooks. They don't need a framework — they
need pandas and a good naming convention. Not a company running Databricks with
a dedicated platform team either — they already have Unity Catalog, Delta Live
Tables, and MLflow baked in.

The real target is the gap in between: a **data team of 2–8 people** that has
outgrown notebooks but can't justify a full platform engineering hire. Teams where
the same person writes the ingestion job, trains the model, deploys it to
production, and then gets paged when it breaks at 3am. Teams where "model
versioning" currently means a folder called `models_final_v3_USE_THIS_ONE/`.

For that team, Ubunye Engine's value proposition is specific:

- You write `transform()`. The engine handles I/O, lineage, monitoring, and
  model versioning around it.
- Your notebook code and your production code are the same code.
  No rewrite when you go from experiment to prod.
- When something breaks, `ubunye lineage trace` shows you exactly what data
  that run saw. No archaeology.

**The value is not the code. It's the convention.**

This is the thing worth understanding clearly. Every individual component in
Ubunye Engine exists elsewhere. Pydantic v2 config exists in a hundred libraries.
Lineage tracking exists in MLflow. Model versioning exists in DVC. Spark readers
exist in PySpark itself.

The value Ubunye provides is that all of these are *wired together the same way
for everyone on your team.* When a new engineer joins, there is one right place to
look. The ETL lives in `transformations.py`. The config lives in `config.yaml`.
The model artifact is in `.ubunye/model_store/{use_case}/{model}/versions/`. The
lineage for any run is under `.ubunye/lineage/`. The CLI has one entry point:
`ubunye`.

Compare this to the alternative, which every data team knows intimately: every
engineer makes different choices. One uses a bash script. One uses a Python file
with hardcoded paths. One trains a model in a notebook and pickles it to a shared
drive with a name that includes "FINAL". One writes a Spark job that nobody else
knows how to run. All of them work. None of them are compatible with each other.

Ubunye's actual value is **one convention, enforced by code, shared by the whole
team.** The framework is an organizational protocol dressed as a Python package.
That convention — the agreement about how things are done — is worth more than any
individual feature the engine provides, because it's the thing that lets a second
engineer pick up where the first one left off.

**Why not just use Airflow + MLflow + Delta Lake?**

You could. That stack is proven and well-supported. But it also means:
three separate tools to learn, three separate configs to maintain, three separate
places to look when something breaks, and a minimum infrastructure footprint that
requires someone to care for it full-time.

Ubunye Engine is not a replacement for that stack at scale. It is a lower-friction
path to *getting to* scale — one config file, one CLI, one place where the whole
pipeline lives.

Whether that trade-off is right depends entirely on your team's size and context.
This project doesn't pretend otherwise.

---

> **[Image: A simple 2x2 matrix. X-axis: Team Size (small → large). Y-axis:
> Infrastructure Complexity (low → high). Bottom-left quadrant labelled "notebooks
> + pandas". Top-right quadrant labelled "Databricks + Unity Catalog". The
> middle band — small-to-medium team, moderate complexity — is labelled
> "Ubunye Engine". A circle is drawn around that band. No claim is made about
> the other quadrants.]**

---

## What Ubunye Means

*Ubunye* is a Zulu word meaning **oneness** or **unity**.

The name was chosen deliberately. The goal was never to add another tool to the
stack — it was to unify the stack. One config. One CLI. One lineage record. One
model registry. One way to move data from raw to production, regardless of the
source system, the ML library, or the cloud provider.

Whether that goal has been achieved is for users to decide.

---

## What This Post Is Actually Trying to Say

This is the part I want understood clearly. Not the architecture. Not the cost
breakdown. This part.

---

The raw journey format, the specific error messages, the honest retrospective —
they all serve one purpose: to show that this project was *finished*, not just
started.

**Most open-source projects are abandoned.** GitHub's data is consistent on this:
the majority of public repositories have fewer than five commits. Of the projects
that survive past an initial push, most stall at 60–70% complete — the interesting
part is built, the boring work isn't. No tests. No CI. No docs. No example that
anyone actually ran. A `README.md` that describes what it *will* do someday.

The frameworks that have this level of completion — the ones with 200+ tests,
real documentation, CI matrices, published packages, and end-to-end examples —
were built by teams:

- **Kedro** (QuantumBlack/McKinsey) — 15+ engineers, 3 years, millions in
  organizational backing
- **Prefect** — 20+ engineers, VC-funded, dedicated docs and devrel teams
- **DVC (Data Version Control)** — Iterative.ai, 30+ engineers
- **Great Expectations** — 50+ engineers, dedicated quality assurance team

These were all built by teams. This was built by one person in approximately
30 human hours with an AI agent.

That comparison is the one I'm most proud of. Not because solo work is better
than team work — it isn't. But because it demonstrates what this specific
combination of human judgment and agent capability can produce at a level of
completion that was previously reserved for well-funded teams.

---

**Why did I build this particular thing?**

Because I've lived the problem it solves. In production data engineering roles,
I've seen the archaeology — the week spent figuring out which version of which
script produced which model. I've seen the `models_final_v3_USE_THIS_ONE/` folder.
I've been the person paged at 3am who had to reconstruct what a pipeline did from
git history and intuition because there was no lineage record.

The idea was not "build a framework." The idea was "never do that archaeology
again." The framework is the consequence of the constraint.

---

**Is the architecture new?**

Partially. The individual components — Pydantic config, Spark I/O, lineage JSON
records, model versioning — all exist elsewhere. What's different is the
*combination* and the *design principle*:

- **Kedro** has a similar config-driven pipeline approach, but doesn't own the
  model lifecycle — you integrate MLflow separately.
- **MLflow** has model registry and versioning, but it's coupled to the MLflow
  server and doesn't own the data pipeline.
- **DVC** has data versioning and pipeline tracking, but requires a separate model
  serving layer.
- **Metaflow** has a similar decorator-based approach to pipeline definition, but
  it's AWS-native and doesn't have the config-first philosophy.

The combination that doesn't exist elsewhere: **config-driven + library-independent
model interface + filesystem-native + single CLI + lineage-by-default.** No server
required. No cloud account required. Run it locally. Run it on Databricks. Run it
on a Raspberry Pi if your data is small enough. The config is the interface; the
rest is plugs.

The `UbunyeModel` hexagonal design — the engine never importing ML libraries —
is the piece I'm most proud of architecturally. It appeared naturally from the
constraint "don't force users to install libraries they don't use," and the
architectural result is something most ML frameworks never achieve: a model layer
that is genuinely backend-agnostic. Swap sklearn for ONNX for a custom C++ model
without touching the engine.

That's not done elsewhere in this exact form. And I thought of it not by reading
about hexagonal architecture, but by following the constraint. Sometimes the right
principle arrives when you're not looking for it.

---

**The discipline that actually matters:**

Writing test 251 is not interesting. It is necessary.
Debugging the `fetch-depth: 0` CI failure at 11pm is not interesting. It is necessary.
Running your own example code before publishing it is not interesting. It is necessary.

The discipline to ship boring work — the work that most engineers skip because
it's not intellectually stimulating — is rarer than the ability to design
interesting architecture. And it is worth considerably more, because interesting
architecture that isn't tested, documented, or running is just a whiteboard photo.

That's the real message. Not "look at this framework." But:

**I am the kind of engineer who finishes things — not just codes things.**

The pip install works. The tests pass. The docs are live. The example ran on real
data and the errors were fixed before this post was written.

That's not a start. That's the point. And it's rarer than it should be.

---

---

## One More Thing You Should Know

This blog was co-written with an AI agent.

That's not a disclaimer. It's the point.

The blog is a document about building software with a human-AI collaborator.
The blog itself was built through human-AI collaboration. The agent drafted the
structure. I pushed back on the sections that were too clean, too polished, too
careful. The agent rewrote them. I pushed back again. The rough edges in this
document are the places where I said "no, say it like this" — and those are the
parts worth reading.

I'm not mentioning this because I have to. I'm mentioning it because not
mentioning it would be dishonest. The entire argument of this post is that
human-agent collaboration produces something neither could produce alone — a
combination of the agent's capacity for volume and consistency and the human's
capacity for judgment and authenticity. This post is evidence of that. Hiding
the evidence while making the argument would undermine the argument.

There's a version of this blog that a professional editor polished, all rough
edges smoothed, all awkward sentences fixed. That version would read better.
It would say less.

The version you read is the one where a human who built a framework over
30 hours and R1,520 and hundreds of small decisions sat down and tried to tell
the truth about it, with an agent doing the structural work and a human doing
the substantive work. That is the collaboration model described above. The
blog is a proof of the thesis, not just a description of it.

---

*The Ubunye Engine is open source.*
*Source code: [github.com/ubunye-ai-ecosystems/ubunye_engine](https://github.com/ubunye-ai-ecosystems/ubunye_engine)*
*Documentation: [ubunye-ai-ecosystems.github.io/ubunye_engine](https://ubunye-ai-ecosystems.github.io/ubunye_engine)*
*Install: `pip install ubunye-engine`*
