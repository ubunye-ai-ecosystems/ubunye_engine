# Interfaces (Protocol APIs)

A seam is a place you plug your own code in. Ubunye has five, for anyone who
wants to keep models, run records, deploys or credentials somewhere Ubunye does
not ship.

Each seam is a Python `typing.Protocol`: any class with the right methods fits,
with no base class to inherit. Your class is found through an entry point (a
name your package registers in its `pyproject.toml`).

| Protocol | What you plug in | Entry point group | Chosen by |
|---|---|---|---|
| [`DeployAdapter`](#deployadapter) | A **deploy adapter**: sends a task to a remote platform | `ubunye.deploy_adapters` | The deploy command |
| [`RegistryBackend`](#registrybackend) | A **registry backend**: where model versions and their stages live | `ubunye.registry_backends` | Name, then `UBUNYE_REGISTRY_BACKEND`, then `filesystem` |
| [`RunRecordStore`](#runrecordstore) | A **run record store**: where run records go | `ubunye.lineage_backends` | `--lineage-backend`, then `UBUNYE_LINEAGE_BACKEND`, then `filesystem` |
| [`LineageBackend`](#lineagebackend) | The older **lineage backend** contract, kept for code that uses it | `ubunye.lineage_backends` | As for the run record store |
| [`AuthBackend`](#authbackend) | An **auth backend**: finds and checks Databricks credentials | `ubunye.auth_backends` | The credentials that are set |

These are not the same as an **execution backend** (`spark`, `pandas`, the
engine a task runs on). That seam is a base class, not a protocol, and has its
own page: [Execution backends](backends.md). Readers, writers and transforms
are plugins too: [Writing a plugin](connectors/plugin_guide.md).

---

## Design principles

Three decisions shape how every seam behaves.

### Decision 1: Non-blocking metadata writes

`record()`, `register()`, and `promote()` send their metadata updates to a
**background worker thread** with a bounded queue (default 1 000,
`UBUNYE_METADATA_QUEUE_SIZE`).

- Queue overflow: drop the oldest pending write, log a warning.
- Shutdown: `flush()` drains the queue, with a timeout (default 30 s,
  `UBUNYE_METADATA_FLUSH_TIMEOUT`).
- Artifact writes (model files) stay synchronous: the method returns
  only after the model is safely stored.

It uses a worker thread, not `async`/`await`, because async fights Spark's
threading model in Python.

### Decision 2: Graceful degradation

When a metadata write fails, the run goes on and the record is kept in a local
file to replay later. See [Fallback manifests](#fallback-manifests-and-ubunye-sync).

**Auth is excluded**: authentication failures always stop the run at once. A
pipeline must not run with invalid credentials.

### Decision 3: Schema evolution

Every dataclass that crosses a seam separates **strict core fields** from a
flexible `metadata: Dict[str, str]`:

- Core fields never change without a major engine version bump.
- New data goes into `metadata`: old clients ignore unknown keys, new
  clients write new keys without touching the schema.
- Every record stamps `engine_version` so readers can filter by writer version.

---

## Protocols

### DeployAdapter

A deploy adapter sends a task's config and artifacts to a remote environment.

```python
from ubunye.interfaces import DeployAdapter, DeployContext, DeployResult
```

::: ubunye.interfaces.deploy.DeployAdapter
    options:
      show_root_heading: false
      show_source: false
      heading_level: 4

::: ubunye.interfaces.deploy.DeployContext
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

::: ubunye.interfaces.deploy.DeployResult
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

**Built-in**: `DatabricksDeployAdapter`, entry point `ubunye.deploy_adapters:databricks`.

---

### RegistryBackend

A registry backend manages model versions: register, promote, demote, delete.

```python
from ubunye.interfaces import RegistryBackend, ModelVersionInfo
```

::: ubunye.interfaces.registry.RegistryBackend
    options:
      show_root_heading: false
      show_source: false
      heading_level: 4

::: ubunye.interfaces.registry.ModelVersionInfo
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

**Built-in**:

- `FilesystemRegistryBackend`, entry point `ubunye.registry_backends:filesystem`.
  Keeps models and their stages in a folder.
- `MLflowModelRegistryBackend`, entry point `ubunye.registry_backends:mlflow`.
  Keeps them in the MLflow Model Registry: each Ubunye version is a registered
  model version, and `staging` and `production` are MLflow aliases. Needs
  `pip install 'ubunye-engine[ml]'`. See
  [ADR 014](architecture/adr-014-mlflow-registry-aliases.md).

`MLflowRegistryBackend` (a folder, with runs logged to MLflow on top, best
effort, never blocking the pipeline) is deprecated since 0.9 and still works;
use `MLflowModelRegistryBackend`.

**Which one is used**: the name you give (`ubunye models --backend`, or
`backend:` in the config's registry block), else `UBUNYE_REGISTRY_BACKEND`, else
`filesystem`. Ubunye does not guess from the host: on Databricks, set
`UBUNYE_REGISTRY_BACKEND=mlflow` if you want MLflow. Why:
[ADR 010](architecture/adr-010-registry-backends.md).

---

### RunRecordStore

A run record store keeps run records (the whole `RunContext`, what
`lineage show` prints). Every store in `ubunye.lineage_backends` provides it.
No base class needed.

```python
from ubunye.interfaces import RunRecordStore
```

::: ubunye.interfaces.lineage.RunRecordStore
    options:
      show_root_heading: false
      show_source: false
      heading_level: 4

**Built-in**:

- `FileSystemLineageStore`, entry point `ubunye.lineage_backends:filesystem`.
  One JSON file per run. Uses a `run_id` to path index for fast lookups and an
  in-memory `RunContext` cache to avoid parsing JSON again on search.
- `DeltaLineageBackend`, entry point `ubunye.lineage_backends:delta`.
  Stores each run record whole (JSON plus index columns) in a Delta table or
  path, through the active SparkSession or a local one it starts. A record it
  cannot write goes to a fallback manifest, with a warning.

**Which one is used**: `--lineage-backend` (or `lineage_backend=` in Python),
else `UBUNYE_LINEAGE_BACKEND`, else `filesystem`. Where each keeps its records:
[Where run records are stored](cli.md#where-run-records-are-stored). Why:
[ADR 011](architecture/adr-011-run-record-stores.md).

Prove a store keeps the contract with the shipped suite:

```python
import pytest

from ubunye.testing.lineage_conformance import LineageStoreConformance


class TestMyStore(LineageStoreConformance):
    @pytest.fixture
    def store(self, tmp_path):
        return MyStore(str(tmp_path / "runs"))
```

---

### LineageBackend

The older lineage contract: record, search, compact, over a flat
`LineageRecord`. Kept for code that uses it; no run record is stored through it,
since a `LineageRecord` keeps only a few fields of one.

```python
from ubunye.interfaces import LineageBackend, LineageRecord
```

::: ubunye.interfaces.lineage.LineageBackend
    options:
      show_root_heading: false
      show_source: false
      heading_level: 4

::: ubunye.interfaces.lineage.LineageRecord
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

**Built-in**: the same two classes as the run record store. The Delta one's
older `LineageRecord` methods still use JSON files when no SparkSession existed
when it was built, and log that.

---

### AuthBackend

An auth backend finds and checks Databricks credentials.

```python
from ubunye.interfaces import AuthBackend, Credentials
```

::: ubunye.interfaces.auth.AuthBackend
    options:
      show_root_heading: false
      show_source: false
      heading_level: 4

::: ubunye.interfaces.auth.Credentials
    options:
      show_root_heading: true
      show_source: false
      heading_level: 4

**Built-in**:

- `TokenAuthBackend`, entry point `ubunye.auth_backends:token`.
  Reads the token from `DATABRICKS_TOKEN` (or the env var named by `token_env` in `targets.yaml`).
- `ServicePrincipalAuthBackend`, entry point `ubunye.auth_backends:service_principal`.
  Uses `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` for OAuth M2M auth.

**Which one is used**:

| Set in the environment | Auth backend |
|---|---|
| `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` | `service_principal` (wins when both kinds are set) |
| `DATABRICKS_TOKEN` | `token` |
| Neither | Raises `AuthNotFoundError` |

See [Databricks authentication](databricks-auth.md).

---

## Entry point groups

Every group the engine reads, with what it ships. Third-party packages add
their own classes under these groups in their `pyproject.toml`.

| Group | What it holds | Built-in names |
|---|---|---|
| `ubunye.deploy_adapters` | Deploy adapters | `databricks` (`DatabricksDeployAdapter`) |
| `ubunye.registry_backends` | Registry backends | `filesystem` (`FilesystemRegistryBackend`), `mlflow` (`MLflowModelRegistryBackend`) |
| `ubunye.lineage_backends` | Run record stores | `filesystem` (`FileSystemLineageStore`), `delta` (`DeltaLineageBackend`) |
| `ubunye.auth_backends` | Auth backends | `token` (`TokenAuthBackend`), `service_principal` (`ServicePrincipalAuthBackend`) |
| `ubunye.artifact_stores` | Where model files go, by URL scheme | `file` (`LocalArtifactStore`); a third party adds a scheme here |
| `ubunye.backends` | Execution backends ([Execution backends](backends.md)) | `spark`, `databricks`, `pandas` |
| `ubunye.readers` | Readers (`format:` of an input) | `hive`, `jdbc`, `s3`, `delta`, `binary`, `unity`, `rest_api` |
| `ubunye.writers` | Writers (`format:` of an output) | `s3`, `delta`, `hive`, `jdbc`, `unity`, `rest_api` |
| `ubunye.transforms` | Transforms | `noop`, `model` |
| `ubunye.ml` | Model wrappers | `sklearn`, `sparkml` |
| `ubunye.monitors` | Monitors (older than hooks) | `mlflow`, `lineage` |
| `ubunye.hooks` | Hooks, always on | `events`, `otel`, `prometheus` |
| `ubunye.secrets` | Secret providers for `secret://` ([Secrets](config/secrets.md)) | `env`, `file`, `databricks`, `aws-sm`, `gcp-sm`, `azure-kv` |
| `ubunye.llm_backends` | Model call providers ([LLM calls](patterns/llm.md)) | `anthropic`, `openai_compatible`, `databricks_serving` |

How to write and register readers, writers and transforms:
[Writing a plugin](connectors/plugin_guide.md).

---

## Fallback manifests and `ubunye sync`

When a metadata write fails (a run record, or a model registry update), the run
goes on, and the record is appended to a local **fallback manifest**:

```
~/.ubunye/fallback/{run_id}/{kind}.jsonl
```

`kind` is `lineage` or `registry`. Each line is a JSON object with all record
fields plus a `_fallback_at` timestamp. The run logs a warning that says so.

Replay them once the store is back:

```bash
ubunye sync lineage                    # replay all run record manifests
ubunye sync lineage --run-id abc123    # replay one run
ubunye sync lineage --lineage-backend delta --lineage-dir main.ops.ubunye_runs
ubunye sync registry                   # replay all registry manifests
```

Replay skips records already written (the key is `run_id + task + recorded_at`),
so running it twice is safe. Replayed manifests are moved to
`~/.ubunye/fallback/synced/`. Every flag: [`ubunye sync`](cli.md#ubunye-sync).

---

## Writing your own

1. Implement the protocol methods (no base class needed).
2. Register the entry point in your `pyproject.toml`:

    ```toml
    [project.entry-points."ubunye.lineage_backends"]
    my_store = "my_package.lineage:MyRunRecordStore"
    ```

3. Install the package (`pip install -e .`).
4. The engine finds it by name. A run record store is then used with
   `--lineage-backend my_store` (or `UBUNYE_LINEAGE_BACKEND=my_store`), and
   built with one argument, `--lineage-dir` as given:

    ```python
    from ubunye.lineage import stores

    store = stores.open_store("my-location", "my_store")
    ```

   A registry backend is used with `ubunye models --backend my_registry` (or
   `UBUNYE_REGISTRY_BACKEND=my_registry`).
