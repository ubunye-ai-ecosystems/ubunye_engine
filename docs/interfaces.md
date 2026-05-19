# Interfaces (Protocol APIs)

Ubunye Engine defines four formal `typing.Protocol` interfaces for its
pluggable seams.  Any class that implements the required methods satisfies
the protocol — no inheritance needed.

Implementations are discovered at runtime via
[entry-point groups](https://packaging.python.org/en/latest/specifications/entry-points/)
and environment-based auto-detection.

---

## Design principles

Three production-policy decisions shape how every interface behaves:

### Decision 1 — Non-blocking metadata writes

`record()`, `register()`, and `promote()` dispatch their metadata updates
to a **background worker thread** with a bounded queue (default 1 000,
configurable via `UBUNYE_METADATA_QUEUE_SIZE`).

- Queue overflow: drop oldest pending write, log a warning.
- Shutdown: `flush()` drains with a configurable timeout (default 30 s,
  `UBUNYE_METADATA_FLUSH_TIMEOUT`).
- Artifact writes (model files) remain synchronous — the method returns
  only after the model is safely stored.

Uses a worker-thread pattern, not `async`/`await` — async fights Spark's
threading model in Python.

### Decision 2 — Graceful degradation

When a metadata write fails, the record is appended to a local **fallback
manifest** at `~/.ubunye/fallback/{run_id}/{kind}.jsonl`.  Pipeline
execution continues.  `ubunye sync lineage` (or `ubunye sync registry`)
replays the manifest with idempotent deduplication (key:
`run_id + task + recorded_at`).

**Auth is excluded** — authentication failures always propagate
immediately.  A pipeline must not execute with invalid credentials.

### Decision 3 — Schema evolution

Every cross-boundary dataclass separates **strict core fields** from a
flexible `metadata: Dict[str, str]`:

- Core fields never change without a major engine version bump.
- New data goes into `metadata` — old clients ignore unknown keys, new
  clients write new keys without touching the schema.
- Every record stamps `engine_version` so downstream consumers can filter
  by writer version.

---

## Protocols

### DeployAdapter

Deploys a task's config and artifacts to a remote environment.

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

**Built-in**: `DatabricksDeployAdapter` — entry point `ubunye.deploy_adapters:databricks`.

---

### RegistryBackend

Model lifecycle management: register, promote, demote, delete.

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

- `FilesystemRegistryBackend` — entry point `ubunye.registry_backends:filesystem`.
- `MLflowRegistryBackend` — entry point `ubunye.registry_backends:mlflow`.
  Layers MLflow experiment/run logging on top of filesystem storage.
  MLflow logging is best-effort — never blocks the pipeline.

---

### LineageBackend

Run provenance: record, search, compact.

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

**Built-in**:

- `FileSystemLineageStore` — entry point `ubunye.lineage_backends:filesystem`.
  Uses a `run_id→Path` index for O(1) lookups and an in-memory
  `RunContext` cache to avoid repeated JSON parsing on search.
- `DeltaLineageBackend` — entry point `ubunye.lineage_backends:delta`.
  Writes to a Delta table via Spark SQL on Databricks; falls back to
  JSONL files locally.

---

### AuthBackend

Credential resolution and validation.

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

- `TokenAuthBackend` — entry point `ubunye.auth_backends:token`.
- `ServicePrincipalAuthBackend` — entry point `ubunye.auth_backends:service_principal`.
  Uses `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` for OAuth M2M auth.

---

## Discovery and auto-detection

### Entry-point groups

| Group | Built-in name | Class |
|---|---|---|
| `ubunye.deploy_adapters` | `databricks` | `DatabricksDeployAdapter` |
| `ubunye.registry_backends` | `filesystem` | `FilesystemRegistryBackend` |
| `ubunye.registry_backends` | `mlflow` | `MLflowRegistryBackend` |
| `ubunye.lineage_backends` | `filesystem` | `FileSystemLineageStore` |
| `ubunye.lineage_backends` | `delta` | `DeltaLineageBackend` |
| `ubunye.auth_backends` | `token` | `TokenAuthBackend` |
| `ubunye.auth_backends` | `service_principal` | `ServicePrincipalAuthBackend` |

Third-party packages register backends by adding entry points under these
groups in their `pyproject.toml`.

### Auto-detection

When no backend is explicitly configured, the engine probes the
environment:

| Seam | Probe | Result |
|---|---|---|
| Registry | `DATABRICKS_RUNTIME_VERSION` + `mlflow` importable | `"mlflow"` |
| Registry | Otherwise | `"filesystem"` |
| Lineage | `UBUNYE_LINEAGE_TABLE` env var set | `"delta"` |
| Lineage | Otherwise | `"filesystem"` |
| Auth | `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` | `"service_principal"` |
| Auth | `DATABRICKS_TOKEN` | `"token"` |
| Auth | None | Raises `AuthNotFoundError` |

Service principal takes priority over token when both are set.

---

## Fallback manifests and `ubunye sync`

When a metadata write fails (Decision 2), the record is appended to:

```
~/.ubunye/fallback/{run_id}/{kind}.jsonl
```

Each line is a JSON object with all record fields plus a `_fallback_at`
timestamp.

Replay with:

```bash
ubunye sync lineage                    # replay all lineage manifests
ubunye sync lineage --run-id abc123    # replay a specific run
ubunye sync registry                   # replay all registry manifests
```

Deduplication key: `run_id + task + recorded_at`.  Replayed manifests are
archived to `~/.ubunye/fallback/synced/`.

---

## Writing a custom backend

1. Implement the protocol methods (no base class needed).
2. Register the entry point in your `pyproject.toml`:

    ```toml
    [project.entry-points."ubunye.lineage_backends"]
    my_backend = "my_package.lineage:MyLineageBackend"
    ```

3. Install the package (`pip install -e .`).
4. The engine discovers it automatically:

    ```python
    from ubunye._internal.discovery import get_lineage_backend

    cls = get_lineage_backend("my_backend")
    store = cls()
    ```
