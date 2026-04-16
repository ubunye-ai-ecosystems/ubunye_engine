# Ubunye Engine: A Config-First Framework for Portable Spark Pipelines with CI-Enforced Runtime Contracts

## Target venue

SoftwareX (primary) or arXiv preprint for early visibility.
Framing: systems / experience report, not novel algorithm.

## Abstract sketch

Config-driven ETL/ML frameworks promise portability but rarely enforce it.
We present Ubunye Engine, a Spark-native framework where pipeline logic is
defined in thin Python transforms while all infrastructure concerns
(connectors, scheduling, profiles) live in validated YAML. The framework's
key contribution is a CI-enforced portability contract: the same
`transformations.py` runs byte-identically across local, notebook, and
Databricks runtimes, with `ubunye validate` in CI guaranteeing config
resolution parity across profiles before merge. We report on a fire-test
sprint that took four production examples from local to Databricks
serverless + Unity Catalog, documenting the bugs found, their root causes,
and the design decisions that prevented or caused them.

---

## 1. Introduction (1.5 pages)

- The laptop-to-production gap in data engineering
- Why existing frameworks leave portability as a documentation promise
- Contribution: config-first design + CI-enforced contract + empirical fire-test data

## 2. Design and Architecture (3 pages)

### 2.1 Config-first design stack

- YAML → Jinja2 rendering → Pydantic v2 validation → Engine dispatch
- Why config-first over code-first (Dagster, Prefect) or SQL-first (dbt)
- Trade-offs: expressiveness vs. guardrails

### 2.2 Plugin registry

- Entry-point-based discovery (Readers, Writers, Transforms, Hooks)
- How third-party packages extend the engine without modifying it

### 2.3 Unified Task/Transform dispatch

- User `Task` classes wrapped as ephemeral `Transform` plugins
- Single Engine code path for CLI, Python API, and notebook callers
- `_with_task_dir_on_path` and `sys.modules` isolation (the bug that motivated this)

### 2.4 Hook abstraction for observability

- `Hook` base class, `HookChain` multiplexer
- Decouples the engine from telemetry (OTel, Prometheus, events, legacy monitors)

### 2.5 ML lifecycle as a first-class CLI surface

- `UbunyeModel` contract, `ModelRegistry`, `PromotionGate`
- Comparison with external-tool delegation (MLflow-only, W&B-only)

## 3. The Portability Contract (2 pages)

### 3.1 What the contract says

- `transformations.py` is byte-identical across runtimes
- Only `config.yaml` and deployment wrappers change
- `ubunye validate` in CI enforces config resolution parity

### 3.2 How the contract is enforced

- CI diff step: `diff -q` between local and Databricks copies
- Jinja resolver + Pydantic validation as a pre-merge gate
- Post-render residue scan (the `DebugUndefined` silent pass-through bug)

### 3.3 What the contract does not cover

- Spark version skew, driver/executor resource differences
- UC vs. DBFS storage semantics (Delta commit visibility)
- Known CE limitations (no service principals, restricted Jobs API)

## 4. Empirical Study: Fire-Test Sprint (3 pages)

### 4.1 Method

- Four production examples taken from local to Databricks serverless + UC
- Overnight autonomous audit + CI-triggered workflow runs
- Bug catalogue: symptom, root cause, fix, runtime where surfaced

### 4.2 Bug catalogue

| # | Bug | Root cause | Severity | Runtime |
|---|-----|-----------|----------|---------|
| 1 | Unity writer `DATA_SOURCE_NOT_FOUND` | `cfg["format"]` is plugin key, not Spark format | High | Databricks serverless |
| 2 | Sibling modules leak between tasks | `sys.modules` cache keyed on short name | High | Any (multi-task) |
| 3 | Undefined CLI vars pass through silently | `DebugUndefined` leaves `{{ var }}` verbatim | High | Any |
| 4 | `transformations.py` import fails | `sys.path` not extended before `_load_task_class` | High | Any |
| 5 | Node 20 deprecation in GitHub Actions | `actions/checkout@v4` uses Node 20 | Low | CI |

### 4.3 Patterns observed

- Config-level bugs (1, 3) caught by validation tightening, not runtime
- Import-level bugs (2, 4) invisible in single-task pipelines
- The portability contract caught zero bugs — it worked as designed

### 4.4 Threats to validity

- Small example count (4), single Databricks workspace
- Overnight autonomous session — bugs found by audit, not organic usage
- No multi-tenant or high-concurrency testing

## 5. Related Work (2 pages)

### Comparison matrix

| Framework | Config/Code | Portable | Plugins | ML lifecycle | CI contract |
|-----------|-------------|----------|---------|-------------|-------------|
| Kedro | Code (YAML catalog) | Partial | Yes | No | No |
| Flyte | Code | Partial (K8s) | Yes | Partial | No |
| Metaflow | Code | Partial (AWS/Azure) | No | Partial | No |
| Dagster | Code | Partial | Yes | No | No |
| Prefect | Code | Partial | Partial | No | No |
| dbt | Config (SQL) | Locked | Yes | No | No |
| Mage | Hybrid | Partial | Partial | No | No |
| Apache Beam | Code | Yes | Yes | No | No |
| **Ubunye** | **Config** | **Yes** | **Yes** | **Yes** | **Yes** |

### Key differentiators to argue

1. Only framework combining config-first + runtime portability + CI enforcement
2. ML lifecycle as CLI commands, not external tool delegation
3. Beam is portable but code-first; dbt is config-first but runtime-locked
4. Hook abstraction decouples observability — surveyed frameworks embed it

## 6. Limitations and Future Work (1 page)

- No streaming support (batch-only; design doc needed before schema change)
- No native DAG scheduler (delegates to Airflow/Databricks Asset Bundles)
- Plugin discovery via entry points requires pip install — no dynamic hot-load
- Model Registry is filesystem-backed — no distributed locking
- Fire-test sprint covered CE/serverless; dedicated clusters untested

## 7. Conclusion (0.5 pages)

---

## Estimated length

~13 pages (SoftwareX format). Could compress to 8 for a demo/industry track.

## Evidence to gather before drafting

- [ ] Exact line counts for config vs. code across the 5 production examples
- [ ] CI run times for validate vs. full pipeline
- [ ] User's writing style from MSc thesis (voice sheet)
