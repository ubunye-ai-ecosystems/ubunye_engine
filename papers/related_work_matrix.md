# Related Work Comparison Matrix

| Framework | Config/Code-first | Runtime portable? | Plugin connectors | ML lifecycle | CI portability contract |
|-----------|-------------------|-------------------|-------------------|-------------|------------------------|
| **Kedro** | Code (YAML catalog) | Partial — needs adapters for cloud | Yes (DataSets) | No — external MLflow | No |
| **Flyte** | Code (typed tasks) | Partial — needs K8s cluster | Yes (task plugins) | Partial — artifact tracking | No |
| **Metaflow** | Code (decorators) | Partial — AWS/Azure decorators | No formal system | Partial — artifact versioning | No |
| **Dagster** | Code (assets/ops) | Partial — local or Cloud | Yes (IOManagers) | No — external tools | No |
| **Prefect** | Code (decorators) | Partial — local or Cloud | Partial (integrations) | No | No |
| **dbt** | Config (SQL + YAML) | Locked to SQL warehouse | Yes (adapters) | No — analytics only | No |
| **Mage** | Hybrid (GUI + code) | Partial — env differences | Partial (blocks) | No | No |
| **Apache Beam** | Code (SDK) | Yes (DirectRunner, Dataflow, Flink, Spark) | Yes (I/O connectors) | No | No — runner parity varies |
| **Ubunye** | **Config (YAML + thin Python)** | **Yes (local, notebook, Databricks)** | **Yes (entry-point Readers/Writers/Transforms/Hooks)** | **Yes (promote/demote/rollback/archive/compare)** | **Yes (`ubunye validate` + CI diff)** |

## Key observations

1. **Config-first is rare.** Only dbt and Ubunye are truly config-first. dbt is SQL-first (domain-specific); Ubunye is YAML+Python (general-purpose Spark).

2. **Runtime portability is claimed but not enforced.** Beam comes closest to true portability, but has no config-driven contract enforcement. Most frameworks say "runs locally and in the cloud" but the deployment configs, environment variables, and infrastructure differ silently.

3. **ML lifecycle is delegated.** Every framework except Ubunye delegates model management to external tools (MLflow, W&B, SageMaker). Ubunye ships `UbunyeModel`, `ModelRegistry`, `PromotionGate`, and CLI commands as first-class features.

4. **CI enforcement is unique to Ubunye.** No surveyed framework enforces portability at CI time via `validate` + diff steps. The closest analogue is dbt's `dbt compile` + schema tests, but that validates SQL compilation, not runtime portability.
