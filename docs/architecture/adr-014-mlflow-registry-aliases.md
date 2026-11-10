# ADR 014: The MLflow registry backend uses aliases

**Status:** accepted, 0.9.0

## Context

The `mlflow` registry backend did not use the MLflow Model Registry. It kept
every model in a folder (the filesystem registry) and, on register and promote,
logged a best effort MLflow run with params and metrics. No registered model,
no model version, no alias. A team on MLflow could not load a Ubunye model with
`models:/...`, and the run logging failed silently.

MLflow's fixed stages (`Staging`, `Production`, `Archived`) are deprecated, and
Unity Catalog has none: only aliases and tags.

## Decision

`mlflow` (`MLflowModelRegistryBackend`) keeps models in the MLflow Model
Registry and implements only the six protocol methods (ADR 010). Rollback,
archive, compare and one production version come from the lifecycle layer.

- Where MLflow is comes from MLflow itself (`MLFLOW_TRACKING_URI`,
  `MLFLOW_REGISTRY_URI`). The store string is the experiment that holds one run
  per version; the run's `model` artifact is the saved model and the version's
  source.
- A Ubunye model is the registered model `{use_case}.{model_name}`, or the
  template in `UBUNYE_MLFLOW_MODEL_NAME`.
- A Ubunye version is a model version. Its Ubunye version string, stage,
  metrics, metadata, gates and lineage run id are version tags under `ubunye.`.
  MLflow's own version numbers stay MLflow's.
- The stage tag is the truth for every version. `staging` and `production` are
  also aliases, held by the version moved there last. When that version leaves
  the stage, the alias moves to another version still in it, or is removed.
  `development` and `archived` have no alias: many versions sit in them.
- The stage API (`transition_model_version_stage`, `get_latest_versions`) is
  never called. The tests forbid it.

The old class stays as `MLflowRegistryBackend`, works as before when built in
Python, and warns that it is deprecated. The `mlflow` entry point name became
reachable only in 0.9 (ADR 010), so no released config changes meaning.

## Consequences

- `models:/<name>@production` works for any MLflow client, serving included.
- The conformance suite runs against a local MLflow (SQLite and a file
  artifact store), no server.
- Unity Catalog should work by naming the model `catalog.schema.model` through
  `UBUNYE_MLFLOW_MODEL_NAME`. Not tried on a workspace yet. Unity Catalog also
  wants an `MLmodel` file with a signature in the source; Ubunye's saved models
  have none, so registering there may be refused until the artifact carries one.
- Every call lists the model's versions (paged), fine for the tens of versions a
  model has.
- Like ADR 010, the alias moves are a read then a write, not atomic.
