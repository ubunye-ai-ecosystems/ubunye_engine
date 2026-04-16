# task-04 — Add a multi-task DAG example (task dependencies)

**Priority:** medium — coverage gap flagged during fire-test planning.

## Why

All four current examples run a single task. The engine's
`run_pipeline()` accepts a list of tasks but we have no reference for a
DAG where task B's input is task A's output (feature engineering → train
is the obvious shape).

## Suggested shape

- `examples/production/churn_pipeline_databricks/`
- Three tasks: `ingest_raw` → `feature_engineering` → `train_classifier`.
- Task B reads the UC table written by task A; task C reads B's.
- One Databricks Asset Bundle with job dependencies wired via
  `depends_on`.
- CI runs all three in sequence on a nonprod target.

## Questions to answer

- Does the Spark app-name convention still read cleanly across tasks?
- Does `ubunye lineage trace` correctly stitch cross-task provenance?
- What breaks if task B starts before task A's Delta commit is visible?
