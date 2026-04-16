# task-05 — Multi-task pipeline example (local)

**Status:** done
**Commit:** `aab60c4`

## What was done

Built `examples/production/titanic_multitask_local/` — a two-task pipeline
demonstrating sequential task chaining:

1. `clean_data`: reads Titanic CSV, drops nulls, adds `survived_label` and
   `age_group` columns, writes intermediate Parquet.
2. `aggregate`: reads the intermediate Parquet, computes survival rates by
   class and age group, writes summary Parquet.

Exercises `run_pipeline()`, sibling-module isolation between tasks (the fix
from commit `1732d25`), and cross-task lineage.

Includes: Spark unit tests, CI workflow (`.github/workflows/multitask_local.yml`),
README, changelog entry, and production examples index update.

## Notes

- CI workflow can't be triggered via `workflow_dispatch` until merged to
  `main` (GitHub only discovers new workflows on the default branch).
- The Databricks multi-task variant (task-04) is still in `tasks/todo/` —
  needs UC tables as intermediate storage.
