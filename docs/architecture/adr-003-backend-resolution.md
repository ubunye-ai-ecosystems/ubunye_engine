# ADR 003: One order for choosing a backend; the task never names it

**Status:** accepted, 0.7.0

## Context

The same task folder should run on a laptop, on a cluster and on Databricks.
If `config.yaml` named its engine, the folder would stop being portable. But
something has to decide, and before 0.7.0 the CLI and the Python API decided
differently: the API attached to an active Databricks session, the CLI always
created its own (and stopped it at the end).

## Decision

Every entry point (CLI, `run_task`, `run_pipeline`, `notebook`, `Engine`) picks
the backend in the same order:

1. **Asked for by name:** `--backend pandas`, or `backend="pandas"` (an
   instance is also accepted in the API).
2. **The platform's session:** a backend that finds a session it should attach
   to claims the run. On Databricks the notebook's SparkSession is attached to,
   never created and never stopped.
3. **The default:** `spark`, a new local or cluster session.

`config.yaml` has no backend field, and there is no default-engine setting yet.
The default lives in one named constant (`ubunye.core.backends.DEFAULT_BACKEND`),
so a future engine can become the default by changing one value.

## Consequences

- A Databricks job keeps working with no change: step 2 finds its session.
- On a laptop, `--backend pandas` runs the same folder with no Java.
- `ubunye run` inside a process that already has a SparkSession now attaches
  to it instead of stopping it at the end, as the Python API always did.
