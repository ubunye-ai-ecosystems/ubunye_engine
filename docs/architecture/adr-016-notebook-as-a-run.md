# ADR 016: A notebook's `with` block is one recorded run

**Status:** accepted, 0.9.2

## Context

A team's Databricks job is a notebook. Before 0.9.2 it called
`ubunye.notebook(...)`, then `read()`, `transform()` and `write()`, and the run
record was made only inside `write()`. So a run that failed while reading, in the
transform, in an expectation or in the task's own `setup()` left no record at all.
The job showed red in Databricks and the run record store said nothing.

Turning records on also took three arguments (`lineage=True`, `lineage_dir=`,
`lineage_backend=`) that every notebook had to repeat. On Databricks serverless
there are no environment variables, so the only channel a job has is its job
parameters.

## Decision

1. **`with ubunye.notebook(task_dir, dt=dt) as run:` is one run.** Entering the
   block opens the run in the engine (`Engine.open_run`): the hooks see one task
   from entry to exit. `read`, `transform`, `write` and `run.step(name)` inside
   join it. A clean exit records a success; any exception records a failure.
2. **Every failure names its stage.** The engine keeps `state["stage"]` while it
   works: `read`, `expectations` (input contracts and output rules), `transform`
   (the transform, and computing its outputs), `write`. A step the notebook names
   with `run.step("score")` is a stage too. Code in the block outside any stage is
   `notebook`. A failure before the run could open (config, backend,
   `CONFIG.requires`, the Task's `setup()`) is recorded at `setup`. The stage goes
   into the record as `failed_stage`, one optional field: no new record version.
   `run_task` gets the same stage names, since they come from the engine.
3. **Records turn on from where they go.** A run-record location turns records
   on: an explicit `run_records=` argument, else the job parameter
   `ubunye_run_records`, else `$UBUNYE_RUN_RECORDS`. A table name goes to the
   Delta store, a path to the filesystem store; a store named with
   `lineage_backend=` or `$UBUNYE_LINEAGE_BACKEND` is kept. `lineage=False`
   turns records off. The old `lineage=True, lineage_dir=` works as before.
   `dt` and `mode` left empty come from the job parameters `dt` / `ubunye_mode`.
   Job parameters are read through the job settings reader (ADR 015).
4. **A record that cannot be saved never fails the job.** The Delta store
   creates its table on the first save. When it cannot, the error says what to
   do: create the table once, or grant CREATE TABLE (MODIFY for an append) on the
   schema. The recorder logs that as a warning. The fallback manifest goes to a
   folder only when one is configured (`ubunye_run_records_fallback`, usually a
   Unity Catalog Volume); with none it only warns, because a serverless job's home
   folder dies with it. The old switch keeps the old fallback,
   `~/.ubunye/fallback`.
5. **One line per step.** Each step prints what it did and how long it took, or
   why it failed; the run's last line says where its record went. `quiet=True`
   turns it off. No rows are counted for the line: on Spark that is a job.

## Consequences

- A job notebook is three lines inside a `with`; everything else comes from the
  job.
- A dashboard over the run record table can say which stage fails most, per task.
- The notebook without `with` keeps its 0.9.1 behaviour: a record at each write.
- The notebook now checks `CONFIG.requires` (it never did) against the same
  store, when records are on; without a store it warns that it did not check.
