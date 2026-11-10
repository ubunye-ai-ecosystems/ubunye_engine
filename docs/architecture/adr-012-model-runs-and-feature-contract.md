# ADR 012: Model runs on the receipt; features checked at predict

**Status:** accepted, 0.9.0

## Context

A real bank project with 14 ML notebooks had to build two things around the
engine by hand.

1. **The link from a run to its model.** The registry stores the run id with each
   model version (`lineage_run_id`), so a model points at its run. The run record
   did not point back. Reading a training run, you could not tell which version it
   registered, whether it was promoted, or which gate stopped it. Reading a
   scoring run, you could not tell which version scored the rows. A blocked
   promotion was an exception and nothing else: once the job failed, the gate and
   the metric that blocked it were only in a log.
2. **The feature contract at predict.** A model declares its features in
   `metadata()["features"]`, and the registry stores them with the version. Nothing
   used them. A frame missing a column reached the model and failed deep inside
   it (a pandas `KeyError`, a Spark analysis error), or worse, a model that fills
   gaps scored it.

## Decision

**The run record gets `model_runs`**, a list with one entry per train or predict a
`ModelTransform` does in the run. The engine opens a recording around the
transforms (like `llm_calls`); the transform adds to it; the recorder writes it.
An entry is plain JSON, so every run record store keeps it unchanged (the Delta
store keeps the whole record as JSON). The record version stays 2: the field is
additive and an old record reads as `[]`.

| Key | Train | Predict |
|---|---|---|
| `action`, `model`, `model_class`, `use_case` | yes | yes |
| `version` | the version registered (none without a registry) | the version used |
| `stage` | the stage it reached; `development` when not promoted or blocked | the stage it was loaded from |
| `registered`, `metrics`, `promote_to` | yes | |
| `gate_result` | `passed`, `blocked`, `no gates`, or none (no promotion asked) | |
| `gates` | every gate checked: gate, metric, passed, actual, threshold, message | |
| `blocked_by`, `error` | the failed gates (gate, metric, actual, threshold) and the error | |
| `model_path`, `features` | | the path (no registry); declared and missing features |

**A blocked promotion is recorded, then behaves as before.** It still raises
`PromotionBlockedError` and fails the run, and the failed run's record says which
gate blocked it. `registry.on_blocked: record` keeps the run going instead (the
version stays in `development`, a warning is logged).

**Scored rows can say which model scored them.** `registry.stamp_columns: true`
adds `model_version` and `model_stage` columns after `predict()`, on pandas and on
Spark (`withColumn(lit(...))`). It is off by default, so no output changes. It
refuses to overwrite a column `predict()` already made.

**Features are checked before the model is called.** Only the names: the
registry stores `features` as a list of names and no dtypes, and this ADR adds no
new storage format, so there is no dtype check. Extra columns are fine.
`registry.check_features`:

- `true`: a frame without a declared feature is refused with
  `FeatureContractError` (a `KeyError`), naming the missing columns, the model,
  the version and stage, and what to try.
- not set (the default): the missing columns are logged as a warning and put on
  the record, and the model is called as before. If it then fails, its own error
  is raised with a note naming the missing columns (Python 3.11 and later).
- `false`: no check at all.

A model with no declared features is never checked and works exactly as before.

### Why the default does not refuse

Refusing by default is not safe. A model may declare the features it builds
inside `predict()` from raw columns: an sklearn model's `feature_names_in_` are the
columns it was fitted on, which are often derived in `train()` and again in
`predict()`. Such a model works today with a frame that lacks those names, and a
refusing default would break it. The default keeps every working model working,
still puts the evidence on the record, and turns a deep failure into one that
names the columns. A team that wants the hard check sets `check_features: true`.

## Consequences

- `ubunye lineage show` prints `model_runs` with the record; `ubunye lineage trace`
  prints a MODELS section, one line per model.
- The lineage conformance suite's reference record sets `model_runs`, so every
  run record store (filesystem, Delta, third party) must keep it.
- The core runtime imports `ubunye.models.receipt` (plain Python, no engine) to
  open the recording; the core still imports no engine.
- Only `ModelTransform` records. A task that calls `ModelRegistry` itself is not on
  the record; it can call `ModelTransform().apply(...)` from its `transformations.py`
  to get the record, the stamped columns and the feature check.
- No registry backend changes: the gate result is evaluated from the same gates
  `promote()` uses, and the registry's own check still decides.
