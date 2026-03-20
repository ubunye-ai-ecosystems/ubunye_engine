# CLI Reference

The Ubunye CLI (`ubunye`) is built with [Typer](https://typer.tiangolo.com/).
All commands accept `--help` for full option details.

---

## Top-level commands

| Command | Description |
|---|---|
| `init` | Scaffold a new task folder |
| `validate` | Validate a config file without running |
| `plan` | Print the execution plan |
| `run` | Execute one or more tasks |
| `plugins` | List all discovered plugins |
| `config` | Show the expanded (Jinja-rendered + validated) config |
| `version` | Print the engine version |

### Sub-command groups

| Group | Description |
|---|---|
| `lineage` | Inspect run provenance records |
| `models` | Manage model versions and lifecycle |
| `test` | Run tasks in test mode and report PASS/FAIL |

---

## Common flags

Most commands share a set of path and variable flags:

| Flag | Short | Description |
|---|---|---|
| `--usecase-dir` | `-d` | Root pipelines directory |
| `--usecase` | `-u` | Use-case name |
| `--package` | `-p` | Package / pipeline name |
| `--task-list` | `-t` | Task name(s) — repeatable |
| `--data-timestamp` | `-dt` | Data timestamp, injected as `{{ dt }}` in Jinja |
| `--data-timestamp-format` | `-dtf` | Timestamp format, injected as `{{ dtf }}` |
| `--mode` | `-m` | Engine profile / run mode (default: `DEV`) |

---

## `ubunye init`

Scaffold a new use-case / pipeline / task directory.

```bash
ubunye init \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl
```

Creates:

```
pipelines/fraud_detection/ingestion/claim_etl/
    config.yaml
    transformations.py
    notebooks/
        claim_etl_dev.ipynb    ← interactive dev notebook (Databricks-ready)
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | — | Root directory |
| `--usecase` | `-u` | yes | — | Use-case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) to scaffold (repeatable) |
| `--overwrite` | | no | `no-overwrite` | Overwrite existing files |

---

## `ubunye validate`

Render Jinja and validate the config schema. Exits `0` on success, `1` on error.

```bash
ubunye validate \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl \
    -dt 2024-06-01
```

Validate all tasks in a package:

```bash
ubunye validate -d pipelines -u fraud_detection -p ingestion --all
```

Validate against a specific profile:

```bash
ubunye validate -d pipelines -u fraud_detection -p ingestion -t claim_etl --profile dev
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | — | Root directory |
| `--usecase` | `-u` | yes | — | Use-case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | no | — | Task(s) to validate (repeatable) |
| `--all` | | no | false | Validate all tasks in the package |
| `--profile` | | no | — | Profile to validate against (e.g. dev, prod) |
| `--data-timestamp` | `-dt` | no | — | Data timestamp |

---

## `ubunye plan`

Print the execution plan — inputs, transform, outputs — without running.

```bash
ubunye plan \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl
```

---

## `ubunye run`

Execute one or more tasks sequentially: read inputs, transform, write outputs.

```bash
ubunye run \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl \
    -m PROD \
    --lineage \
    -dt 2024-06-01
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | — | Root pipelines directory |
| `--usecase` | `-u` | yes | — | Use-case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) to execute (repeatable) |
| `--data-timestamp` | `-dt` | no | — | Data timestamp |
| `--data-timestamp-format` | `-dtf` | no | — | Timestamp format |
| `--mode` | `-m` | no | `DEV` | Run mode (DEV/PROD) |
| `--deploy-mode` | | no | `client` | Spark deploy mode (cluster/client) |
| `--lineage` | | no | false | Record lineage for this run |
| `--lineage-dir` | | no | `.ubunye/lineage` | Root directory for lineage records |

!!! note
    The `run` command does not have `--all` or `--profile` flags.
    Use `-m/--mode` for environment switching and list tasks explicitly with `-t`.

---

## `ubunye test run`

Run one or more tasks with a test profile and report PASS/FAIL per task.
Config is validated before Spark starts; invalid configs are reported as `[CONFIG FAIL]`.

```bash
ubunye test run \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | — | Root directory |
| `--usecase` | `-u` | yes | — | Use-case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) to test (repeatable) |
| `--profile` | | no | `test` | Config profile to use |
| `--data-timestamp` | `-dt` | no | — | Data timestamp |
| `--lineage / --no-lineage` | | no | `lineage` | Record lineage (ON by default) |
| `--lineage-dir` | | no | `.ubunye/lineage` | Lineage directory |

---

## `ubunye plugins`

List all readers, writers, transforms, and monitors discovered via entry points.

```bash
ubunye plugins
```

---

## `ubunye config`

Show the fully expanded (Jinja-rendered, Pydantic-validated) config.

```bash
ubunye config \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl \
    -dt 2024-06-01
```

---

## `ubunye version`

```bash
ubunye version
# Ubunye Engine v0.1.0
```

---

## `ubunye lineage`

Inspect run provenance records written by `ubunye run --lineage`.

!!! note
    Lineage sub-commands use `--task` (`-t` singular), not `--task-list`.

### `lineage show`

Show a run record as formatted JSON (latest or specific run).

```bash
ubunye lineage show \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl

ubunye lineage show \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl \
    --run-id <run_id>
```

### `lineage list`

List recent runs for a task (newest first).

```bash
ubunye lineage list \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl

ubunye lineage list \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl -n 20
```

### `lineage compare`

Diff two run records — highlight changes in hashes, row counts, and status.

```bash
ubunye lineage compare \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl \
    --run-id1 <id1> --run-id2 <id2>
```

### `lineage search`

Search all recorded runs across tasks with optional filters.

```bash
ubunye lineage search -d pipelines -t claim_etl
ubunye lineage search -d pipelines --status success --since 2024-06-01
```

### `lineage trace`

Print the input, transform, output data flow graph for a run.

```bash
ubunye lineage trace \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl
```

---

## `ubunye models`

Manage ML model versions and lifecycle.
All sub-commands require: `--use-case` (`-u`), `--model` (`-m`), `--store` (`-s`).

!!! warning
    Note: `models` uses `--use-case` (hyphenated), while other commands use `--usecase`.

### `models list`

```bash
ubunye models list \
    -u fraud_detection -m FraudRiskModel -s .ubunye/model_store
```

### `models info`

```bash
ubunye models info \
    -u fraud_detection -m FraudRiskModel -v 1.3.0 -s .ubunye/model_store
```

### `models promote`

```bash
ubunye models promote \
    -u fraud_detection -m FraudRiskModel -v 1.3.0 \
    --to production --promoted-by alice -s .ubunye/model_store
```

Target stages: `staging`, `production`.

### `models demote`

```bash
ubunye models demote \
    -u fraud_detection -m FraudRiskModel -v 1.3.0 \
    --to staging -s .ubunye/model_store
```

Target stages: `development`, `staging`, `archived`.

### `models rollback`

```bash
ubunye models rollback \
    -u fraud_detection -m FraudRiskModel -v 1.2.0 -s .ubunye/model_store
```

Archives the current production version and restores `--version` to production.

### `models archive`

```bash
ubunye models archive \
    -u fraud_detection -m FraudRiskModel -v 1.1.0 -s .ubunye/model_store
```

### `models compare`

```bash
ubunye models compare \
    -u fraud_detection -m FraudRiskModel \
    --versions 1.2.0 --versions 1.3.0 -s .ubunye/model_store
```
