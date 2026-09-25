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
| `backends` | List the execution backends and what each can do |
| `doctor` | Check this machine, and optionally tasks, before a run |
| `gate` | Fail a pull request when a run receipt regresses ([Gate](patterns/gate.md)) |
| `mcp` | Serve the engine to agents over MCP ([Agents over MCP](patterns/mcp.md)) |
| `plugins` | List all discovered plugins |
| `config` | Show the expanded (Jinja-rendered + validated) config |
| `version` | Print the engine version |

### Sub-command groups

| Group | Description |
|---|---|
| `lineage` | Inspect run provenance records |
| `models` | Manage model versions and lifecycle |
| `test` | Run tasks in test mode and report PASS/FAIL |
| `sync` | Replay fallback manifests against configured backends |
| `deploy` | Deploy pipelines to remote environments |
| `export` | Export orchestration artifacts (Airflow, Databricks) |

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
| `--var` | | Extra template variable, `key=value`, repeatable (`run`, `validate`, `plan`, `config`, `test run`). See [Jinja](config/jinja.md#cli-injected-variables) |
| `--json` | | Print one JSON document to stdout, errors included, for scripts and agents (`plan`, `validate`, `backends`, `lineage *`, `models list/info/compare`). The exit code still says whether it worked |

---

## `ubunye init`

Scaffold a task folder that runs straight away.

```bash
ubunye init -d pipelines -u demo -p starter -t filter_adults
```

Creates:

```
pipelines/demo/starter/filter_adults/
    config.yaml            reads data/people.csv, writes output/adults as Parquet
    transformations.py     keeps people aged 18 and over
    data/people.csv        a small sample to start from
    notebooks/
        filter_adults_dev.ipynb
```

and prints what to run next. The transform, `people[people["age"] >= 18]`, means
the same thing in pandas and in Spark, so the task runs with no Java
(`--backend pandas`) and on Spark unchanged. Its paths use `{{ task_dir }}` (the
task's own folder), so it runs from any folder.

`--template databricks` writes the older scaffold instead: read a Unity Catalog
table, write to an `s3a://` bucket. `ubunye init pipeline ...` is the same
command with a subcommand name.

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | — | Root directory |
| `--usecase` | `-u` | yes | — | Use-case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) to scaffold (repeatable) |
| `--template` | | no | `local` | `local` or `databricks` |
| `--overwrite` | | no | `no-overwrite` | Overwrite existing files |

!!! note "Changed in 0.7.0"
    `ubunye init -d ...` now works as written (it needed `ubunye init pipeline`),
    and the default scaffold is the local one above.

---|---|---|---|---|
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
| `--backend` | | no | — | Also check every input and output against what this backend can do, without starting it |

Check a task can run on a backend before running it:

```bash
ubunye validate -d pipelines -u fraud_detection -p ingestion -t claim_etl --backend pandas
```

---

## `ubunye plan`

A dry run: what each task will read and write, and what will stop it. It starts
no engine and moves no data, and exits `1` if it finds a problem.

```bash
ubunye plan -d pipelines -u titanic -p pipeline -t clean_data -t aggregate --backend pandas
```

```text
Task titanic/pipeline/clean_data   (backend: pandas)
  Inputs
    titanic      s3 csv         file:///.../data/titanic.csv
                 found: 1 file(s), 60,302 bytes
  Transform  CleanData  (transformations.py)
  Outputs
    cleaned      s3 parquet     file:///.../output/cleaned/dt=latest
                 mode overwrite

Task titanic/pipeline/aggregate   (backend: pandas)
  Inputs
    cleaned      s3 parquet     file:///.../output/cleaned/dt=latest
                 written by task 'clean_data' earlier in this plan
  ...
Plan OK: 2 task(s) can run.
```

It checks:

- each local input exists, or is written by an earlier task in the same plan
  (list the tasks in the order they run);
- the transform class loads from `transformations.py`;
- every write mode resolves (`merge` without `merge_keys` is caught here, not
  on a cluster after the transform ran);
- with `--backend`, that the backend can do what each input and output needs;
- which environment variables the config uses but are not set (a warning).

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | — | Root directory |
| `--usecase` | `-u` | yes | — | Use-case name |
| `--package` | `-p` | yes | — | Package name |
| `--task-list` | `-t` | yes | — | Task(s) to plan, in run order (repeatable) |
| `--data-timestamp` | `-dt` | no | — | Data timestamp |
| `--data-timestamp-format` | `-dtf` | no | — | Timestamp format |
| `--mode` | `-m` | no | `DEV` | Run mode |
| `--var` | | no | — | Extra template variable, `key=value` (repeatable) |
| `--backend` | | no | — | Also check against what this backend can do |

!!! note "Changed in 0.7.0"
    `plan` used to print the config's names back and always exit `0`. It now
    checks the task and exits `1` when something would stop the run, so it can
    guard a CI job or an agent before a real run.

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
| `--backend` | | no | platform, else `spark` | Execution backend by name, e.g. `pandas` for a run with no Java. See [Execution backends](backends.md) |

!!! note
    `run` picks the profile with `-m/--mode` (it has no `--profile`), and `--all`
    runs every task in the package. The mode must match a profile name exactly.

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
| `--backend` | | no | platform, else `spark` | Execution backend by name, e.g. `pandas` to test with no Java |

---

## `ubunye backends`

List the execution backends that are installed and what each can do: its
features, file formats, write modes, and whether it is distributed or needs
Java. The default is marked.

```bash
ubunye backends
ubunye backends --json
```

A backend that is registered but cannot load (a missing dependency) is listed
with the reason. See [Execution backends](backends.md).

---

## `ubunye doctor`

Check this machine before a run: what is installed, what will fail, and why.

```bash
ubunye doctor
ubunye doctor -d pipelines -u fraud -p ingestion -t claims   # also check a task
ubunye doctor --json
```

It checks:

- the Python version (tested: 3.10 to 3.13) and the engine version;
- every backend: usable here, or what it needs and the command to install it;
  and whether a run without `--backend` would work;
- for Spark: that Java is found and suits the installed Spark (Spark 4 needs
  Java 17 or 21, Spark 3.5 needs 8, 11 or 17), that `delta-spark` is built for
  the same Spark major, and on Windows that `HADOOP_HOME` points at `winutils.exe`;
- that every reader, writer, transform and hook plugin loads;
- for each task named with `-t`: the `{{ env.X }}` variables it uses without a
  default that are not set, and whether its config loads and validates.

Each line is `[OK]`, `[WARN]` or `[FAIL]`, with a fix under anything that is not
OK. A warning is about the environment and matters only for what you use (Spark
without Java is fine if you run on pandas). A failure means a run would fail: no
backend can run here, or a named task's config cannot load. Any failure makes
the exit code 1, so `ubunye doctor` can gate a script or a CI step.

```text
[OK] python: Python 3.13.6 on win32
[OK] backend: pandas: usable, single machine
[WARN] backend: spark (default): needs pyspark
       fix: pip install 'ubunye-engine[spark]'
[WARN] default backend: a run without --backend uses 'spark', which cannot run here
       fix: Pass --backend pandas, or install what 'spark' needs.
[FAIL] task claims: environment: not set, and used without a default: CLAIMS_ROOT
       fix: Set them, or give each a default: {{ env.NAME | default('value') }}.
```

---

## Machine output: `--json`

For a script, a CI step or an AI agent, `--json` makes a command print exactly
one JSON document on stdout and nothing else. Errors are JSON too
(`{"ok": false, "error": "..."}`), and the exit code is unchanged, so a caller
can check either.

```bash
ubunye plan -d pipelines -u sales -p etl -t daily --json | jq '.ok'
ubunye validate -d pipelines -u sales -p etl --all --backend pandas --json
ubunye lineage compare -d pipelines -u sales -p etl -t daily --run-id1 A --run-id2 B --json
```

| Command | JSON shape |
|---|---|
| `plan` | `{"ok": bool, "tasks": [plan, ...]}`: each plan has `inputs`, `transform`, `outputs`, `problems`, `warnings`, `config_hash` |
| `validate` | `{"ok": bool, "tasks": [{"task", "ok", "problems"}]}` |
| `backends` | `[{"name", "default", "loaded", "capabilities"}]` |
| `lineage list`, `lineage search` | `[run record, ...]` (an empty list when there are none) |
| `lineage show` | one run record |
| `lineage compare` | per field `{"a", "b", "changed"}`; per output `data_hash.state` is `unchanged`, `changed`, `unknown` or `not comparable` |
| `lineage trace` | `{"task", "run_id", "status", "inputs", "transform", "outputs"}` |
| `models list`, `models info` | model version(s), with `stage` as text |
| `models compare` | `{metric: {"a", "b", "delta"}}` |

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

Diff two run records: status, config, code and environment hashes (naming the
packages whose versions changed), and each input's and output's row count,
schema hash and data hash.

```bash
ubunye lineage compare \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl \
    --run-id1 <id1> --run-id2 <id2>
```

A data hash is compared only when both runs have one made the same way.
Two missing hashes show as "unknown", and a record from before 0.7.0 (which
hashed a sample) shows as "not comparable" with a newer one. See
[the run record](architecture/adr-006-run-record.md).

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

The version must pass the model's promotion gates first: the `promotion_gates`
its training run registered it with (for example `min_auc: 0.85`). If one fails,
nothing is promoted and the failing gate is named. `--force` promotes anyway,
prints which gates were skipped, and marks the version (`promotion_forced`) so
the skip stays on the record.

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

---

## `ubunye sync`

Replay fallback manifests against configured backends.  When metadata
writes fail during pipeline execution (Decision 2), records are appended
to local JSONL manifests under `~/.ubunye/fallback/`.  This command
replays them with idempotent deduplication.

### `sync lineage`

```bash
ubunye sync lineage                    # replay all lineage manifests
ubunye sync lineage --run-id abc123    # replay a specific run
```

### `sync registry`

```bash
ubunye sync registry                   # replay all registry manifests
ubunye sync registry --run-id abc123   # replay a specific run
```

| Flag | Required | Default | Description |
|---|---|---|---|
| `--run-id` | no | — | Replay only this run's manifest |

Processed manifests are archived to `~/.ubunye/fallback/synced/`.

---

## `ubunye deploy`

Deploy a task to a remote environment.

### `deploy databricks`

```bash
ubunye deploy databricks \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl \
    --target nonprod

ubunye deploy databricks \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl \
    --target nonprod \
    --dry-run
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | — | Root pipelines directory |
| `--usecase` | `-u` | yes | — | Use-case name |
| `--package` | `-p` | yes | — | Package name |
| `--task` | `-t` | yes | — | Task to deploy |
| `--target` | | yes | — | Deployment target from `targets.yaml` |
| `--dry-run` | | no | false | Preview job spec without deploying |
| `--data-timestamp` | `-dt` | no | — | Data timestamp |
| `--mode` | `-m` | no | `PROD` | Run mode |

---

## `ubunye export`

Generate orchestration artifacts from a task's config.

### `export airflow`

```bash
ubunye export airflow \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl \
    --output dag_fraud_etl.py
```

### `export databricks`

```bash
ubunye export databricks \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl \
    --output job_fraud_etl.json
```
