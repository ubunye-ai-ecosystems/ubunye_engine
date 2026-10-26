# CLI Reference

The Ubunye CLI (`ubunye`) is built with [Typer](https://typer.tiangolo.com/).
All commands accept `--help` for full option details. `python -m ubunye` runs the
same CLI where there is no console script (a cloud job, for example).

---

## Top-level commands

| Command | Description |
|---|---|
| `validate` | Validate config file(s) without running |
| `plan` | Dry run: what each task will read and write, and what will stop it |
| `run` | Execute one or more tasks |
| `backends` | List the execution backends and what each can do |
| `doctor` | Check this machine, and optionally tasks, before a run |
| `gate` | Fail a pull request when a run receipt regresses ([Gate](patterns/gate.md)) |
| `mcp` | Serve the engine to agents over MCP ([Agents over MCP](patterns/mcp.md)) |
| `plugins` | List the discovered reader, writer and transform plugins |
| `config` | Load and validate config files, printing `[OK]` or the error |
| `version` | Print the engine version |

### Sub-command groups

| Group | Description |
|---|---|
| `init` | Scaffold a task (`ubunye init -d ... -u ... -p ... -t ...`) or a CI workflow |
| `lineage` | Inspect run records |
| `models` | Manage model versions and lifecycle |
| `prove` | The proving ground: compare one workload's run records across environments |
| `test` | Run tasks in test mode and report PASS/FAIL |
| `sync` | Replay fallback manifests against configured backends |
| `deploy` | Run or deploy tasks on Databricks, Glue, Dataproc, Kubernetes, Container Apps, EMR Serverless |
| `export` | Export a task to an orchestrator: Airflow DAG, Databricks job, Spark pipeline |

---

## Common flags

Most commands share a set of path and variable flags:

| Flag | Short | Description |
|---|---|---|
| `--usecase-dir` | `-d` | Root pipelines directory |
| `--usecase` | `-u` | Use-case name |
| `--package` | `-p` | Package / pipeline name |
| `--task-list` | `-t` | Task name(s), repeatable |
| `--data-timestamp` | `-dt` | Data timestamp, injected as `{{ dt }}` in Jinja |
| `--data-timestamp-format` | `-dtf` | Timestamp format, injected as `{{ dtf }}` |
| `--mode` | `-m` | Engine profile / run mode (default: `DEV`) |
| `--var` | | Extra template variable, `key=value`, repeatable (`run`, `validate`, `plan`, `config`, `doctor`, `test run`, `export spark-pipeline`). See [Jinja](config/jinja.md#cli-injected-variables) |
| `--json` | | Print one JSON document to stdout, errors included, for scripts and agents (`plan`, `validate`, `backends`, `doctor`, `gate`, `lineage show/list/compare/search/trace`, `models list/compare`). The exit code still says whether it worked |

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
| `--usecase-dir` | `-d` | yes | | Root directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task-list` | `-t` | yes | | Task(s) to scaffold (repeatable) |
| `--template` | | no | `local` | `local` or `databricks` |
| `--overwrite / --no-overwrite` | | no | `no-overwrite` | Overwrite existing files |

!!! note "Changed in 0.7.0"
    `ubunye init -d ...` now works as written (it needed `ubunye init pipeline`),
    and the default scaffold is the local one above.

### `init github-actions`

Write a GitHub Actions workflow that validates the config and runs tests on pull
requests, then deploys to Databricks on merge to `main`.

```bash
ubunye init github-actions -d pipelines -u fraud -p ingestion -t claim_etl
ubunye init github-actions -d pipelines -u fraud -p ingestion -t claim_etl --no-deploy
ubunye init github-actions -d pipelines -u fraud -p ingestion -t task_a -t task_b --extras ml,dev
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | | Root directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task-list` | `-t` | yes | | Task(s) to include in the deploy steps (repeatable) |
| `--extras` | | no | `spark,dev` | pip install extras, comma-separated |
| `--target` | | no | `nonprod` | Databricks deploy target |
| `--no-deploy` | | no | false | Leave out the Databricks deploy steps (CI only) |
| `--output` | `-o` | no | `.github/workflows/<usecase>_<package>.yml` | Output file |
| `--overwrite` | | no | false | Overwrite an existing workflow file |

---

## `ubunye validate`

Render Jinja and validate the config schema, before Spark starts. Exits `0` on
success and non-zero if any task fails.

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
| `--usecase-dir` | `-d` | yes | | Root directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task-list` | `-t` | no | | Task(s) to validate (repeatable) |
| `--all` | | no | false | Validate all tasks in the package |
| `--profile` | | no | | Profile to validate against (e.g. dev, prod) |
| `--mode` | `-m` | no | | Alias for `--profile`, the same word `run` uses. Giving both with different values is an error |
| `--data-timestamp` | `-dt` | no | | Data timestamp |
| `--data-timestamp-format` | `-dtf` | no | | Timestamp format |
| `--var` | | no | | Extra template variable, `key=value` (repeatable) |
| `--backend` | | no | | Also check every input and output against what this backend can do, without starting it |
| `--json` | | no | false | Print one JSON document |

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
| `--usecase-dir` | `-d` | yes | | Root directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task-list` | `-t` | yes | | Task(s) to plan, in run order (repeatable) |
| `--data-timestamp` | `-dt` | no | | Data timestamp |
| `--data-timestamp-format` | `-dtf` | no | | Timestamp format |
| `--mode` | `-m` | no | `DEV` | Run mode |
| `--var` | | no | | Extra template variable, `key=value` (repeatable) |
| `--backend` | | no | | Also check against what this backend can do |
| `--json` | | no | false | Print one JSON document |

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
| `--usecase-dir` | `-d` | yes | | Root pipelines directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task-list` | `-t` | one of `-t` or `--all` | | Task(s) to execute (repeatable) |
| `--all` | | | false | Run every task in the package |
| `--data-timestamp` | `-dt` | no | | Data timestamp |
| `--data-timestamp-format` | `-dtf` | no | | Timestamp format |
| `--mode` | `-m` | no | `DEV` | Run mode (DEV/PROD) |
| `--deploy-mode` | | no | `client` | Spark deploy mode (cluster/client) |
| `--lineage` | | no | false | Record lineage for this run |
| `--lineage-dir` | | no | `.ubunye/lineage` | Root directory for lineage records |
| `--backend` | | no | platform, else `spark` | Execution backend by name, e.g. `pandas` for a run with no Java. See [Execution backends](backends.md) |
| `--var` | | no | | Extra template variable, `key=value` (repeatable) |
| `--rerun` | | no | false | Replace a batch (same `-dt` and `--var`) that a finished run already appended |
| `--resume` | | no | false | Skip tasks that already finished this batch, run the rest |

!!! note "Running a batch again"
    A batch is the task with its `-dt` and `--var` values. If the task appends and a
    run already finished that batch, `run` refuses, because the batch would land twice.
    Pass `--rerun` to replace it: the earlier run's files are removed once the new
    run succeeds. That works for appends to a folder of files on a local or shared
    disk, on pandas and on Spark. An append Ubunye cannot take back (a Delta, catalog
    or JDBC table, or a Spark path on object storage such as `s3a://`) is appended
    again, and the run says so; write such outputs with
    `mode: overwrite_partitions` and `partitionBy` if they must be rerun. A task that only overwrites,
    and a run with no `-dt` and no `--var`, are never refused. To finish a pipeline
    that stopped half way, run it again with `--resume`: tasks that finished the
    batch are skipped with `[SKIP]`, the rest run.

!!! note
    `run` picks the profile with `-m/--mode` (it has no `--profile`), and `--all`
    runs every task in the package. The mode must match a profile name exactly.

---

## `ubunye test run`

Run one or more tasks with a test profile and report PASS/FAIL per task.
Config is validated before Spark starts; invalid configs are reported as `[CONFIG FAIL]`
and the command exits non-zero at once.

```bash
ubunye test run \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | | Root directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task-list` | `-t` | yes | | Task(s) to test (repeatable) |
| `--profile` | | no | `test` | Config profile to use |
| `--data-timestamp` | `-dt` | no | | Data timestamp |
| `--lineage / --no-lineage` | | no | `lineage` | Record lineage (on by default) |
| `--lineage-dir` | | no | `.ubunye/lineage` | Lineage directory |
| `--backend` | | no | platform, else `spark` | Execution backend by name, e.g. `pandas` to test with no Java |
| `--var` | | no | | Extra template variable, `key=value` (repeatable) |

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

| Flag | Short | Default | Description |
|---|---|---|---|
| `--usecase-dir` | `-d` | | Also check tasks under this pipelines folder |
| `--usecase` | `-u` | | Use-case name |
| `--package` | `-p` | | Package name |
| `--task-list` | `-t` | | Task(s) to check (repeatable) |
| `--data-timestamp` | `-dt` | | Data timestamp |
| `--mode` | `-m` | `DEV` | Run mode |
| `--var` | | | Extra template variable, `key=value` (repeatable) |
| `--json` | | false | Print one JSON document |

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

## `ubunye gate`

Compare a run's receipt with a baseline and exit `1` if it regresses. See
[Gate](patterns/gate.md).

```bash
# two run records saved with `ubunye lineage show --json`
ubunye gate --baseline base.json --candidate head.json

# from the lineage store: by run id, or latest / previous
ubunye gate -d pipelines -u shop -p orders -t clean                # previous vs latest
ubunye gate -d pipelines -u shop -p orders -t clean --baseline 1a2b3c4d
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--baseline` | | `previous` | A run record file, a run id, or `previous` |
| `--candidate` | | `latest` | A run record file, a run id, or `latest` |
| `--usecase-dir` | `-d` | | Root pipelines directory |
| `--usecase` | `-u` | | Use-case name |
| `--package` | `-p` | | Package name |
| `--task` | `-t` | | Task name |
| `--lineage-dir` | | `.ubunye/lineage` | Lineage directory |
| `--allow-data-change` | | false | A changed output warns instead of failing |
| `--max-slowdown` | | | Fail when slower than the baseline by more than this (`0.5` = 50%) |
| `--max-seconds` | | | Fail when the candidate took longer than this |
| `--max-row-change` | | | Fail when an output's row count moved by more than this (`0.1` = 10%) |
| `--require-replay` | | false | Fail if any model call went live instead of replaying (`UBUNYE_LLM_MODE=replay`) |
| `--max-llm-cost-increase` | | | Fail if the model bill grew by more than this share, e.g. `0.2` |
| `--summary` | | | Append a Markdown table to this file (`$GITHUB_STEP_SUMMARY`) |
| `--json` | | false | Print one JSON document |

---

## `ubunye mcp`

Serve the engine to agents over MCP (stdio): tasks, doctor, plan, runs, gate,
focus. See [Agents over MCP](patterns/mcp.md).

```bash
ubunye mcp -d pipelines
ubunye mcp -d pipelines --allow-run
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--usecase-dir` | `-d` | `.` | The pipelines folder the server works in |
| `--allow-run` | | false | Offer the `run` tool (every other tool only reads) |
| `--allow-live-llm` | | false | Let `run` call models live (default: replay, no key, no spend) |
| `--lineage-dir` | | `.ubunye/lineage` | Lineage directory |

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
| `models list` | model versions, with `stage` as text |
| `models compare` | `{metric: {"a", "b", "delta"}}` |

`models info` always prints the version as JSON, so it has no `--json` flag.

---

## `ubunye plugins`

List the readers, writers and transforms discovered through entry points.

```bash
ubunye plugins
```

---

## `ubunye config`

Load each task's config (Jinja rendered, Pydantic validated) and check the
profile for `--mode`. Prints `[OK] Config valid: <path>` per task, or the error
and exit code `1`.

```bash
ubunye config \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl \
    -dt 2024-06-01
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | | Root directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task-list` | `-t` | yes | | Task(s) to check (repeatable) |
| `--data-timestamp` | `-dt` | no | | Data timestamp |
| `--data-timestamp-format` | `-dtf` | no | | Timestamp format |
| `--mode` | `-m` | no | `DEV` | Run mode |
| `--var` | | no | | Extra template variable, `key=value` (repeatable) |

---

## `ubunye version`

```bash
ubunye version
# Ubunye Engine v0.8.0
```

The version comes from the installed package metadata.

---

## `ubunye lineage`

Inspect run records written by `ubunye run --lineage` (and by `ubunye test run`,
which records lineage by default).

!!! note
    Lineage sub-commands use `--task` (`-t` singular), not `--task-list`. All of
    them take `--lineage-dir` (default `.ubunye/lineage`, under `--usecase-dir`).

### `lineage show`

Show a run record as formatted JSON (latest or specific run).

```bash
ubunye lineage show \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl

ubunye lineage show \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl \
    --run-id <run_id>
```

Each input also says whether its data hash is of what the run read. On Spark an
input is hashed at the end of the task by reading its source again, so the record
takes the source's version when the input is read and again right after the hash:

```json
"source_version": {"kind": "files", "files": 2, "bytes": 2030, "etags": false, "listing_hash": "sha256:..."},
"source_version_at_hash": {"kind": "files", "files": 2, "bytes": 2034, "etags": false, "listing_hash": "sha256:..."},
"source_changed": true,
"source_note": "The source changed between the read and the hash (2 files, 2030 bytes -> 2 files, 2034 bytes). This digest is of the later state, not of what the task read."
```

A file input's version is its files (`files`, `bytes`, `latest_modified`, `etags`,
`listing_hash`), from Spark's own file index; nothing is read. Where the file system
gives no content tags (a local disk, HDFS), an unchanged version means names, sizes
and times only, and the note says so. A Delta read is pinned to one version (by the
config, or by the engine when the config names none), so its note says the digest is
of what was read and gives the table's `latest_version` at the hash as information.
A SQL query, JDBC, a catalog table that is not Delta, or a listing that failed or took
longer than `UBUNYE_SOURCE_VERSION_TIMEOUT` seconds (30) has `"kind": "none"` and the
reason. On pandas the input is in memory, so the version is recorded but not checked
again. With `LineageRecorder(hash_inputs=False)` no version is taken.
`lineage trace` prints the note, and `lineage compare` and `ubunye gate` do not treat
such an input's hash as evidence. See [the run record](architecture/adr-006-run-record.md).

### `lineage list`

List recent runs for a task (newest first). `-n/--n` sets how many (default 10).

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

Search all recorded runs across tasks. Only `-d` is required; `-u`, `-p` and
`-t` narrow the search, `--status` takes `success`, `error` or `running`, and
`--since` takes an ISO date.

```bash
ubunye lineage search -d pipelines -t claim_etl
ubunye lineage search -d pipelines --status success --since 2024-06-01
```

### `lineage trace`

Print the input, transform and output data flow graph for a run (latest, or
`--run-id`).

```bash
ubunye lineage trace \
    -d pipelines -u fraud_detection -p ingestion -t claim_etl
```

### `lineage openlineage`

Print stored runs as OpenLineage events, one JSON per line (START, then
COMPLETE or FAIL). `--run-id` picks one run (default: every run), `--namespace`
sets the job namespace, and `--send` also sends them where `OPENLINEAGE_URL`
points (a backfill). See [OpenLineage](patterns/openlineage.md).

```bash
ubunye lineage openlineage -d pipelines -u fraud_detection -p ingestion -t claim_etl
```

### `lineage focus`

A run's model calls as FOCUS 1.4 cost rows, for FinOps tools. `--format` is
`csv` (default) or `jsonl`; `-o/--output` writes a file instead of stdout;
`--run-id` picks a run (default: latest).

```bash
ubunye lineage focus -d pipelines -u fraud_detection -p ingestion -t claim_etl -o cost.csv
```

---

## `ubunye models`

Manage ML model versions and lifecycle.
All sub-commands require: `--use-case` (`-u`), `--model` (`-m`), `--store` (`-s`).
`info`, `promote`, `demote`, `rollback` and `archive` also require `--version` (`-v`).

!!! warning
    `models` uses `--use-case` (hyphenated), while other commands use `--usecase`.

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

## `ubunye prove`

The proving ground: run one workload on many environments, turn each run record
into an observation, and compare them. See [Proving ground](proving-ground/index.md).

```bash
ubunye prove observe --workload c01 --env pandas-local -d . -u uc -p pkg -t task -o evidence
ubunye prove observe --workload c01 --env aws-glue --record glue.json -o evidence
ubunye prove skip    --workload c01 --env azure --status not_run --reason "..." -o evidence
ubunye prove report  evidence --workload c01 --reference spark-local --expect aws-glue
```

### `prove observe`

Turn a run record into an observation of a workload in an environment. Name the
record with `--record FILE`, or with `-d -u -p -t` and `--run-id` (default
`latest`). Several tasks (`-t a -t b`) give one observation each, named
`<workload>-<task>`.

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--workload` | | yes | | The workload's id, e.g. `c01-portable-etl` |
| `--env` | | yes | | The environment's name, e.g. `spark-local` |
| `--out` | `-o` | no | `evidence` | The evidence folder |
| `--record` | | no | | A run record JSON file |
| `--usecase-dir`, `--usecase`, `--package`, `--task` | `-d`, `-u`, `-p`, `-t` | no | | Find the record in the lineage store |
| `--run-id` | | no | `latest` | A stored run's id, or `latest` |
| `--kind` | | no | `local` | `local`, `cloud` or `managed` |
| `--provider` | | no | | `aws`, `gcp`, `azure`, `databricks`, ... |
| `--runtime` | | no | | e.g. `Glue 5.0`, `Dataproc Serverless 2.2` |
| `--region` | | no | | Region |
| `--cost-amount` | | no | | Cost of the run |
| `--cost-currency` | | no | `USD` | Currency |
| `--cost-basis` | | no | | `actual`, `provider_estimate`, `ubunye_estimate`, `local` or `unknown` |
| `--cost-source` | | no | | How the figure was obtained |
| `--run-url` | | no | | Where a reader can check this run |

### `prove skip`

Record that an environment did not run the workload, and why. Takes
`--workload`, `--env` and `--reason` (all required), `--status` (`not_run`
(default), `unsupported` or `failed_to_launch`), `-o/--out` (default
`evidence`) and `--run-url`.

### `prove report`

Compare every observation of a workload in the `EVIDENCE` folder with the
`--reference` environment, and exit `1` if any run disagrees. `--expect` takes a
comma-separated list of environments; one with no evidence is reported as NOT
RUN. `--json FILE` and `--md FILE` write the matrix and the table.

---

## `ubunye sync`

Replay fallback manifests against configured backends. When a metadata write
(lineage or model registry) fails during a run, the record is appended to a
local JSONL manifest under `~/.ubunye/fallback/` so the run does not fail. This
command replays them, skipping records already written.

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
| `--run-id` | no | | Replay only this run's manifest |

Processed manifests are archived to `~/.ubunye/fallback/synced/`.

---

## `ubunye deploy`

Deploy or run a task on a remote platform. Each platform has a guide under
[Deployment](deployment.md).

### `deploy databricks`

Deploy a task to Databricks as a scheduled job.

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
| `--usecase-dir` | `-d` | yes | | Root pipelines directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task` | `-t` | yes | | Task to deploy |
| `--target` | | no | `dev` | Deploy target from `targets.yaml` |
| `--mode` | `-m` | no | `PROD` | Config profile for the job |
| `--data-timestamp` | `-dt` | no | | Data timestamp |
| `--dry-run` | | no | false | Print the job spec without deploying |
| `--host` | | no | | Databricks host for an ad-hoc deploy (skips `targets.yaml`) |
| `--token` | | no | `DATABRICKS_TOKEN` | Name of the env var that holds the token |

`targets.yaml` is read from `<usecase-dir>/<usecase>/targets.yaml`, and a
`targets.yaml` in the task folder overrides it. See
[Databricks authentication](databricks-auth.md).

### `deploy glue`

Run a task on AWS Glue 5 (Spark 3.5), with the `aws` CLI. See [One command to the cloud](deployment/serverless.md).

```bash
ubunye deploy glue -d pipelines -u shop -p orders -t clean \
    --bucket my-bucket --role arn:aws:iam::123456789012:role/glue-job --dry-run
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | | Root pipelines directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task` | `-t` | yes | | Task name; repeat to run several in order, in one launch |
| `--bucket` | | yes | | S3 bucket for the task, script and wheel |
| `--role` | | yes | | IAM role ARN the Glue job runs as |
| `--region` | | no | | AWS region |
| `--job` | | no | from the task | Glue job name |
| `--engine` | | no | `ubunye-engine` | pip requirement for the engine |
| `--engine-wheel` | | no | | Install this wheel instead |
| `--workers` | | no | `2` | Number of workers |
| `--worker-type` | | no | `G.1X` | Glue worker type |

Plus the shared launch flags below.

### `deploy dataproc`

Run a task on GCP Dataproc Serverless, in a container image, with `gcloud`. See
[One command to the cloud](deployment/serverless.md).

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase-dir` | `-d` | yes | | Root pipelines directory |
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task` | `-t` | yes | | Task name; repeat to run several in order |
| `--project` | | yes | | GCP project |
| `--region` | | yes | | GCP region |
| `--bucket` | | yes | | GCS bucket for the task and script |
| `--image` | | yes | | Container image with the engine (see `deploy dockerfile dataproc`) |
| `--service-account` | | no | | Service account the batch runs as |
| `--runtime` | | no | `2.2` | Dataproc Serverless runtime version |
| `--batch` | | no | from the task | Batch id |

Plus the shared launch flags below.

### `deploy k8s`

Run a task as a Kubernetes Job, with `kubectl` (its current context). The image
comes from `deploy dockerfile container`, with the pipelines baked in, so there
is no `-d`. See [One command to the cloud](deployment/serverless.md).

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task` | `-t` | yes | | Task name; repeat to run several in order |
| `--image` | | yes | | Image from `deploy dockerfile container` |
| `--namespace` | `-n` | no | `default` | Kubernetes namespace |
| `--job` | | no | from the task | Job name |
| `--cpu` | | no | `1` | CPU request |
| `--memory` | | no | `2Gi` | Memory request |
| `--timeout` | | no | `1800` | Seconds to wait for the job |

Plus the shared launch flags below.

### `deploy container-apps`

Run a task as an Azure Container Apps job, with the `az` CLI. Like `k8s`, the
image holds the pipelines. See [One command to the cloud](deployment/serverless.md).

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task` | `-t` | yes | | Task name; repeat to run several in order |
| `--image` | | yes | | Image from `deploy dockerfile container` |
| `--resource-group` | `-g` | yes | | Azure resource group |
| `--environment` | | yes | | Container Apps environment |
| `--job` | | no | from the task | Job name |
| `--registry-server` | | no | | Registry server |
| `--registry-identity` | | no | | Managed identity (resource id) that pulls the image |
| `--cpu` | | no | `2` | CPU |
| `--memory` | | no | `4Gi` | Memory |
| `--timeout` | | no | `1800` | Seconds a replica may run |

Plus the shared launch flags below.

### `deploy emr-serverless`

Start a task run on AWS EMR Serverless, with the `aws` CLI. It does not wait for
the run to finish, so it has no `--wait` or `--record-out`. See
[One command to the cloud](deployment/serverless.md).

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--usecase` | `-u` | yes | | Use-case name |
| `--package` | `-p` | yes | | Package name |
| `--task` | `-t` | yes | | Task name; repeat to run several in order |
| `--application-id` | | yes | | An application whose image is from `deploy dockerfile emr-serverless` |
| `--role` | | yes | | Job execution role ARN |
| `--bucket` | | yes | | S3 bucket for the job's logs |
| `--region` | | no | | AWS region |
| `--mode` | `-m` | no | `PROD` | Config profile |
| `--data-timestamp` | `-dt` | no | | Data timestamp |
| `--env` | | no | | Environment for the task, `KEY=VALUE` (repeatable) |
| `--var` | | no | | Template variable, `KEY=VALUE` (repeatable) |
| `--dry-run` | | no | false | Print the plan; run nothing |

### Shared launch flags

`glue`, `dataproc`, `k8s` and `container-apps` also take:

| Flag | Short | Default | Description |
|---|---|---|---|
| `--mode` | `-m` | `PROD` | Config profile |
| `--data-timestamp` | `-dt` | | Data timestamp |
| `--env` | | | Environment for the task, `KEY=VALUE` (repeatable) |
| `--var` | | | Template variable, `KEY=VALUE` (repeatable) |
| `--wait / --no-wait` | | `wait` | Follow the run to its end |
| `--record-out` | | | Save the run's record here (for `ubunye gate`); with several tasks, one file each: `NAME.TASK.json` |
| `--dry-run` | | false | Print the plan; run nothing |

### `deploy dockerfile`

Write a Dockerfile for a Spark job image on `PLATFORM` (`dataproc`,
`emr-serverless` or `container`), and the entry script next to it.

```bash
ubunye deploy dockerfile container --pipelines pipelines --out build/Dockerfile
```

| Flag | Default | Description |
|---|---|---|
| `--pipelines` | `pipelines` | Folder copied into the image as `/app/pipelines` |
| `--engine` | `ubunye-engine` | pip requirement for the engine |
| `--out` | | Write here instead of printing |

---

## `ubunye export`

Generate orchestration artifacts from a task's `config.yaml`. Defaults come from
the config's `ORCHESTRATION` block (see [Orchestration](config/orchestration.md)).

### `export airflow`

Generate an Airflow DAG (Airflow 2.4+ and 3) that runs the task with the CLI.

```bash
ubunye export airflow \
    -c pipelines/fraud_detection/ingestion/claim_etl/config.yaml \
    -o dags/claim_etl.py
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--config` | `-c` | yes | | Path to the task's `config.yaml` |
| `--output` | `-o` | yes | | Where to write the DAG |
| `--profile` | | no | `prod` | Profile embedded in the bash command |
| `--usecase-dir` | | no | this folder | The pipelines folder where Airflow runs |
| `--backend` | | no | | Backend for the run |
| `--lineage` | | no | false | Record each run (run record) |

### `export databricks`

Generate a Databricks Jobs API spec (`job.json`) for the task.

```bash
ubunye export databricks \
    -c pipelines/fraud_detection/ingestion/claim_etl/config.yaml \
    -o jobs/claim_etl.json
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--config` | `-c` | yes | | Path to the task's `config.yaml` |
| `--output` | `-o` | yes | | Where to write `job.json` |
| `--profile` | | no | `prod` | Profile embedded in the job command |

### `export spark-pipeline`

Write the task as a Spark Declarative Pipeline (Spark 4.1+), to run with
`spark-pipelines`.

```bash
ubunye export spark-pipeline \
    -c pipelines/fraud_detection/ingestion/claim_etl/config.yaml \
    -o sdp/claim_etl
```

| Flag | Short | Required | Default | Description |
|---|---|---|---|---|
| `--config` | `-c` | yes | | Path to the task's `config.yaml` |
| `--output` | `-o` | yes | | Folder to write the pipeline into |
| `--storage` | | no | a folder inside the output | Pipeline storage URI |
| `--name` | | no | | Pipeline name |
| `--catalog` | | no | | Catalog |
| `--database` | | no | | Database |
| `--mode` | `-m` | no | `PROD` | Config profile |
| `--data-timestamp` | `-dt` | no | | Data timestamp |
| `--var` | | no | | Extra template variable, `key=value` (repeatable) |
