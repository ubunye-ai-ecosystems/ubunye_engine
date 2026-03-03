# CLI Reference

The Ubunye CLI (`ubunye`) is built with [Typer](https://typer.tiangolo.com/).
All commands accept `--help` for full option details.

---

## Top-level commands

| Command | Description |
|---|---|
| `init` | Scaffold a new task folder |
| `validate` | Validate a config file without running |
| `plan` | Print the execution DAG |
| `run` | Execute a task |
| `plugins` | List all discovered plugins |
| `config` | Show the expanded (Jinja-rendered + validated) config |
| `version` | Print the engine version |

### Sub-command groups

| Group | Description |
|---|---|
| `lineage` | Inspect run provenance records |
| `models` | Manage model versions and lifecycle |

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
```

---

## `ubunye validate`

Render Jinja and validate the config schema. Exits `0` on success, `1` on error.

```bash
ubunye validate \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl \
    --var dt=2024-06-01
```

---

## `ubunye plan`

Print the execution DAG — inputs → transform → outputs — without running.

```bash
ubunye plan \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl
```

---

## `ubunye run`

Execute a task: read inputs → transform → write outputs.

```bash
ubunye run \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl \
    --profile prod \
    --lineage \
    --var dt=2024-06-01
```

| Flag | Description |
|---|---|
| `-d, --dir` | Root pipelines directory |
| `-u, --use-case` | Use-case name |
| `-p, --pipeline` | Pipeline name |
| `-t, --task` | Task name |
| `--profile` | Engine profile to activate (dev / staging / prod) |
| `--lineage` | Record run provenance to `.ubunye/lineage/` |
| `--var KEY=VALUE` | Inject a Jinja variable (repeatable) |

---

## `ubunye plugins`

List all readers, writers, transforms, and monitors discovered via entry points.

```bash
ubunye plugins
```

---

## `ubunye config`

Show the fully expanded (Jinja-rendered, Pydantic-validated) config as JSON.

```bash
ubunye config show \
    -d pipelines \
    -u fraud_detection \
    -p ingestion \
    -t claim_etl \
    --var dt=2024-06-01
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

### `lineage list`

```bash
ubunye lineage list
ubunye lineage list --use-case fraud_detection
ubunye lineage list --limit 20
```

### `lineage show`

```bash
ubunye lineage show --run-id <run_id>
```

### `lineage compare`

```bash
ubunye lineage compare --run-ids <id1> <id2>
```

### `lineage search`

```bash
ubunye lineage search --task claim_etl
ubunye lineage search --tag status=success
```

### `lineage trace`

```bash
ubunye lineage trace --run-id <run_id>
```

Prints a full provenance chain: config hash, input data hashes, transform, outputs.

---

## `ubunye models`

Manage ML model versions and lifecycle.
Requires the `--use-case`, `--model`, and `--store` flags on all sub-commands.

### `models list`

```bash
ubunye models list \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --store .ubunye/model_store
```

### `models info`

```bash
ubunye models info \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.3.0 \
    --store .ubunye/model_store
```

### `models promote`

```bash
ubunye models promote \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.3.0 \
    --to production \
    --promoted-by alice \
    --store .ubunye/model_store
```

Target stages: `staging`, `production`.

### `models demote`

```bash
ubunye models demote \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.3.0 \
    --to staging \
    --store .ubunye/model_store
```

Target stages: `development`, `staging`, `archived`.

### `models rollback`

```bash
ubunye models rollback \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.2.0 \
    --store .ubunye/model_store
```

Archives the current production version and restores `--version` to production.

### `models archive`

```bash
ubunye models archive \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.1.0 \
    --store .ubunye/model_store
```

### `models compare`

```bash
ubunye models compare \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --versions 1.2.0 1.3.0 \
    --store .ubunye/model_store
```
