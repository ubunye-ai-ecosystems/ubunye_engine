# Model Registry

The Ubunye model registry manages the full ML lifecycle — versioning, promotion,
demotion, rollback, and gated releases — without coupling to any specific ML library.

---

## Lifecycle stages

```
development → staging → production
      ↓            ↓         ↓
                       archived
```

| Stage | Meaning |
|---|---|
| `development` | Freshly registered; not yet validated |
| `staging` | Passed quality gates; ready for shadow or canary testing |
| `production` | Live; serving predictions |
| `archived` | Replaced or retired; kept for audit |

Every `register()` call creates a `development` version.
Promotion moves a version up the chain; demotion moves it down or to `archived`.

---

## Storage layout

```
{store_path}/
└── {use_case}/
    └── {model_name}/
        ├── registry.json          ← version index (all metadata)
        └── versions/
            └── 1.0.0/
                ├── model/         ← opaque artifact (from model.save())
                ├── metadata.json  ← model.metadata() output
                └── metrics.json   ← train() metrics
```

The engine calls `model.save(path)` — the artifact format (pickle, ONNX, pt, …) is yours.

---

## CLI commands

### List versions

```bash
ubunye models list \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --store .ubunye/model_store
```

Output:

```
  Version      Stage          Registered                    Key metrics
  ------------ -------------- ---------------------------- ------------------------------
  1.3.0        production     2024-06-15T02:10:00Z          auc=0.930, f1=0.880
  1.2.0        staging        2024-06-10T01:45:00Z          auc=0.910, f1=0.870
  1.1.0        archived       2024-06-01T00:30:00Z          auc=0.880, f1=0.850
```

### Show version details

```bash
ubunye models info \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.3.0 \
    --store .ubunye/model_store
```

Returns full JSON including timestamps, metrics, metadata, lineage run ID.

### Promote

```bash
ubunye models promote \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.3.0 \
    --to production \
    --promoted-by alice \
    --store .ubunye/model_store
```

Promoting to `production` automatically archives the current production version.

### Demote

```bash
ubunye models demote \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.3.0 \
    --to staging \
    --store .ubunye/model_store
```

### Rollback

Restores a previous version to production and archives the current one:

```bash
ubunye models rollback \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.2.0 \
    --store .ubunye/model_store
```

### Archive

```bash
ubunye models archive \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --version 1.1.0 \
    --store .ubunye/model_store
```

### Compare metrics

```bash
ubunye models compare \
    --use-case fraud_detection \
    --model FraudRiskModel \
    --versions 1.2.0 1.3.0 \
    --store .ubunye/model_store
```

Output:

```
  Comparing fraud_detection/FraudRiskModel: 1.2.0 vs 1.3.0

  Metric               v1.2.0             v1.3.0             Delta
  -------------------- ------------------ ------------------ ----------
  auc                  0.9100             0.9300             +0.0200
  f1                   0.8700             0.8800             +0.0100
  accuracy             0.9400             0.9500             +0.0100
```

---

## Promotion gates

Gates block promotion unless all thresholds are satisfied.

### Gate types

| Key pattern | Description |
|---|---|
| `min_<metric>` | Metric value must be `>=` threshold |
| `max_<metric>` | Metric value must be `<=` threshold |
| `require_drift_check` | `metadata["drift_check_passed"]` must be `True` |

### Config example

```yaml
transform:
  type: model
  params:
    action: train
    model_class: "model.FraudRiskModel"
    registry:
      store: ".ubunye/model_store"
      use_case: fraud_detection
      auto_version: true
      promote_to: staging
      promotion_gates:
        min_auc: 0.85
        min_f1: 0.80
        max_loss: 0.20
        require_drift_check: true
```

When a gate fails the engine logs each failing gate with its actual value and threshold,
and does **not** promote. The version remains in `development`.

### CLI promote with gates

Gates can also be evaluated via `ubunye models promote` if you specify them in the
registry's `registry.json` on the version. The Python API is:

```python
from ubunye.models.registry import ModelRegistry, ModelStage
from ubunye.models.gates import PromotionGate

registry = ModelRegistry(".ubunye/model_store")
gates = {"min_auc": 0.85, "min_f1": 0.80}
mv = registry.promote(
    use_case="fraud_detection",
    model_name="FraudRiskModel",
    version="1.3.0",
    to_stage=ModelStage.PRODUCTION,
    gates=gates,
    promoted_by="alice",
)
```

---

## Python API

```python
from ubunye.models import ModelRegistry, ModelStage, ModelVersion

registry = ModelRegistry(".ubunye/model_store")

# List all versions (newest first)
versions = registry.list_versions("fraud_detection", "FraudRiskModel")

# Get the production artifact path
artifact_path, mv = registry.get_model(
    "fraud_detection", "FraudRiskModel", stage=ModelStage.PRODUCTION
)
# Load via your own model class
from model import FraudRiskModel
model = FraudRiskModel.load(artifact_path)

# Promote
registry.promote("fraud_detection", "FraudRiskModel", "1.3.0", ModelStage.PRODUCTION)

# Rollback to a previous version
registry.rollback("fraud_detection", "FraudRiskModel", "1.2.0")

# Compare
diff = registry.compare_versions("fraud_detection", "FraudRiskModel", "1.2.0", "1.3.0")
# Returns: {"auc": {"a": 0.91, "b": 0.93, "delta": 0.02}, ...}
```

---

## Auto-versioning

When `auto_version: true` and no explicit `version` is set, the registry bumps the
**patch** component of the highest existing version:

```
(no versions yet) → 1.0.0
1.0.0 → 1.0.1
1.2.3 → 1.2.4
```

To pin a specific version:

```yaml
registry:
  store: ".ubunye/model_store"
  use_case: fraud_detection
  auto_version: false
  version: "2.0.0"
```

---

## Lineage integration

When `--lineage` is passed to `ubunye run`, the run ID is automatically attached
to the registered `ModelVersion.lineage_run_id`.

```bash
ubunye run -d pipelines -u fraud -p ml -t train --lineage
ubunye models info --use-case fraud_detection --model FraudRiskModel \
    --version 1.3.0 --store .ubunye/model_store
# → lineage_run_id: "abc123..."

ubunye lineage show --run-id abc123...
```

This lets you trace exactly which data, config hash, and code produced each model version.
