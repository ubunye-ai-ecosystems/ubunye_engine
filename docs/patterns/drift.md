# Pattern: Drift Detection

Data drift and model drift degrade prediction quality silently.
This pattern shows how to compute drift metrics with Ubunye and gate model promotion on them.

---

## What is drift?

| Type | Description |
|---|---|
| **Data drift** | Input feature distribution has shifted since training |
| **Concept drift** | The relationship between features and target has changed |
| **Prediction drift** | Model output distribution has shifted |

---

## Step 1 — Compute drift metrics

Calculate PSI (Population Stability Index) or KL divergence between a reference
distribution (from training) and the current serving window.

```yaml
# pipelines/fraud/drift/psi/config.yaml
MODEL: etl
VERSION: "1.0.0"

CONFIG:
  inputs:
    reference:
      format: delta
      path: s3://datalake/ml/training_sets/fraud/

    current:
      format: hive
      db_name: raw
      tbl_name: transactions
      sql: >-
        SELECT * FROM raw.transactions
        WHERE event_date >= date_sub(current_date(), 7)

  transform:
    type: task

  outputs:
    drift_metrics:
      format: delta
      path: s3://datalake/ml/drift/fraud/
      mode: overwrite
```

`transformations.py` — PSI per numeric feature:

```python
import math
from pyspark.sql import functions as F
from ubunye.core.interfaces import Task

N_BINS = 10

class DriftTask(Task):
    def transform(self, sources: dict) -> dict:
        ref = sources["reference"]
        cur = sources["current"]

        feature_cols = ["amount_sum_30d", "txn_count_30d", "avg_amount_30d"]
        rows = []
        for col in feature_cols:
            psi = _psi(ref, cur, col)
            rows.append({"feature": col, "psi": psi, "status": "PASS" if psi < 0.2 else "FAIL"})

        spark = ref.sparkSession
        return {"drift_metrics": spark.createDataFrame(rows)}

def _psi(ref_df, cur_df, col):
    """Compute PSI for a single numeric column."""
    breaks = ref_df.approxQuantile(col, [i / N_BINS for i in range(N_BINS + 1)], 0.01)
    ref_hist = _histogram(ref_df, col, breaks)
    cur_hist = _histogram(cur_df, col, breaks)
    return sum(
        (c - r) * math.log((c + 1e-8) / (r + 1e-8))
        for r, c in zip(ref_hist, cur_hist)
    )

def _histogram(df, col, breaks):
    total = df.count()
    counts = []
    for lo, hi in zip(breaks[:-1], breaks[1:]):
        n = df.filter((F.col(col) >= lo) & (F.col(col) < hi)).count()
        counts.append(max(n / total, 1e-8))
    return counts
```

---

## Step 2 — Gate model promotion on drift check

The `require_drift_check` promotion gate checks that `metadata["drift_check_passed"]`
is `True` before allowing promotion to staging or production.

In your training pipeline's `model.py`:

```python
from ubunye.models.base import UbunyeModel

class FraudRiskModel(UbunyeModel):
    def train(self, df):
        # ... training logic
        metrics = {"auc": 0.91, "f1": 0.87}
        # Read drift results and attach to metrics/metadata
        drift_ok = self._check_drift()
        return {**metrics, "_drift_check_passed": drift_ok}
    ...
```

In `config.yaml` for the training task:

```yaml
  transform:
    type: model
    params:
      action: train
      model_class: "model.FraudRiskModel"
      registry:
        store: ".ubunye/model_store"
        use_case: fraud
        auto_version: true
        promote_to: staging
        promotion_gates:
          min_auc: 0.85
          min_f1: 0.80
          require_drift_check: true     # blocks promotion if drift_check_passed=false
```

Or pass `drift_check_passed` via metadata at registration time (from the CLI or
a pre-training orchestration step that computes drift first).

---

## Step 3 — Inspect drift over time

Query the drift metrics Delta table:

```sql
SELECT feature, psi, status, run_date
FROM delta.`s3://datalake/ml/drift/fraud/`
ORDER BY run_date DESC, psi DESC
```

Or use the Ubunye lineage CLI to trace which runs had drift:

```bash
ubunye lineage search --tag drift_check=FAIL
```

---

## Alerting

Write the drift output to a JDBC table and trigger an alert when PSI > 0.2:

```yaml
  outputs:
    drift_metrics:
      format: delta
      path: s3://datalake/ml/drift/fraud/
      mode: append

    drift_alerts:
      format: jdbc
      url: "jdbc:postgresql://{{ env.DB_HOST }}/mlops"
      table: public.drift_alerts
      mode: append
```

Filter in `transformations.py` to write only FAIL rows to the alerts table:

```python
return {
    "drift_metrics": metrics,
    "drift_alerts": metrics.filter("status = 'FAIL'"),
}
```

---

## Promotion gate reference

| Gate key | Type | Description |
|---|---|---|
| `min_<metric>` | float | Metric must be `>=` threshold |
| `max_<metric>` | float | Metric must be `<=` threshold |
| `require_drift_check` | bool | `metadata["drift_check_passed"]` must be `True` |

```yaml
promotion_gates:
  min_auc: 0.85
  max_loss: 0.15
  require_drift_check: true
```

When any gate fails, `registry.promote()` raises `ValueError` listing each failing gate
with the actual value and threshold. The CLI surfaces this as an `[ERROR]` message.
