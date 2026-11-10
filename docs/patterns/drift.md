# Pattern: Drift Detection

For anyone who trains a model with Ubunye and wants to stop a model going live
when today's data no longer looks like the data it learned from.

- **The problem:** the data a model sees changes over time, and its predictions
  get worse without any error. You only find out when someone complains.
- **When to use it:** you score or retrain a model on a schedule, and you want a
  number that says "the inputs have moved", plus a gate that blocks promotion
  when they have.
- **The one command:** `ubunye run -d pipelines -u fraud -p drift -t psi -dt 2026-11-02 --lineage`

The whole guide runs on your laptop on the pandas backend with small sample data.
Every output below is real.

---

## What is drift?

| Type | What changed |
|---|---|
| **Data drift** | The values of the input features moved since training (amounts got bigger, say) |
| **Concept drift** | The link between the features and the answer changed |
| **Prediction drift** | The model's outputs moved |

This guide measures data drift with **PSI** (Population Stability Index): one
number per feature that compares how the values spread out in the training set
with how they spread out now. A common reading:

| PSI | Meaning |
|---|---|
| below 0.1 | no real change |
| 0.1 to 0.2 | some change, keep an eye on it |
| 0.2 or more | big change: this guide calls it `FAIL` |

---

## The project

```
project/
├── lake/fraud/                    # data: on a real lake, your Delta tables
│   ├── training_set.parquet       # the rows the model learned from
│   └── features.parquet           # the feature table: same columns, new rows
└── pipelines/                     # the pipelines folder (-d)
    └── fraud/                     # use case
        ├── drift/psi/             # task 1: measure drift, keep the history
        │   ├── config.yaml
        │   └── transformations.py
        └── ml/train/              # task 2: train, and promote only if drift passed
            ├── config.yaml
            ├── model.py
            └── transformations.py
```

??? note "Make the sample data (`make_data.py`)"
    Run this once from `project/`. The feature table's last week has bigger
    amounts than the training set, so it drifts.

    ```python
    # Small sample data: a training set and a feature table with two weeks of rows.
    import numpy as np
    import pandas as pd

    rng = np.random.default_rng(7)


    def rows(n, amount_mean, start, days):
        amount = rng.gamma(2.0, amount_mean / 2.0, n)
        count = rng.poisson(6, n) + 1
        risk = rng.integers(0, 3, n)
        fraud = ((amount > amount_mean * 1.5) & (risk >= 1)) | (rng.random(n) < 0.01)
        return pd.DataFrame({
            "event_date": (pd.Timestamp(start) + pd.to_timedelta(rng.integers(0, days, n), unit="D")).strftime("%Y-%m-%d"),
            "amount_sum_30d": amount.round(2),
            "txn_count_30d": count,
            "avg_amount_30d": (amount / count).round(2),
            "risk_encoded": risk,
            "is_fraud": fraud.astype(int),
        })


    rows(800, 500, "2026-09-01", 30).to_parquet("lake/fraud/training_set.parquet", index=False)
    # Last week the amounts jumped: the feature table drifts from the training set.
    features = pd.concat([rows(200, 500, "2026-10-20", 7), rows(200, 900, "2026-10-27", 7)])
    features.to_parquet("lake/fraud/features.parquet", index=False)
    ```

---

## Step 1: Measure drift

The task compares the training set (`reference`) with the last 7 days of the
**feature table** (`current`). Both hold the same feature columns, so PSI
compares like with like. Each run adds its rows, stamped with `run_date`, so
you keep a history.

```yaml
# pipelines/fraud/drift/psi/config.yaml
MODEL: etl
VERSION: "1.0.0"

CONFIG:
  inputs:
    reference:              # the rows the model was trained on
      format: s3
      path: lake/fraud/training_set.parquet
      file_format: parquet
    current:                # the feature table: same columns, newer rows
      format: s3
      path: lake/fraud/features.parquet
      file_format: parquet

  transform:
    params:
      run_date: "{{ dt }}"
      window_days: 7

  outputs:
    drift_metrics:          # one row per feature per run, kept as history
      format: s3
      path: lake/fraud/drift_metrics
      file_format: parquet
      mode: append
    drift_alerts:           # only the features that failed
      format: s3
      path: lake/fraud/drift_alerts
      file_format: parquet
      mode: append
```

`{{ dt }}` is the date you pass with `-dt` (see [Jinja](../config/jinja.md)).
The write mode is `append`, so each run adds to the history instead of replacing it.

```python
# pipelines/fraud/drift/psi/transformations.py
import numpy as np
import pandas as pd

from ubunye.core.interfaces import Task

FEATURES = ["amount_sum_30d", "txn_count_30d", "avg_amount_30d"]
N_BINS = 10
PSI_LIMIT = 0.2


class DriftTask(Task):
    def transform(self, sources: dict) -> dict:
        params = self.config["CONFIG"]["transform"]["params"]
        run_date = pd.Timestamp(params["run_date"])
        since = run_date - pd.Timedelta(days=params["window_days"])

        ref = sources["reference"]
        cur = sources["current"]
        dates = pd.to_datetime(cur["event_date"])
        cur = cur[(dates > since) & (dates <= run_date)]

        rows = []
        for col in FEATURES:
            psi = _psi(ref[col], cur[col])
            rows.append({
                "run_date": run_date.strftime("%Y-%m-%d"),
                "feature": col,
                "psi": round(psi, 4),
                "status": "PASS" if psi < PSI_LIMIT else "FAIL",
            })
        metrics = pd.DataFrame(rows)
        return {
            "drift_metrics": metrics,
            "drift_alerts": metrics[metrics["status"] == "FAIL"],
        }


def _psi(ref: pd.Series, cur: pd.Series) -> float:
    """Population Stability Index of one numeric column."""
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, N_BINS + 1)))
    edges[0], edges[-1] = -np.inf, np.inf
    r = np.histogram(ref, edges)[0] / len(ref)
    c = np.histogram(cur, edges)[0] / len(cur)
    r, c = np.clip(r, 1e-6, None), np.clip(c, 1e-6, None)
    return float(np.sum((c - r) * np.log(c / r)))
```

Check it, then run it for two weeks:

```bash
ubunye plan -d pipelines -u fraud -p drift -t psi --backend pandas -dt 2026-10-26
ubunye run  -d pipelines -u fraud -p drift -t psi --backend pandas -dt 2026-10-26 --lineage
ubunye run  -d pipelines -u fraud -p drift -t psi --backend pandas -dt 2026-11-02 --lineage
```

What success looks like:

```
Task fraud/drift/psi   (backend: pandas)
  Inputs
    current      s3 parquet     lake/fraud/features.parquet
                 found: 1 file(s), 9,887 bytes
    reference    s3 parquet     lake/fraud/training_set.parquet
                 found: 1 file(s), 9,957 bytes
  Transform  DriftTask  (transformations.py), written for pandas
  Outputs
    drift_alerts s3 parquet     lake/fraud/drift_alerts
                 mode append
    drift_metrics s3 parquet     lake/fraud/drift_metrics
                 mode append

Plan OK: 1 task(s) can run.
Starting task: psi (Mode: DEV, Deploy: client)
[OK] Run complete for psi
Starting task: psi (Mode: DEV, Deploy: client)
[OK] Run complete for psi
```

`lake/fraud/drift_metrics` now holds both weeks. The second week failed on two features:

```
  run_date        feature    psi status
2026-11-02 amount_sum_30d 0.7342   FAIL
2026-11-02 avg_amount_30d 0.5754   FAIL
2026-11-02  txn_count_30d 0.0285   PASS
2026-10-26 avg_amount_30d 0.0753   PASS
2026-10-26 amount_sum_30d 0.0699   PASS
2026-10-26  txn_count_30d 0.0520   PASS
```

`lake/fraud/drift_alerts` holds only the two `FAIL` rows.

!!! note "On Spark"
    This transform is written for pandas. On Spark, point the inputs at your
    Delta tables (`format: delta`), filter the window with a `sql:` key or
    `.filter(...)`, and turn just the feature columns into pandas before the PSI
    loop: `sources["current"].select(*FEATURES).toPandas()`. For very large tables,
    take a sample first. See [Writing portable transforms](../guides/portable-transforms.md).

Running the same date again is refused, because it would append the same batch
twice. Add `--rerun` to replace it. See [Safe reruns](../guides/safe-reruns.md).

---

## Step 2: Block promotion when drift failed

The registry has a gate for this: `require_drift_check`. It lets a version move
up only when the model's `metadata()` has `drift_check_passed: True`. So the
training task reads that day's drift rows, and puts the answer on the model
before it registers it.

The model keeps the flag as a plain attribute and returns it from `metadata()`:

```python
# pipelines/fraud/ml/train/model.py
from pathlib import Path

import joblib
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import f1_score, roc_auc_score

from ubunye.models.base import UbunyeModel

FEATURES = ["amount_sum_30d", "txn_count_30d", "avg_amount_30d", "risk_encoded"]


def _pandas(df):
    return df.toPandas() if hasattr(df, "toPandas") else df


class FraudRiskModel(UbunyeModel):
    def __init__(self):
        self._clf = GradientBoostingClassifier(n_estimators=100, max_depth=3, random_state=0)
        self.drift_check_passed = False  # set by the training task before register()

    def train(self, df) -> dict:
        pdf = _pandas(df)
        fit, hold = pdf.iloc[: len(pdf) * 3 // 4], pdf.iloc[len(pdf) * 3 // 4 :]
        self._clf.fit(fit[FEATURES], fit["is_fraud"])
        X, y = hold[FEATURES], hold["is_fraud"]  # score on rows it did not see
        return {
            "auc": round(float(roc_auc_score(y, self._clf.predict_proba(X)[:, 1])), 4),
            "f1": round(float(f1_score(y, self._clf.predict(X))), 4),
        }

    def predict(self, df):
        pdf = _pandas(df).copy()
        pdf["fraud_score"] = self._clf.predict_proba(pdf[FEATURES])[:, 1]
        return pdf

    def save(self, path: str) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)
        joblib.dump(self._clf, f"{path}/model.pkl")

    @classmethod
    def load(cls, path: str) -> "FraudRiskModel":
        m = cls()
        m._clf = joblib.load(f"{path}/model.pkl")
        return m

    def metadata(self) -> dict:
        return {
            "library": "scikit-learn",
            "library_version": "1.9",
            "features": FEATURES,
            "params": {"n_estimators": 100, "max_depth": 3},
            "drift_check_passed": self.drift_check_passed,
        }
```

The training task reads the drift history as a second input:

```yaml
# pipelines/fraud/ml/train/config.yaml
MODEL: ml
VERSION: "1.0.0"

CONFIG:
  inputs:
    training_set:
      format: s3
      path: lake/fraud/training_set.parquet
      file_format: parquet
    drift_metrics:          # written by fraud/drift/psi
      format: s3
      path: lake/fraud/drift_metrics
      file_format: parquet

  transform:
    params:
      run_date: "{{ dt }}"

  outputs:
    training_report:
      format: s3
      path: lake/fraud/training_report
      file_format: parquet
      mode: append
```

```python
# pipelines/fraud/ml/train/transformations.py
import pandas as pd

from model import FraudRiskModel
from ubunye.core.errors import PromotionBlockedError
from ubunye.core.interfaces import Task
from ubunye.models import ModelRegistry, ModelStage

GATES = {"min_auc": 0.85, "min_f1": 0.80, "require_drift_check": True}


class TrainTask(Task):
    def transform(self, sources: dict) -> dict:
        run_date = self.config["CONFIG"]["transform"]["params"]["run_date"]

        # 1. Did today's drift check pass? Every feature must say PASS.
        drift = sources["drift_metrics"]
        today = drift[drift["run_date"] == run_date]
        drift_ok = len(today) > 0 and bool((today["status"] == "PASS").all())

        # 2. Train, and put the drift result where the gate looks for it.
        model = FraudRiskModel()
        metrics = model.train(sources["training_set"])
        model.drift_check_passed = drift_ok

        # 3. Register (stores the gates with the model), then try to promote.
        registry = ModelRegistry(".ubunye/model_store")
        mv = registry.register(
            "fraud", "FraudRiskModel", None, model, metrics, promotion_gates=GATES
        )
        try:
            registry.promote("fraud", "FraudRiskModel", mv.version, ModelStage.STAGING)
            stage, reason = "staging", ""
        except PromotionBlockedError as err:
            stage, reason = "development", str(err)

        report = {"run_date": run_date, "version": mv.version, **metrics,
                  "drift_check_passed": drift_ok, "stage": stage, "blocked_by": reason}
        return {"training_report": pd.DataFrame([report])}
```

The gates use `auc` and `f1` because those are the metrics `train()` returns.
A gate on a metric the model does not return (say `max_loss`) always blocks.
The gate rules are on [Model Registry: Promotion gates](../ml/registry.md#promotion-gates).

This version catches the block and writes it to the report, so the run still
succeeds. To make the run fail instead, leave out the `try`.

Train once for each week:

```bash
ubunye run -d pipelines -u fraud -p ml -t train --backend pandas -dt 2026-10-26 --lineage
ubunye run -d pipelines -u fraud -p ml -t train --backend pandas -dt 2026-11-02 --lineage
ubunye models list --use-case fraud --model FraudRiskModel --store .ubunye/model_store
```

What success looks like. The week with no drift reached `staging`; the week
with drift stayed in `development`:

```
  Version      Stage          Registered                   Key metrics
  ------------ -------------- ---------------------------- ------------------------------
  1.0.1        development    2026-11-10T10:38:01.505673   auc=0.983, f1=0.985
  1.0.0        staging        2026-11-10T10:37:59.002519   auc=0.983, f1=0.985
```

`lake/fraud/training_report` says why:

```
  run_date version    auc     f1  drift_check_passed       stage
2026-10-26   1.0.0 0.9831 0.9851                True     staging
2026-11-02   1.0.1 0.9831 0.9851               False development
```

and its `blocked_by` column for 1.0.1 holds:

```
Promotion blocked — 1 gate(s) failed:
  - require_drift_check: Drift check: not found or failed in metadata.

  Version:      1.0.1
  Target stage: staging
  Hint: Fix the failing gates or adjust thresholds in promotion_gates config.
```

---

## Step 3: Look at drift over time

The history is a normal table. On the pandas sample:

```python
import pandas as pd

pd.read_parquet("lake/fraud/drift_metrics").sort_values(["run_date", "psi"], ascending=False)
```

On a Delta lake:

```sql
SELECT feature, psi, status, run_date
FROM delta.`s3://datalake/ml/drift/fraud/`
ORDER BY run_date DESC, psi DESC
```

Run records do not carry the PSI values, so the lineage CLI cannot filter on
them. It can list the drift task's runs, or only the failed ones:

```bash
ubunye lineage list   -d pipelines -u fraud -p drift -t psi
ubunye lineage search -d pipelines -u fraud -p drift -t psi --status error
```

```
------------------------------------------------------------
Task: fraud/drift/psi
------------------------------------------------------------
RUN_ID     STARTED              STATUS        DUR  ROWS
------------------------------------------------------------
1297ad62  2026-11-10T10:37:59  success      0.0s  in:1200  out:5
6484a76e  2026-11-10T10:37:57  success      0.0s  in:1200  out:3
```

To make the drift run itself fail when PSI crosses a line, put an
[expectation](../config/expectations.md) on the `psi` column (see
[Gate on a number](gate.md#gate-on-a-number-accuracy-agreement-error-rate)).

---

## Alerting

Step 1 already writes the `FAIL` rows to their own output, `drift_alerts`. To
send them to a database that an alerting tool watches, point that output at a
JDBC table instead:

```yaml
  outputs:
    drift_alerts:
      format: jdbc
      url: "jdbc:postgresql://{{ env.DB_HOST }}/mlops"
      table: public.drift_alerts
      mode: append
```

The JDBC keys are on [JDBC](../connectors/jdbc.md).

---

## Promotion gates

The gate keys (`min_<metric>`, `max_<metric>`, `require_drift_check`), what
happens when one fails, and how gates stored at `register()` apply to every
version of the model are all on
[Model Registry: Promotion gates](../ml/registry.md#promotion-gates).
