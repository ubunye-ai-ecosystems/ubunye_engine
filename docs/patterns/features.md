# Pattern: Feature Store Pipeline

A feature store pipeline reads raw events, computes features, and writes them
to a Delta table (or Unity Catalog) that training and scoring jobs share.

---

## Architecture

```
Raw events (Hive / S3)
    → Feature ETL task (aggregation, encoding)
    → Delta feature table (point-in-time correct)
    → Training job reads features
    → Scoring job reads latest features
```

---

## Feature computation task

```yaml
# pipelines/fraud/features/transaction/config.yaml
MODEL: etl
VERSION: "1.0.0"

ENGINE:
  profiles:
    dev:
      spark_conf:
        spark.sql.shuffle.partitions: "4"
    prod:
      spark_conf:
        spark.sql.shuffle.partitions: "400"

CONFIG:
  inputs:
    raw_transactions:
      format: hive
      db_name: raw
      tbl_name: transactions

    customer_profiles:
      format: hive
      db_name: raw
      tbl_name: customers

  transform:
    type: task

  outputs:
    transaction_features:
      format: delta
      path: s3://datalake/features/transaction/
      mode: overwrite
```

`transformations.py`:

```python
from pyspark.sql import functions as F, Window
from ubunye.core.interfaces import Task

class FeatureTask(Task):
    def transform(self, sources: dict) -> dict:
        txn = sources["raw_transactions"]
        cust = sources["customer_profiles"]

        # Rolling 30-day aggregations
        w30 = (
            Window.partitionBy("customer_id")
                  .orderBy(F.col("event_ts").cast("long"))
                  .rangeBetween(-30 * 86400, 0)
        )

        features = (
            txn.withColumn("amount_sum_30d", F.sum("amount").over(w30))
               .withColumn("txn_count_30d",  F.count("*").over(w30))
               .withColumn("avg_amount_30d",  F.avg("amount").over(w30))
               .join(cust.select("customer_id", "risk_segment"), "customer_id", "left")
               .withColumn("risk_encoded",
                   F.when(F.col("risk_segment") == "HIGH", 2)
                    .when(F.col("risk_segment") == "MED",  1)
                    .otherwise(0))
               .select(
                   "customer_id", "event_ts",
                   "amount_sum_30d", "txn_count_30d", "avg_amount_30d",
                   "risk_encoded",
               )
        )

        return {"transaction_features": features}
```

---

## Point-in-time correct feature lookup

For training, join features to labels **as of the label timestamp**:

```yaml
# pipelines/fraud/features/training_set/config.yaml
CONFIG:
  inputs:
    features:
      format: delta
      path: s3://datalake/features/transaction/

    labels:
      format: hive
      db_name: ml
      tbl_name: fraud_labels

  transform:
    type: task

  outputs:
    training_set:
      format: delta
      path: s3://datalake/ml/training_sets/fraud/
      mode: overwrite
```

`transformations.py`:

```python
from pyspark.sql import functions as F
from ubunye.core.interfaces import Task

class TrainingSetTask(Task):
    def transform(self, sources: dict) -> dict:
        features = sources["features"]
        labels   = sources["labels"]

        # Asof join: latest feature snapshot before the label timestamp
        training_set = (
            labels.join(features, "customer_id")
                  .filter(F.col("feature_ts") <= F.col("label_ts"))
                  .groupBy("customer_id", "label_ts", "is_fraud")
                  .agg(F.max("feature_ts").alias("feature_ts"))
                  .join(features, ["customer_id", "feature_ts"])
        )
        return {"training_set": training_set}
```

---

## Serving latest features for scoring

```yaml
# pipelines/fraud/features/score_features/config.yaml
CONFIG:
  inputs:
    features:
      format: delta
      path: s3://datalake/features/transaction/

  transform:
    type: task

  outputs:
    latest_features:
      format: delta
      table: main.fraud.latest_features
      mode: overwrite
```

`transformations.py`:

```python
from pyspark.sql import functions as F
from ubunye.core.interfaces import Task

class LatestFeaturesTask(Task):
    def transform(self, sources: dict) -> dict:
        df = sources["features"]
        latest = df.groupBy("customer_id").agg(
            F.max_by(F.struct(df.columns), "event_ts").alias("latest")
        ).select("customer_id", "latest.*")
        return {"latest_features": latest}
```

---

## Schedule with Airflow

```yaml
ORCHESTRATION:
  type: airflow
  schedule: "0 1 * * *"   # 01:00 UTC — runs before the training job at 02:00
  retries: 2
  tags: [features, fraud]
```

```bash
ubunye export airflow \
    -c pipelines/fraud/features/transaction/config.yaml \
    -o dags/fraud_features.py --profile prod
```
