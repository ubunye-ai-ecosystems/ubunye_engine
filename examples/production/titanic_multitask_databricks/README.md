# Titanic Multi-Task Pipeline (Databricks)

Two-task pipeline demonstrating **sequential task chaining** on Databricks with
Unity Catalog. This is the Databricks counterpart of `titanic_multitask_local` —
the `transformations.py` files are **byte-identical** between the two examples
(build once, run everywhere).

## Pipeline shape

```
titanic.csv  -->  [clean_data]  -->  UC: titanic_cleaned  -->  [aggregate]  -->  UC: survival_summary
```

| Task | Reads | Writes | Logic |
|------|-------|--------|-------|
| `clean_data` | Titanic CSV (UC volume) | `titanic_cleaned` Delta table | Drop null survival/class, add `survived_label`, `age_group` |
| `aggregate` | `titanic_cleaned` Delta table | `survival_summary` Delta table | Group by (Pclass, age_group), compute survival rate |

Task chaining uses Unity Catalog tables instead of filesystem paths: task 1
writes to `titanic_cleaned`, task 2 reads the same table via the unity reader.

## What this example exercises

- **`ubunye.run_pipeline()`**: sequential multi-task execution through a single
  Spark backend on Databricks.
- **Unity Catalog readers/writers**: intermediate and final outputs are managed
  Delta tables.
- **Portability contract**: `transformations.py` is identical to the local
  example — only `config.yaml` changes (s3 → unity).
- **Serverless compute**: no cluster spec in `databricks.yml`.

## Deploying

```bash
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<pat>

cd examples/production/titanic_multitask_databricks

# Validate the bundle
databricks bundle validate --target nonprod

# Deploy to Databricks
databricks bundle deploy --target nonprod

# Run the job
databricks bundle run titanic_multitask --target nonprod
```

## Running tests

From the repo root:

```bash
pip install -e ".[spark,dev]"
pytest examples/production/titanic_multitask_databricks/tests -v
```

Seven tests on a local SparkSession. They need Java 17 or newer.
