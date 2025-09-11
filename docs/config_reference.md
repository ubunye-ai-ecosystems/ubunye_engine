
---

# 📄 `docs/config_reference.md`
```markdown
# Config Reference

Ubunye configs are YAML files with the following sections:

---

## 🔑 Top-level keys

| Key           | Purpose                                  |
|---------------|------------------------------------------|
| `MODEL`       | Type of job (`etl`, `ml`)                |
| `VERSION`     | Version of the config schema             |
| `ENGINE`      | Spark/compute settings                   |
| `CONFIG`      | Task-specific inputs, transforms, outputs|
| `ORCHESTRATION` | Orchestrator metadata (optional)       |

---

## ⚙️ ENGINE
```yaml
ENGINE:
  spark_conf:
    spark.sql.shuffle.partitions: "50"
  profiles:
    dev:
      spark_conf:
        spark.master: "local[*]"
    prod:
      spark_conf:
        spark.master: "yarn"
```
1. CONFIG
   
```yaml
CONFIG:
  inputs:
    claims:
      format: jdbc
      url: jdbc:postgresql://db:5432/insurance
      table: claims
      user: "{{ env.DB_USER }}"
      password: "{{ env.DB_PASS }}"
  transform:
    type: noop
  outputs:
    curated:
      format: delta
      table: main.fraud.curated_claims
      mode: overwrite
      options:
        overwriteSchema: "true"
```
2. ORCHESTRATION
```yaml
ORCHESTRATION:
  type: airflow
  schedule: "@daily"
  retries: 2
  owner: "fraud-team"
  tags: ["fraud", "etl"]

  databricks:
    spark_version: "14.3.x-scala2.12"
    num_workers: 4
    wheel_dbfs_path: "dbfs:/libs/ubunye_engine-0.1.0.whl"
```