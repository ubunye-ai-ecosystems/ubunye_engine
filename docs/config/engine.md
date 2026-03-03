# Engine & Profiles

The `ENGINE` section controls Spark configuration and lets you define per-environment
overrides without duplicating your config.

---

## Structure

```yaml
ENGINE:
  spark_conf:                          # base Spark settings (all profiles)
    spark.sql.shuffle.partitions: "200"
    spark.executor.memory: "4g"

  profiles:
    dev:                               # overrides applied with --profile dev
      spark_conf:
        spark.sql.shuffle.partitions: "4"
        spark.executor.memory: "512m"
    staging:
      spark_conf:
        spark.executor.memory: "8g"
    prod:
      spark_conf:
        spark.executor.memory: "32g"
        spark.executor.cores: "8"
```

---

## Fields

### `EngineConfig`

| Field | Type | Default | Description |
|---|---|---|---|
| `spark_conf` | `Dict[str, str]` | `{}` | Base Spark configuration keys applied in all profiles |
| `profiles` | `Dict[str, EngineProfile]` | `{}` | Named profiles that override `spark_conf` |

### `EngineProfile`

| Field | Type | Default | Description |
|---|---|---|---|
| `spark_conf` | `Dict[str, str]` | `{}` | Spark conf overrides for this profile |

---

## Profile merge rules

When `--profile <name>` is passed, the engine calls `merged_spark_conf(profile)`:

1. Start with the base `ENGINE.spark_conf` dict.
2. Update with the named profile's `spark_conf` (profile values **win** on conflict).

Profile-only keys are additive; base keys not overridden by the profile are kept.

---

## Using profiles at runtime

```bash
# Development — small cluster, few shuffle partitions
ubunye run -d pipelines -u fraud -p etl -t claims --profile dev

# Production — full cluster
ubunye run -d pipelines -u fraud -p etl -t claims --profile prod
```

---

## Common Spark settings

```yaml
ENGINE:
  spark_conf:
    # Shuffle tuning
    spark.sql.shuffle.partitions: "200"
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"

    # Memory
    spark.executor.memory: "8g"
    spark.driver.memory: "4g"
    spark.executor.memoryOverhead: "1g"

    # Delta
    spark.databricks.delta.optimizeWrite.enabled: "true"
    spark.databricks.delta.autoCompact.enabled: "true"

    # Hive
    spark.sql.catalogImplementation: "hive"
    spark.sql.warehouse.dir: "/user/hive/warehouse"
```

!!! note
    All values must be **strings** — Spark's configuration API accepts only strings.
    Use `"true"` not `true`, `"200"` not `200`.

---

## ENGINE is optional

If you omit `ENGINE` entirely the engine uses Spark defaults:

```yaml
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    ...
  outputs:
    ...
```
