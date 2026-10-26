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
    dev:                               # overrides applied with -m dev
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
| `profiles` | `Dict[str, EngineProfile]` | `{}` | Named profiles that override `spark_conf`, `catalog` and `schema_name` |
| `catalog` | string | `null` | Catalog to switch to before the task runs (`USE CATALOG`) |
| `schema_name` | string | `null` | Schema to switch to before the task runs (`USE SCHEMA`) |

### `EngineProfile`

| Field | Type | Default | Description |
|---|---|---|---|
| `spark_conf` | `Dict[str, str]` | `{}` | Spark conf overrides for this profile |
| `catalog` | string | `null` | Replaces `ENGINE.catalog` for this profile |
| `schema_name` | string | `null` | Replaces `ENGINE.schema_name` for this profile |

`catalog` and `schema_name` apply on a Spark backend only. Open-source Spark has
no `USE CATALOG`; when the switch fails, the engine logs it and carries on, so
fully qualified table names still work.

---

## Profile merge rules

When `-m <name>` (`--mode`) is passed, the engine calls `merged_spark_conf(name)`:

1. Start with the base `ENGINE.spark_conf` dict.
2. Update with the named profile's `spark_conf` (profile values **win** on conflict).

Profile-only keys are additive; base keys not overridden by the profile are kept.
A profile's `catalog` or `schema_name`, when set, replaces the base value.

---

## Using profiles at runtime

`run` picks a profile with `-m` (`--mode`). `validate` takes `-m` or `--profile`
(the same thing). The name must match a key under `profiles:` exactly, including
its case.

`-m` defaults to `DEV` (upper case). When a config defines `profiles:` and the
mode names none of them, the run stops with `ConfigProfileError` and lists the
valid names. So with profiles named `dev` and `prod`, always pass `-m`. A config
with no `profiles:` ignores the mode and uses the base `spark_conf`.

```bash
# Development: small cluster, few shuffle partitions
ubunye run -d pipelines -u fraud -p etl -t claims -m dev

# Production: full cluster
ubunye run -d pipelines -u fraud -p etl -t claims -m prod
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
    All values must be **strings**: Spark's configuration API accepts only strings.
    Use `"true"` not `true`, `"200"` not `200`.

!!! warning "Settings fixed when the JVM starts"
    Keys such as `spark.sql.extensions`, `spark.jars`, `spark.driver.*` and
    `spark.sql.catalogImplementation` only take effect when Ubunye creates the
    Spark session (`ubunye run` with the `spark` backend). On Databricks, or any
    session the platform started, such a key in `spark_conf` is refused with an
    error. Set them on the platform (cluster settings, `spark-submit --conf`).
    A config may not set `spark.master` when the platform already chose one.

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
