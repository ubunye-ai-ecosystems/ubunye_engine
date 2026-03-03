# Jinja Templating

Ubunye resolves Jinja2 expressions in your `config.yaml` **before** Pydantic validation.
This lets you inject environment variables, CLI arguments, dates, and computed values
without changing the schema.

---

## How it works

1. `ConfigLoader.load(path, variables)` reads the raw YAML.
2. The resolver renders every string value through Jinja2.
3. The rendered YAML is parsed and validated by Pydantic.

Jinja expressions can appear in **any string value** — paths, table names, SQL,
header values, tokens, etc.

---

## Environment variables

```yaml
CONFIG:
  inputs:
    source:
      format: jdbc
      url: "{{ env.JDBC_URL }}"
      user: "{{ env.DB_USER }}"
      password: "{{ env.DB_PASS }}"
```

`env` is a dict-like object populated from `os.environ` at load time.

---

## CLI-injected variables

Pass arbitrary key-value pairs with `--var` (repeatable):

```bash
ubunye run -d pipelines -u fraud -p etl -t claims \
    --var dt=2024-06-01 \
    --var env_name=prod
```

Reference them directly:

```yaml
CONFIG:
  inputs:
    events:
      format: hive
      tbl_name: "events_{{ dt | replace('-', '_') }}"
```

---

## Default filter

Use `| default(value)` to provide a fallback when a variable is not set:

```yaml
db_name: "{{ env.HIVE_DB | default('raw') }}"
tbl_name: "events_{{ dt | default('2024-01-01') | replace('-', '_') }}"
```

---

## String filters

Any Jinja2 filter works:

```yaml
path: "s3://{{ env.BUCKET | lower }}/{{ use_case | upper }}/{{ dt | replace('-', '/') }}/"
```

Common filters:

| Filter | Description |
|---|---|
| `default(val)` | Fallback if variable is undefined |
| `upper` | Convert to UPPERCASE |
| `lower` | Convert to lowercase |
| `replace(a, b)` | Replace substring `a` with `b` |
| `trim` | Strip leading/trailing whitespace |
| `int`, `float` | Type coercion |

---

## Conditional expressions

```yaml
mode: "{{ 'overwrite' if env.RESET | default('false') == 'true' else 'append' }}"
```

---

## Multi-line SQL

```yaml
sql: >-
  SELECT id, amount, event_date
  FROM raw.claims
  WHERE event_date = '{{ dt | default('2024-01-01') }}'
    AND status = '{{ env.CLAIM_STATUS | default('OPEN') }}'
```

---

## Best practices

!!! warning "Never commit secrets"
    Use `{{ env.SECRET }}` to reference secrets from the environment.
    Never hardcode passwords, tokens, or keys in `config.yaml`.

!!! tip "Validate after templating"
    `ubunye validate` renders the template and validates the result.
    Run it in CI to catch missing variables early:
    ```bash
    ubunye validate -d pipelines -u fraud -p etl -t claims \
        --var dt=$(date +%F)
    ```

!!! note "Values must still be valid YAML after rendering"
    Jinja renders to a string, which is then parsed as YAML.
    Make sure numeric values that must stay numeric are quoted or cast appropriately.

---

## Full example

```yaml
MODEL: etl
VERSION: "1.0.0"

ENGINE:
  spark_conf:
    spark.sql.warehouse.dir: "{{ env.HIVE_WAREHOUSE | default('/user/hive/warehouse') }}"

CONFIG:
  inputs:
    raw_events:
      format: hive
      db_name: "{{ env.RAW_DB | default('raw') }}"
      tbl_name: "events_{{ dt | default('2024-01-01') | replace('-', '_') }}"

  transform:
    type: task

  outputs:
    clean_events:
      format: delta
      path: "s3://{{ env.BUCKET }}/clean/events/dt={{ dt | default('2024-01-01') }}/"
      mode: "{{ 'overwrite' if full_refresh | default('false') == 'true' else 'append' }}"
```
