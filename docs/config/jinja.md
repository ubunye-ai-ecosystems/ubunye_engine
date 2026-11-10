# Jinja Templating

Ubunye fills in Jinja2 expressions (`{{ ... }}`) in your `config.yaml` **before** the
config is checked against its schema. This lets you inject environment variables, CLI
arguments, dates, and computed values without changing the schema.

---

## How it works

1. `load_config(path, variables)` (in `ubunye.config`) parses the YAML.
2. The resolver renders every string value through Jinja2.
3. The result is checked against the config schema (see
   [Validation](overview.md#validation)).

Jinja expressions can appear in **any string value**: paths, table names, SQL,
header values, tokens, etc. Jinja renders values, not structure: keys and
`{% if %}` blocks around keys are not templated.

The variables a template can use:

| Variable | Where it comes from |
|---|---|
| `dt` | `-dt` / `--data-timestamp` (or `--var dt=...`) |
| `dtf` | `-dtf` / `--data-timestamp-format` |
| `mode` | `-m` / `--mode` (default `DEV`) |
| `task_dir` | the task's own folder, always set |
| `env` | the environment: `{{ env.NAME }}` |
| any other name | `--var name=value` |

A flag that was not given leaves its variable undefined, so `| default(...)` works
for it.

---

## Environment variables

```yaml
CONFIG:
  inputs:
    source:
      format: jdbc
      url: "{{ env.JDBC_URL }}"
      user: "{{ env.DB_USER }}"
      password: "secret://env/DB_PASS"   # a secret: not templated, see Secrets
```

`env` is a dict-like object populated from `os.environ` at load time. Use it for
settings, not for passwords: a templated value becomes part of the rendered config,
while a [`secret://`](secrets.md) reference is read only when the connector runs.

---

## The task's own folder: `{{ task_dir }}`

Every config gets `{{ task_dir }}`, the absolute path of the folder that holds
it. Use it for files that live next to the task:

```yaml
CONFIG:
  inputs:
    people:
      format: s3
      path: "{{ task_dir }}/data/people.csv"
```

A relative path is resolved by the engine's own filesystem: pandas reads it from
the folder you ran the command in, and Spark from where its JVM started (on a
cluster, its default filesystem). `{{ task_dir }}` means the same thing
everywhere, so prefer it for anything next to the task.

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

`--var` works on every command that renders a config: `run`, `validate`, `plan`,
`config` and `test run`. From Python, pass the same thing as `variables=`:

```python
ubunye.run_task("pipelines/fraud/etl/claims", variables={"env_name": "prod"}, dt="2024-06-01")
```

A few rules, so a typo is caught instead of rendering the wrong thing:

- A name must be a valid template name: letters, digits and underscores, not
  starting with a digit (`env_name`, not `env-name`).
- `env` is reserved for the environment (`{{ env.NAME }}`), `task_dir` for the
  task's folder, and `mode` is set with
  `-m`.
- `--var dt=...` works like `-dt`; giving both with different values is refused.
- A value arrives as text: `--var min_age=30` gives the string `'30'`, and a
  rendered value stays text even with `| int`. Convert it in your Python code
  (`int(params["min_age"])`).
- The variables a run used are kept in its run record (`--lineage`).

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
path: "s3://{{ env.BUCKET | lower }}/{{ mode | lower }}/{{ dt | replace('-', '/') }}/"
```

Common filters:

| Filter | Description |
|---|---|
| `default(val)` | Fallback if variable is undefined |
| `upper` | Convert to UPPERCASE |
| `lower` | Convert to lowercase |
| `replace(a, b)` | Replace substring `a` with `b` |
| `trim` | Strip leading/trailing whitespace |
| `ident` | Quote a SQL name once, part by part ([SQL names](#sql-names-ident)) |
| `int`, `float` | Number conversion inside an expression (`{{ (n \| int) + 1 }}`); the rendered value is still text |

---

## SQL names: `ident`

Ubunye adds one filter of its own. `| ident` quotes a table, schema or catalog
name for SQL, once, part by part:

```yaml
CONFIG:
  inputs:
    trips:
      format: unity
      sql: "SELECT * FROM {{ env.TELM_READ_CATALOG | ident }}.raw.trips"
  outputs:
    clean:
      format: unity
      table: "{{ env.TELM_WRITE_TABLE | ident }}"   # a whole catalog.schema.table
```

| The value | Through `ident` |
|---|---|
| `aws-x-catalog` | `` `aws-x-catalog` `` |
| `` `aws-x-catalog` `` | `` `aws-x-catalog` `` (not quoted twice) |
| ` aws-x-catalog ` | `` `aws-x-catalog` `` (spaces dropped) |
| `aws-x-catalog.gold.trips` | `` `aws-x-catalog`.`gold`.`trips` `` |

Use it for every name that comes from a variable. A name with a hyphen must be
quoted in SQL (`aws-x-catalog.raw.trips` unquoted reads as a subtraction), and a
job parameter may arrive with backticks or without them, so writing the backticks
into the config breaks one of the two. `| ident` takes either.

An empty value, or one with an empty part (`a..b`), is refused when the config is
loaded, naming the value. With a default, put the default first:
`{{ env.CAT | default('dev-cat') | ident }}`. `ubunye validate` and `ubunye plan`
print each input's SQL after templating, so you can see the quoted name.

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
    Never hardcode passwords, tokens, or keys in `config.yaml`, and do not template
    them in with `{{ env.SECRET }}` either. Use a `secret://` reference
    (`secret://env/DB_PASS` reads the same environment variable). See
    [Secrets](secrets.md).

!!! tip "Validate after templating"
    `ubunye validate` renders the template and validates the result.
    Run it in CI to catch missing variables early:
    ```bash
    ubunye validate -d pipelines -u fraud -p etl -t claims \
        --var dt=$(date +%F)
    ```

!!! note "A templated value is always a string"
    The YAML is parsed first, then each string is rendered. The result is not
    parsed again, so `"{{ n }}"` gives the string `"8"`, never the number 8, and a
    rendered `"true"` stays text. Write a value that must be a number or a list
    without a template.

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

  transform: {}

  outputs:
    clean_events:
      format: delta
      path: "s3://{{ env.BUCKET }}/clean/events/dt={{ dt | default('2024-01-01') }}/"
      mode: "{{ 'overwrite' if full_refresh | default('false') == 'true' else 'append' }}"
```
