# Pattern: REST API Ingestion

For anyone who needs to pull data from a web API (a shop, a CRM, an exchange
rate service) into their lake on a schedule. Each section is one common case,
with a config you can copy.

- **The problem:** the data you need sits behind an HTTP API, spread over many
  pages, behind a token, with rate limits.
- **When to use it:** the API returns JSON and you want it as a table. No code is
  needed for the request itself: the `rest_api` connector does the paging, the
  login and the retries.
- **The one command:** `ubunye run -d pipelines -u shop -p ingest -t orders --lineage`

Every `rest_api` key is listed on [REST API Connector](../connectors/rest_api.md).
This page shows how to put them together.

---

## A full example you can run

A task that reads every order from a shop API, a page of 3 at a time, and lands
them as Parquet:

```
pipelines/                    # the pipelines folder (-d)
└── shop/                     # use case
    └── ingest/               # pipeline
        └── orders/           # task
            ├── config.yaml
            └── transformations.py
```

```yaml
# pipelines/shop/ingest/orders/config.yaml
MODEL: etl
VERSION: "1.0.0"

CONFIG:
  inputs:
    orders:
      format: rest_api
      url: "{{ env.SHOP_API | default('https://api.shop.example.com') }}/v1/orders"
      auth:
        type: api_key_header
        header: X-Shop-Access-Token
        key: "secret://env/SHOP_TOKEN"
      params:
        limit: 3                # the API's own page size parameter
      pagination:
        type: offset
        page_size: 3
        max_pages: 200
      response:
        root_key: orders
      schema:
        - name: id
          type: long
        - name: created_at
          type: string          # text; convert it in the transform
        - name: total_price
          type: double
        - name: currency
          type: string
        - name: status
          type: string

  outputs:
    raw_orders:
      format: s3
      path: lake/shop/raw_orders
      file_format: parquet
      mode: overwrite
```

```python
# pipelines/shop/ingest/orders/transformations.py
from ubunye.core.interfaces import Task


class IngestOrders(Task):
    def transform(self, sources: dict) -> dict:
        return {"raw_orders": sources["orders"]}
```

- `auth` sends the token in the `X-Shop-Access-Token` header. The token comes
  from the `SHOP_TOKEN` environment variable through a
  [secret reference](../config/secrets.md), so it is never in the file, the plan
  or the run record.
- `pagination: offset` asks for `offset=0`, then `3`, then `6`, until a page
  comes back empty.
- `schema` fixes the column types, so an empty or odd page cannot change them.

Check it, then run it:

```bash
export SHOP_TOKEN=...        # your token
ubunye plan -d pipelines -u shop -p ingest -t orders --backend pandas
ubunye run  -d pipelines -u shop -p ingest -t orders --backend pandas --lineage
```

What success looks like (here against a small local test server that holds 7
orders, with `SHOP_API` pointing at it):

```
Task shop/ingest/orders   (backend: pandas)
  Inputs
    orders       rest_api       http://127.0.0.1:38031/v1/orders
                 not checked (not a local path)
  Transform  IngestOrders  (transformations.py)
  Outputs
    raw_orders   s3 parquet     lake/shop/raw_orders
                 mode overwrite

Plan OK: 1 task(s) can run.
Starting task: orders (Mode: DEV, Deploy: client)
[OK] Run complete for orders
```

`lake/shop/raw_orders` holds all 7 orders from the 3 pages:

```
 id           created_at  total_price currency   status
  1 2026-11-01T09:01:00Z        120.5      ZAR     paid
  2 2026-11-01T09:02:00Z        241.0      ZAR     paid
  3 2026-11-02T09:03:00Z        361.5      ZAR refunded
  4 2026-11-02T09:04:00Z        482.0      ZAR     paid
  5 2026-11-02T09:05:00Z        602.5      ZAR     paid
  6 2026-11-03T09:06:00Z        723.0      ZAR refunded
  7 2026-11-03T09:07:00Z        843.5      ZAR     paid
```

`ubunye lineage trace -d pipelines -u shop -p ingest -t orders` shows the API
as the input, with `rows : 7`. A wrong token fails the run at once:

```
[ERROR] Run failed for orders: 401 Client Error: Unauthorized for url: http://127.0.0.1:38031/v1/orders?limit=3&offset=0
```

The same task runs on Spark with no change: drop `--backend pandas`.

---

## Single request

For a small answer that fits in one response, leave `pagination` out:

```yaml
MODEL: etl
VERSION: "1.0.0"

CONFIG:
  inputs:
    exchange_rates:
      format: rest_api
      url: "https://api.exchangerate.host/latest"
      params:
        base: USD
        symbols: EUR,GBP,ZAR,JPY
      response:
        root_key: rates

  outputs:
    rates:
      format: delta
      path: s3://datalake/forex/rates/
      mode: append
```

---

## Incremental ingestion (cursor / date filter)

Fetch only what changed since the last run: pass the last-seen date as a
variable with `--var`, and use it in the config with [Jinja](../config/jinja.md):

```bash
ubunye run -d pipelines -u crm -p ingest -t contacts \
    --var since=2026-11-01 \
    -m PROD \
    --lineage
```

```yaml
CONFIG:
  inputs:
    contacts:
      format: rest_api
      url: "https://crm.example.com/api/contacts"
      params:
        updated_after: "{{ since | default('2020-01-01') }}"
      auth:
        type: bearer
        token: "secret://env/CRM_TOKEN"
      pagination:
        type: next_link
        next_key: next          # a top level key of the response; dotted paths are not read
      response:
        root_key: contacts
```

---

## Cursor-based pagination

For APIs that return a cursor/token to request the next page:

```yaml
CONFIG:
  inputs:
    events:
      format: rest_api
      url: "https://api.mixpanel.com/export/events"
      auth:
        type: basic
        username: "{{ env.MP_USER }}"
        password: "secret://env/MP_SECRET"
      pagination:
        type: cursor
        cursor_response_key: next_page_token
        cursor_param: page_token
```

---

## Rate-limited API

For APIs with strict rate limits:

```yaml
CONFIG:
  inputs:
    github_repos:
      format: rest_api
      url: "https://api.github.com/orgs/my-org/repos"
      auth:
        type: bearer
        token: "secret://env/GITHUB_TOKEN"
      headers:
        X-GitHub-Api-Version: "2022-11-28"
      pagination:
        type: next_link
        next_key: next            # from the response body; the Link header is not read
      rate_limit:
        requests_per_second: 1    # GitHub: 5000 req/hr authenticated
        retry_on: [429, 503]
        max_retries: 5
```

---

## POST with body

For APIs that require POST requests:

```yaml
CONFIG:
  inputs:
    search_results:
      format: rest_api
      url: "https://api.elastic.example.com/search"
      method: POST
      auth:
        type: bearer
        token: "secret://env/ES_TOKEN"
      body:                     # sent as the JSON body; params would go in the URL
        query: "fraud risk"
        size: 100
```

---

## Writing back to an API

After enrichment, send results back to an external system:

```yaml
CONFIG:
  outputs:
    scored_customers:
      format: rest_api
      url: "https://crm.example.com/api/bulk/scores"
      method: POST
      auth:
        type: bearer
        token: "secret://env/CRM_TOKEN"
      batch_size: 200
      rate_limit:
        requests_per_second: 10
        retry_on: [429, 503]
        max_retries: 3
```

---

## Handling nested JSON

The `response.root_key` only extracts one level (see
[Response extraction](../connectors/rest_api.md#response-extraction)). For deeply nested responses,
use a transform to flatten:

```python
from pyspark.sql import functions as F
from ubunye.core.interfaces import Task

class FlattenTask(Task):
    def transform(self, sources: dict) -> dict:
        df = sources["api_data"]
        flat = df.select(
            "id",
            F.col("address.street").alias("street"),
            F.col("address.city").alias("city"),
            F.col("metadata.tags").alias("tags"),
            F.explode("line_items").alias("item"),
        ).select("id", "street", "city", "tags", "item.*")
        return {"api_data": flat}
```

---

## Scheduling

Run nightly via Airflow. Every key and flag is on
[Orchestration](../config/orchestration.md):

```yaml
ORCHESTRATION:
  type: airflow
  schedule: "0 4 * * *"
  retries: 3
  tags: [rest_api, ingestion, crm]
```

```bash
ubunye export airflow \
    -c pipelines/crm/ingest/contacts/config.yaml \
    -o dags/crm_contacts.py --profile PROD
```
