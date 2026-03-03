# Pattern: REST API Ingestion

Pull data from third-party APIs, normalise it, and land it in your data lake.
This pattern covers single-request, paginated, and incremental ingestion strategies.

---

## Single-request ingestion

For small payloads that fit in one response:

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

  transform:
    type: noop

  outputs:
    rates:
      format: delta
      path: s3://datalake/forex/rates/
      mode: append
```

---

## Paginated ingestion (offset)

For APIs that return large datasets across multiple pages:

```yaml
CONFIG:
  inputs:
    orders:
      format: rest_api
      url: "https://api.shopify.example.com/v1/orders"
      auth:
        type: api_key
        header: X-Shopify-Access-Token
        key: "{{ env.SHOPIFY_TOKEN }}"
      pagination:
        type: offset
        page_size: 250
        max_pages: 200
      response:
        root_key: orders
      schema:
        - name: id
          type: long
        - name: created_at
          type: timestamp
        - name: total_price
          type: double
        - name: currency
          type: string
        - name: status
          type: string
```

---

## Incremental ingestion (cursor / date filter)

Pass the last-seen cursor or date as a Jinja variable:

```bash
ubunye run -d pipelines -u crm -p ingest -t contacts \
    --var since=2024-06-01 \
    --profile prod \
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
        token: "{{ env.CRM_TOKEN }}"
      pagination:
        type: next_link
        link_field: _links.next.href
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
        password: "{{ env.MP_SECRET }}"
      pagination:
        type: cursor
        cursor_field: next_page_token
        page_size: 1000
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
      headers:
        Authorization: "Bearer {{ env.GITHUB_TOKEN }}"
        X-GitHub-Api-Version: "2022-11-28"
      pagination:
        type: next_link
        link_field: next          # parsed from Link header (or response body)
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
      headers:
        Content-Type: application/json
        Authorization: "Bearer {{ env.ES_TOKEN }}"
      params:
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
        token: "{{ env.CRM_TOKEN }}"
      batch_size: 200
      rate_limit:
        requests_per_second: 10
        retry_on: [429, 503]
        max_retries: 3
```

---

## Handling nested JSON

The `response.root_key` only extracts one level. For deeply nested responses,
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

Run nightly via Airflow:

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
    -o dags/crm_contacts.py --profile prod
```
