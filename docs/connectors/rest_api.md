# REST API Connector

Reads from and writes to HTTP REST endpoints.
Supports pagination, authentication, rate limiting, retries, and optional schema enforcement.

---

## Read

### Minimal example

```yaml
CONFIG:
  inputs:
    customers:
      format: rest_api
      url: "https://api.example.com/v1/customers"
```

### Full example

```yaml
CONFIG:
  inputs:
    customer_data:
      format: rest_api
      url: "https://api.example.com/v1/customers"
      method: GET                        # GET (default) | POST

      headers:
        Accept: application/json

      params:                            # query string parameters
        since: "{{ dt | default('2025-01-01') }}"
        status: active

      auth:
        type: bearer                     # bearer | api_key | basic
        token: "{{ env.API_TOKEN }}"

      pagination:
        type: offset                     # offset | cursor | next_link
        page_size: 100
        max_pages: 50

      response:
        root_key: data                   # extract records from response["data"]

      rate_limit:
        requests_per_second: 10
        retry_on: [429, 503]
        max_retries: 3

      schema:                            # optional — inferred if omitted
        - name: customer_id
          type: string
        - name: created_at
          type: timestamp
        - name: email
          type: string
```

---

## Authentication

=== "Bearer token"

    ```yaml
    auth:
      type: bearer
      token: "{{ env.API_TOKEN }}"
    ```

=== "API key (header)"

    ```yaml
    auth:
      type: api_key
      header: X-Api-Key
      key: "{{ env.API_KEY }}"
    ```

=== "API key (query param)"

    ```yaml
    auth:
      type: api_key
      param: api_key
      key: "{{ env.API_KEY }}"
    ```

=== "Basic auth"

    ```yaml
    auth:
      type: basic
      username: "{{ env.API_USER }}"
      password: "{{ env.API_PASS }}"
    ```

---

## Pagination strategies

=== "Offset"

    Increments `offset` (or `page`) by `page_size` until fewer than `page_size` records
    are returned.

    ```yaml
    pagination:
      type: offset
      page_size: 200
      max_pages: 100      # safety cap
    ```

=== "Cursor"

    Reads a cursor field from each response and passes it as a query parameter
    in the next request.

    ```yaml
    pagination:
      type: cursor
      cursor_field: next_cursor      # response JSON key containing the cursor
      page_size: 500
    ```

=== "Next link"

    Follows a URL field in each response until the field is absent or `null`.

    ```yaml
    pagination:
      type: next_link
      link_field: next              # response JSON key containing the next URL
    ```

=== "None (single request)"

    Omit the `pagination` key entirely.

---

## Response extraction

If the API returns records nested inside a key:

```json
{ "data": [...], "meta": { "total": 500 } }
```

```yaml
response:
  root_key: data
```

Without `root_key`, the entire response body is treated as the record (or list of records).

---

## Schema

If omitted, the connector infers schema from the first page of records.
For production pipelines, declare the schema explicitly for stability:

```yaml
schema:
  - name: id
    type: long
  - name: name
    type: string
  - name: score
    type: double
  - name: created_at
    type: timestamp
```

Supported types: `string`, `integer`, `long`, `float`, `double`, `boolean`, `timestamp`, `date`, `binary`.

---

## Write

```yaml
CONFIG:
  outputs:
    predictions_api:
      format: rest_api
      url: "https://api.example.com/v1/predictions"
      method: POST
      headers:
        Content-Type: application/json
      auth:
        type: bearer
        token: "{{ env.API_TOKEN }}"
      batch_size: 100            # records per request
      rate_limit:
        requests_per_second: 5
        retry_on: [429, 503]
        max_retries: 3
```

The writer batches rows into JSON payloads of `batch_size` records and POSTs each batch.

---

## Read fields reference

| Field | Type | Default | Description |
|---|---|---|---|
| `url` | string | required | API endpoint URL |
| `method` | `GET` \| `POST` | `GET` | HTTP method |
| `headers` | dict | `{}` | Additional HTTP headers |
| `params` | dict | `{}` | Query string parameters |
| `auth` | dict | `null` | Authentication config |
| `pagination` | dict | `null` | Pagination strategy |
| `response.root_key` | string | `null` | Key to extract records from |
| `rate_limit` | dict | `null` | Rate limiting and retry config |
| `schema` | list of dicts | `null` | Field name + type declarations |

---

## Example — customer sync pipeline

```yaml
MODEL: etl
VERSION: "1.0.0"

CONFIG:
  inputs:
    crm_customers:
      format: rest_api
      url: "https://crm.example.com/api/v2/customers"
      auth:
        type: bearer
        token: "{{ env.CRM_TOKEN }}"
      pagination:
        type: next_link
        link_field: next
      response:
        root_key: results

  transform:
    type: noop

  outputs:
    customers_delta:
      format: delta
      path: s3://datalake/crm/customers/
      mode: overwrite
```
