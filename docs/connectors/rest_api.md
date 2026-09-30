# REST API Connector

Reads from and writes to HTTP REST endpoints.
Supports pagination, authentication, rate limiting, retries, and optional schema enforcement.

It runs on every shipped backend: `spark`, `databricks` and `pandas`. On a laptop,
`--backend pandas` pulls from an API with no Spark and no Java. Both backends give
the same rows and types (see [Types](#types)).

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
        type: bearer                     # bearer | api_key_header | api_key_query | basic
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
          type: string          # text; convert it in the transform
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
      type: api_key_header
      header: X-Api-Key          # the default
      key: "{{ env.API_KEY }}"
    ```

=== "API key (query param)"

    ```yaml
    auth:
      type: api_key_query
      param: api_key             # the default
      key: "{{ env.API_KEY }}"
    ```

=== "Basic auth"

    ```yaml
    auth:
      type: basic
      username: "{{ env.API_USER }}"
      password: "{{ env.API_PASS }}"
    ```

Secrets stay out of logs and error messages. The auth token, key and password,
an `Authorization` header or a header with a secret-looking name, and a query
parameter with a secret-looking name (`access_token`, `api_key`...) are masked as
`***` in every log line the connector and the HTTP library write, and in every
error it raises. An `api_key_query` key travels in the URL, so this matters most
there. A secret under a plain parameter name (`key`, `sig`) is masked only when
it is the auth `key`; put keys in `auth`, not in `params`.

An auth `type` the connector does not know is refused by `ubunye validate` and
before any request, so a typo never sends a request without its key. The old
name `type: api_key` still works, with a warning: it means `api_key_header` when
`header` is set and `api_key_query` when `param` is set.

---

## Pagination strategies

=== "Offset"

    Sends `offset` (name it with `offset_param`) starting at 0, and adds
    `page_size` after each page, until a page comes back empty or `max_pages` is
    reached. `page_size` is not sent to the API: put the API's own page size
    parameter under `params` if it needs one.

    ```yaml
    params:
      limit: 200
    pagination:
      type: offset
      page_size: 200
      max_pages: 100      # safety cap
    ```

=== "Cursor"

    Reads a cursor from each response and sends it as a query parameter in the
    next request, until the cursor is missing or empty.

    ```yaml
    pagination:
      type: cursor
      cursor_response_key: next_cursor   # the response's key holding the cursor (default)
      cursor_param: cursor               # the query parameter it is sent as (default)
      max_pages: 100
    ```

=== "Next link"

    Follows a URL in each response until it is missing or `null`. The URL must be
    a top level key of the response body; a dotted path and the HTTP `Link` header
    are not read.

    ```yaml
    pagination:
      type: next_link
      next_key: next                 # the response's key holding the next URL (default)
    ```

The old names `cursor_field` (for `cursor_response_key`) and `link_field` (for
`next_key`) still work, with a warning.

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

## Types

Without a `schema`, the types come from every record, by Spark's own rules
(`createDataFrame`). The pandas backend follows the same rules, so a task gives the
same frame on both:

| JSON value | Column type |
|---|---|
| whole number | `bigint` |
| decimal number | `double` |
| text | `string` |
| `true` / `false` | `boolean` |
| object | `map<string, ...>` (not a struct) |
| array | `array<...>` |

- Columns are in name order. A key that first appears in a later record goes last.
- A field that is a number in some records and text in others is `string`: `7`
  becomes `"7"`, `true` becomes `"true"`.
- A map has no entry order. pandas keeps the JSON's order. Spark does not keep
  one you can rely on: Spark classic takes it from a Java HashMap, and that order
  changed between Java 11 and Java 21; Spark Connect keeps the JSON's. Do not
  depend on it. The run record's hash sorts maps by key, so it does not either.
  On pandas a map cell is a list of `(key, value)` pairs; `dict(cell)` makes it a
  dict.

Spark refuses some records, and so does pandas, with the same error:

- a field that is a whole number in one record and a decimal in another
  (`CANNOT_MERGE_TYPE`);
- a field that is null in every record (`CANNOT_DETERMINE_TYPE`);
- a field that is an object in one record and a number in another.

For any of these, declare the field under `schema`. `string` takes any value, as text.

Known differences, on records where Spark 3.5 and Spark 4 also disagree:

- Spark 3.5 writes a large or tiny decimal number in a text column the Java way
  (`1.0E7`), where Spark 4 and pandas write `10000000.0`.
- Spark 3.5 takes a map's value type from its first non-null entry only. Spark 4
  and pandas look at every entry, so `{"a": 1, "b": "x"}` is `map<string,string>`
  there. On Spark 3.5, give a map one kind of value.

Records with no fields (`[{}, {}]`) give one row each and no columns, on both.

A whole number outside the 64 bit range (past 9223372036854775807) is refused on
pandas, with the field's name. Spark classic stores null there without a word.
Declare the field as `string` to keep it as text on both.

One difference from every Spark: a record that is a JSON array (`[1, "a"]`)
instead of an object. Spark reads it as a row with columns `_1`, `_2`...; pandas
refuses it. Point `response.root_key` at the list of objects instead.

## Schema

Declare the schema for production pipelines. Only the declared columns are kept,
in the declared order, and a missing field is null:

```yaml
schema:
  - name: id
    type: long
  - name: name
    type: string
  - name: score
    type: double
  - name: created_at
    type: string          # text; convert it in the transform
```

Supported types: `string`, `integer`, `long`, `float`, `double`, `boolean`, `timestamp`, `date`, `binary`.

A value must have the column's kind, as Spark checks it. A whole number in a
`double` or `float` column is read as that decimal number (`1` becomes `1.0`), on
both backends, when a double holds it exactly; a whole number past 2**53 that a
double cannot hold is refused. `timestamp`, `date` and `binary` columns refuse
text, which is all JSON can send. Read such a field as `string` and convert it in
the transform.

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

The writer batches rows into JSON payloads of `batch_size` records and POSTs each batch
as `{"records": [...]}`. A map column is sent as an object. Spark and pandas send the
same payloads, with one JSON form for each kind of value:

| Value | Sent as |
|---|---|
| NaN, `Infinity`, `-Infinity` | `null` (JSON has none of them) |
| timestamp (an instant) | ISO 8601 text in UTC: `"2024-01-02T01:04:05.123456Z"` |
| `timestamp_ntz` (wall clock time, Spark) | ISO 8601 text without an offset |
| date | `"2024-01-02"` |
| decimal | its exact text, as a string: `"12.50"` (a JSON number could lose digits) |
| binary | base64 text |

On pandas a naive timestamp is an instant in the backend's time zone, as everywhere
on the pandas backend, so it is sent with `Z` too.

Each batch is encoded whole before any of it is sent. A row holding a value that
has no JSON form stops the write with an error naming the row and the field; none
of its batch is posted (earlier batches were, and the error says how many).

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
        next_key: next
      response:
        root_key: results

  transform: {}

  outputs:
    customers_delta:
      format: delta
      path: s3://datalake/crm/customers/
      mode: overwrite
```
