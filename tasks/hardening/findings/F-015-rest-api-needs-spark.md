# F-015: the REST connector needs Spark; a laptop cannot pull from an API

**Status:** fixed on fix/f015-rest-on-pandas (off hardening/real-world b6ff30b)
**Severity:** major
**Source:** experiment E-04 (2026-09-28)
**Promise:** 1 (same task anywhere)

## What happens
`ubunye plan` and `run` on the pandas backend refuse `format: rest_api`: "the
'rest_api' connector, which needs spark; the pandas backend does not provide it."
Pulling JSON from an HTTP API is one of the most common first tasks, and the backend
for small machines cannot do it.

## Repro
tests/experiments/e04_secrets.py <ubunye> pandas

## Expected
The REST reader (and writer) on pandas: the HTTP, auth, pagination and retry logic is
not Spark-specific; only the final frame is. Same rows as Spark, checked by the
parity-checker.

## Evidence
The refusal above (clear, which is good).

## Fix
The HTTP side was never Spark's. It is now one module, `ubunye/plugins/rest_http.py`
(session, headers, auth, rate limit, retries, the `requests` import), used by the
reader and the writer on every backend. Pagination and record extraction stay in
the reader, unchanged. Only the last step is the backend's, through two new `Backend`
methods behind a new capability, `records`:

- `frame_from_records(records, schema=None)`: Spark calls `createDataFrame`, as
  before. pandas uses `ubunye/adapters/pandas_records.py`, a port of PySpark's own
  inference and schema check (`_infer_type`, `_infer_schema`, `_merge_type`,
  `_create_converter`, `_make_type_verifier`, read in pyspark 4.2 and 3.5.8). The
  config's `schema:` list becomes a Spark DDL string that both backends read.
- `iter_records(frame)`: Spark uses `toLocalIterator` and `Row.asDict(recursive=True)`,
  as before; pandas goes through Arrow a batch at a time, maps back to dicts.

The spark, databricks and pandas backends declare `records`; the reader and the
writer require it instead of `spark` (the writer declared nothing before).

Spark's rules, now pandas' too: keys sorted per record, a new key added at the end;
`bigint`, `double`, `boolean`, `string`; an object is a `map` (not a struct), an array
an `array`; a number mixed with text is `string` (`7` becomes `"7"`); a whole number
mixed with a decimal, or a field null in every record, is refused. A map's entry order
is Java's `HashMap` order, because Pyrolite unpickles a dict as `new HashMap(0)` and
`ArrayBasedMapData` iterates its entry set (both read from the Spark 4.2 jars with
`javap`); `java_map_order` reproduces it and is checked against Java 11 on eight key
sets.

Differences, documented in `docs/connectors/rest_api.md`: Spark 3.5 turns a decimal
number in a text column into text on the JVM (`1.0E7`), Spark 4 and pandas in Python
(`10000000.0`). Eight or more map keys in one hash bucket is not modelled.

Evidence:

- `tests/experiments/e04_secrets.py <ubunye> pandas`: before, `plan --json` and `run`
  exit 1 with the refusal and no secret reaches the API; after, both exit 0, all six
  secrets are used by the run, none is found in any artifact, and the outputs hold
  the served rows.
- `tests/unit/connectors/test_rest_api_pandas.py`: 25 tests against a local HTTP
  server (offset, cursor, next_link, explicit schema, refused records, empty
  response, writer batches, a whole task through `run_task`, HashMap order). All 25
  fail on b6ff30b and pass on the fix. They pass on the minimum versions too
  (pandas 2.2.0, pyarrow 14), except the `run_task` one on Windows, where the pandas
  backend refuses pyarrow before 24 by design.
- Inference cross-checked offline against pyspark's own pure Python functions (no
  JVM): the same schema on 4 record sets with 4.2 and 3.5.8, the same values with
  4.2, and the same refusals.
- `tests/integration/test_rest_api_parity.py` (runs in CI on Spark 4 and 3.5): the
  same served JSON read on live Spark and on pandas with each pagination and with a
  schema must give the same rows, in the same order, including map entry order, and
  the same `rows-v1` schema and data hash; the refused record sets are refused on
  both; the writer posts byte-identical payloads. Not run on the dev box (no local
  Spark by rule); CI decides.

## Review fixes (skeptic, on d668f8b)

The skeptic proved six bugs in the fix and older gaps in the connector. Each is
fixed in its own commit on this branch.

1. **The hash of a map column differed on the two engines.** The pandas side left
   null map values out (Spark's `to_json` writes them) and hashed a map in its
   entry order, which no two engines agree on. Now every map is sorted by key
   before hashing, on both sides, recursively, and null values are written (ADR 006
   clarified). Proof: `runtime_attacks.py` case D gave two different data hashes
   for `{"m":{"a":null,"b":"x"}}`; the pandas line was `{"m":{"b":"x"}}`. Tests:
   `TestMaps` in `tests/unit/lineage/test_content_hash.py` (5 fail before, pass
   after); `test_maps_hash_the_same_whatever_their_entry_order` in the integration
   tier (CI).
