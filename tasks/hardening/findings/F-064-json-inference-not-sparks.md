# F-064: JSON on pandas is typed by pyarrow, not by Spark's rules

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** major (crashes where Spark reads, and a silent schema difference)
**Source:** experiment E-09 (awkward data), shape 3 (nested JSON) and 9 (big numbers)
**Promise:** 1 (same result anywhere)

## What happens
The pandas JSON reader sorted the keys and let `pa.array` infer the types. Five ways
that differs from Spark, found with API-style data:

| Input | pandas before | Spark |
|---|---|---|
| `{"o":{"b":1}}` then `{"o":{"a":2}}` | `struct<b, a>` (first seen order) | `struct<a, b>` (sorted) |
| `{"a":1}` then `{"a":"x"}` | stops: "Could not convert 'x'" | `string`: `"1"`, `"x"` |
| `{"a":{"k":1}}` then `{"a":"x"}` | stops: "cannot mix struct and non-struct" | `string`: `{"k":1}`, `x` |
| `{"a":9223372036854775808}` | stops: "int too big to convert" | `decimal(20,0)` |
| `{"":1,"e":{}}` | columns `""` and `e` (`struct<>`) | both dropped |

The first is silent: the run succeeds with a different schema, so the run record's
schema hash and rows-v1 digest differ from Spark's for the same file.

## Repro
```python
from ubunye.backends.pandas_backend import PandasBackend
open("c.json", "w").write('{"a":1,"o":{"b":1}}\n{"a":"x","o":{"a":2}}\n')
PandasBackend().read_frame("json", "c.json")
```

## Expected
Spark's `JsonInferSchema` (3.5 and 4): `inferField` (a whole number in 64 bits is
`bigint`, a longer one `decimal(digits,0)`, past 38 digits `double`; an empty string
counts as null), `compatibleType` (bigint and double give double, decimals widen,
structs merge by field, anything else is string), `canonicalizeType` (empty names
and empty structs dropped, SPARK-8093). `JacksonParser` keeps the JSON text of a
value read into a string column (`copyCurrentStructure`).

## Fix
`ubunye/adapters/spark_json.py`, a port with Spark's names, used by the pandas JSON
reader. A line holding an array of objects gives one row per object, as Spark reads
it. Field names sort by UTF-16 code unit, as Java's `compareTo`. Arrow takes each
column as it is when it can; a column that needs Spark's conversion is converted
value by value. Speed: 200,000 records of 10 fields read in 3.37 s (3.30 s before).

## Not ported
A record that is not an object (`5`, `"x"`): Spark keeps it as `_corrupt_record`; the
pandas backend now says so and stops. The options `prefersDecimal`, `inferTimestamp`,
`dropFieldIfAllNull` and `primitivesAsString` are still refused by the pandas backend.

## Evidence
Unit: `tests/unit/backends/test_pandas_awkward_data.py::TestJsonInference` (9 failed
before, 11 pass after). Integration (CI): `test_json_inference` (11 cases, among them
depth 30 nesting and per-row keys), `test_json_multiline_document`,
`test_json_empty_string_in_a_number_field` (Spark's partial-result rule; the one case
this port assumes rather than reads from source).

## After CI on live Spark (E-09, 2026-09-30)
Nine of the eleven JSON cases matched on Spark 4.2 and all on Spark 3.5. The two that
did not on Spark 4.2 (`number-and-text`, `object-and-text`) are F-084: Spark 4 keeps
the source text of a value read into a text column. `test_json_empty_string_in_a_number_field`
(the assumed partial-result rule) passed on both.
