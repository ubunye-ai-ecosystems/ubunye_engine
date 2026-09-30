# F-051: the REST writer crashes on NaN, timestamps and decimals, mid-write

**Status:** fixed on fix/f015-rest-on-pandas
**Severity:** major (a common frame cannot be sent, and the failure leaves a partial post)
**Source:** skeptic review of F-015 (2026-09-30)
**Promise:** 5 (nothing is lost or half done silently)

## What happens
The writer handed each batch to `requests` as `json=`. requests encodes with
`allow_nan=False`, so a NaN or an infinity raises `ValueError: Out of range float
values are not JSON compliant`; a `datetime`, `date`, `Decimal` or `bytes` value
raises `TypeError: Object of type ... is not JSON serializable`. Neither is an
`HTTPError`, so the writer's per-batch handling did not catch it: the write stopped
at the first such batch, after the earlier batches had been posted. On Spark, where
a double column with NaN or any timestamp column is ordinary, this was true before
F-015 too.

## Repro
A frame with a NaN double, a timestamp or a decimal column, written with
`format: rest_api`, on either backend.

## Fix
`ubunye/plugins/rest_http.py`: `jsonable` gives every value one JSON form, the same
from every backend: NaN and the infinities `null`; an instant ISO 8601 in UTC with
`Z`; wall clock time (Spark's `timestamp_ntz`) ISO 8601 without an offset; a date
`yyyy-mm-dd`; a decimal its exact text as a string (chosen over a JSON number, which
a receiver parses as a float and can lose digits); binary base64. `json_body` encodes
a whole batch before anything is sent; a value with no JSON form stops the write
with `SinkWriteError` naming the row and field, and none of that batch is posted.
PySpark hands a `timestamp` back as naive local time, so `frame_io.iter_records`
makes it aware (UTC) first, by the frame's schema, nested fields too; the pandas
backend already gives aware UTC timestamps. Documented in
`docs/connectors/rest_api.md`, Write.

## Evidence
- `tests/unit/connectors/test_rest_api_writer_values.py`: 5 tests (every kind of value
  from pandas; a Spark frame's naive local timestamps made UTC, nested in an array
  too; a batch with an unsendable row posts nothing while the earlier batch stands;
  NaN never reaches the encoder; wall clock time without an offset). All 5 fail
  before the fix and pass after.
- `tests/integration/test_rest_api_parity.py::test_nan_timestamps_decimals_are_posted_the_same`
  (CI, Spark 4 and 3.5): the same NaN, timestamp, date, decimal and binary row posts
  identical payloads from Spark and pandas.
