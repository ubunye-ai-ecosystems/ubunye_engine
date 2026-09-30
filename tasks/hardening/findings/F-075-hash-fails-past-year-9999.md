# F-075: the pandas run record has no digest for an instant past year 9999

**Status:** open (needs Spark's text for such a year, measured in CI)
**Severity:** minor (no silent error: the record says why; the digest is missing)
**Source:** experiment E-08 (awkward data), shape 5 (time zones)
**Promise:** 1 (same result anywhere: the Spark record has a digest, the pandas one not)

## What happens
The E-08 time zone tasks (session zone `America/New_York`) read
`9999-12-31 23:59:59` as wall clock time, which is `10000-01-01 04:59:59` UTC.
The run succeeds and writes every row, but every step's rows-v1 hash is missing:

```
"hash_error": "OverflowError: date value out of range"
```

The hash writes a timestamp through Python's `datetime`, which stops at year 9999
(and at year 1: `0001-01-01 00:00` in a zone east of UTC is year 0). pyarrow, the
parquet file and Spark hold such instants.

## Expected
A digest, equal to Spark's for the same rows. Spark formats the instant with its
`timestampFormat` (`yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'` in the hash's `to_json`
options); what Java's formatter writes for year 10000 (`+10000-01-01...` or
`10000-...`) decides the text the pandas side must write. Not known without a live
run, so not fixed here.

## Evidence
E-08 harness: `tz_csv_ny`, `tz_csv_schema_ny`, `tz_parquet` all `success` with no
digests. `tests/integration/test_awkward_data_parity.py::test_digest_of_instants_outside_python_years`,
`xfail(strict=False)`: CI shows whether Spark's digest exists and what the pandas
side must match.
