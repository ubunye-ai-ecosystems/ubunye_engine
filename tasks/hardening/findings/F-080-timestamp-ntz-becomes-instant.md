# F-080: a timestamp without a time zone becomes an instant on the pandas round trip

**Status:** open (design question)
**Severity:** major (silent: type and values shift by the session offset)
**Source:** experiment E-09 (awkward data), shape 5 (time zones)
**Promise:** 1 (same result anywhere)

## What happens
A parquet column written without a time zone (`TIMESTAMP(isAdjustedToUTC=false)`: what
pandas, pyarrow and Spark's `timestamp_ntz` write) is read by the pandas backend as a
naive timestamp. Written back, every naive timestamp is taken as wall clock time in
the session zone and stored as an instant (`timestamp` with UTC). The E-08 task
`tz_parquet` (session zone New York) turned `2024-03-10 02:30` (no zone) into
`2024-03-10 07:30Z` and changed the column type.

## Expected
Spark 3.4 and later read such a column as `timestamp_ntz` (with
`spark.sql.parquet.inferTimestampNTZ.enabled`, true by default) and write it back as
`timestamp_ntz`, unchanged.

## The question
The pandas backend cannot tell a naive column that came from a `timestamp_ntz` input
from a naive column a pandas transform made (`pd.to_datetime("2024-01-02")`), which
by the engine's rule (ADR 007) means session time. Options: keep the rule; or carry
the "no zone" type from the input when a column passes through unchanged (Arrow keeps
the type, so a passthrough column could be written as `timestamp_ntz`), and apply the
session zone only to naive columns the transform made. The second follows Spark for
passthrough data but makes the rule depend on history. Not changed here.

## Evidence
`tests/integration/test_awkward_data_parity.py::test_timestamp_ntz_round_trip`,
`xfail(strict=False)`.
