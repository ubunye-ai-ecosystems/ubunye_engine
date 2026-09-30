# F-060: a CSV header with a duplicate or blank name stops the pandas read

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** major (a file Spark reads cannot be read on pandas)
**Source:** experiment E-08 (awkward data), shape 4 (messy CSV) and 10 (names)
**Promise:** 1 (same result anywhere)

## What happens
A header such as `id,when,amount,amount,,spaced` (a repeated name and a blank one,
common in spreadsheet exports) stops the pandas read:

```
SourceReadError: The pandas backend could not read csv at .../mixed.csv:
Can't unify schema with duplicate field names.
```

A header that differs only by case (`Col,col`) is read with both names, and a blank
name becomes a column called `""`.

## Repro
```python
from ubunye.backends.pandas_backend import PandasBackend
open("h.csv", "wb").write(b"a,a,,A,b,Col,col\n1,2,3,4,5,6,7\n")
PandasBackend().read_frame("csv", "h.csv", options={"header": "true"})
```

## Expected
Spark's `CSVUtils.makeSafeHeader` (Spark 3.5 and 4): a null or empty name, or one
equal to `nullValue`, becomes `_c<index>`; a name that appears more than once
(ignoring case, as `spark.sql.caseSensitive` is false by default) gets its index
appended to every copy. So the header above reads as
`a0, a1, _c2, A3, b, Col5, col6`.

## Evidence
Unit: `tests/unit/backends/test_pandas_awkward_data.py::TestCsvHeaderNames`
(3 failed before, 3 pass after). Integration (Spark 4 and 3.5 in CI):
`tests/integration/test_awkward_data_parity.py::test_csv_header_names`.
