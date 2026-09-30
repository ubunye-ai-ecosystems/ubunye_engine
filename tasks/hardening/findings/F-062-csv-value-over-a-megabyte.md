# F-062: a CSV row over 1 MB stops the pandas read

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** major (a file Spark reads cannot be read on pandas)
**Source:** experiment E-08 (awkward data), shape 2 (long text)
**Promise:** 1 (same result anywhere)

## What happens
A CSV with one value of about 1 MB (a document body, a JSON blob, a base64 image):

```
SourceReadError: The pandas backend could not read csv at .../long.csv:
straddling object straddles two block boundaries (try to increase block size?)
```

With multiLine on or off, quoted or not. A file that also has a ragged row (or a
header) with a value over 131,072 characters then stopped in Python's csv module:
`field larger than field limit (131072)`.

## Repro
```python
from ubunye.backends.pandas_backend import PandasBackend
open("l.csv", "w").write("id,t\n1," + "x" * 3_000_000 + "\n")
PandasBackend().read_frame("csv", "l.csv", options={"header": "true"})
```

## Expected
Spark has no limit on a value's length: `maxCharsPerColumn` is -1 by default
(Spark 3.x and 4, `CSVOptions`), so univocity reads any length.

## Fix
pyarrow parses in 1 MB blocks; a file whose row does not fit is parsed again as
one block. Python's csv module (used for the header and for ragged rows) runs with
its field limit raised for the read, then put back.

## Evidence
Unit: `tests/unit/backends/test_pandas_awkward_data.py::TestCsvLongRows` (6 failed
before, 6 pass after). Integration (CI): `test_csv_values_longer_than_a_megabyte`
(3 MB and 1.2 MB values with LF, CRLF, NUL, tab and non-ASCII, multiLine on and off).
