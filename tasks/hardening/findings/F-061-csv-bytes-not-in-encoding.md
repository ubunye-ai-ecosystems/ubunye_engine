# F-061: a CSV byte that is not valid in its encoding stops the pandas read

**Status:** fixed on hardening/awkward-data (2026-09-30), CSV only
**Severity:** major (a file Spark reads cannot be read on pandas)
**Source:** experiment E-09 (awkward data), shape 4 (messy CSV)
**Promise:** 1 (same result anywhere)

## What happens
A Windows export (cp1252) read without `encoding`, so as UTF-8:

```
SourceReadError: The pandas backend could not read csv at .../cp1252.csv:
'utf-8' codec can't decode byte 0xe9 in position 14: invalid continuation byte
```

## Repro
```python
from ubunye.backends.pandas_backend import PandasBackend
open("c.csv", "wb").write("name,price\nCafé,€5\n".encode("cp1252"))
PandasBackend().read_frame("csv", "c.csv", options={"header": "true"})
```

## Expected
Spark decodes each line with `new String(bytes, charset)` (and a reader with the
charset for multiLine), Java's default: a byte that is not valid becomes U+FFFD and
the read goes on. So `Caf�` and `�5`. Spark never stops on it.

## Fix
Decode strictly first (no cost for valid files); on failure decode with
replacement, the way Java does, and hand the text to pyarrow as UTF-8.

## Not fixed here
JSON: invalid UTF-8 inside a JSON line makes Jackson fail on that record, so Spark
keeps a row of nulls (PERMISSIVE); the pandas reader stops. Not measured on Spark
yet; left for a JSON parse-mode finding.

## Evidence
Unit: `tests/unit/backends/test_pandas_awkward_data.py::TestCsvBytesNotInTheEncoding`
(2 failed before, 3 pass after). Integration (CI, Spark 4 and 3.5):
`test_awkward_data_parity.py::test_csv_bytes_not_in_the_encoding` and a seeded fuzz of
cut, overlong and surrogate sequences, `test_csv_invalid_utf8_fuzz`. The fuzz is the
check that Python's replacement matches Java's for each broken sequence.

## After CI on live Spark (E-09, 2026-09-30)
Spark 3.5 read all four cases as the pandas backend does. Spark 4.2 refused the
`cp1252` and `latin1` encoding names outright (F-085), and the seeded fuzz of broken
UTF-8 differed on both versions: Java's decoder gives one U+FFFD for a whole encoded
surrogate (`ED A0 80`), Python three (F-086).
