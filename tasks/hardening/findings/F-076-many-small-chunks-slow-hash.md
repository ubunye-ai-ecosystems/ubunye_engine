# F-076: the pandas run record hash is slow on a folder of many small files

**Status:** fixed on hardening/awkward-data (2026-09-30)
**Severity:** minor (speed)
**Source:** experiment E-09 (awkward data), shape 6 (many small files)
**Promise:** none (cost of the record)

## What happens
A task reading 5,000 parquet files of 2 rows each ran 9.2 s with `--lineage` on the
dev box; the read took 2.6 s and the two rows-v1 hashes (input and output) 4.8 s,
for 10,000 rows. The frame keeps one Arrow chunk per file, and the hash worked one
slice per chunk, at about a millisecond each whatever its size.

## Repro
```python
import pyarrow as pa, time
from ubunye.lineage.content_hash import fingerprint_arrow
t = pa.concat_tables([pa.table({"id": [i, i + 1]}) for i in range(0, 10000, 2)])
s = time.perf_counter(); fingerprint_arrow(t); print(time.perf_counter() - s)  # 1.9 s
```

## Expected
The same cost as the same rows in one chunk (0.02 s). The digest is order and
chunking independent, so putting the rows together cannot change it.

## Fix
`content_hash._slices` puts a table of many small chunks (fewer than 4,096 rows per
chunk on average) into one chunk before slicing it.

## Evidence
Unit: `tests/unit/lineage/test_content_hash_fast_path.py::TestManySmallChunks` (the
slice count test failed before: 2,000 slices; 1 after; the digest test passes on
both). Timing: 1.875 s before, 0.021 s after, same digest.
