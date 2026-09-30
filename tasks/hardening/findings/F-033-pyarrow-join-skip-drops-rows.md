# F-033: pyarrow's binary_join_element_wise with null_handling="skip" drops rows

**Status:** worked around in Ubunye (F-014 fix); not reported upstream (owner's call)
**Severity:** major if used (rows vanish silently), none today
**Source:** building the vectorised rows-v1 hash (F-014), 2026-09-29
**Promise:** 5 (nothing is lost or doubled silently)

## What happens
On pyarrow 25.0.1 (Windows, Python 3.13), `pyarrow.compute.binary_join_element_wise`
with `null_handling="skip"` returns an array shorter than its inputs: every row in
which all inputs are null is dropped, not joined to an empty string. No error.

## Repro
```python
import pyarrow as pa, pyarrow.compute as pc
a = pa.array(["x", None, None, "y"])
b = pa.array(["p", None, None, "q"])
j = pc.binary_join_element_wise(a, b, ",", null_handling="skip")
print(len(j), j.to_pylist())   # 2 ['x,p', 'y,q']   expected 4, with '' twice
```

## How Ubunye avoids it
The rows-v1 line builder (`ubunye/lineage/content_hash.py`, `_slice_lines`) joins
with `null_handling="replace", null_replacement=""` and gives every member a leading
comma, removed after the opening brace. It also checks that the joined array has one
line per row and no nulls, and hashes the slice the Python way if not. Property tests
cover rows where every column is null.

## Not done
No upstream issue filed (anything public under the owner's name waits on him).
Not checked on other pyarrow versions.
