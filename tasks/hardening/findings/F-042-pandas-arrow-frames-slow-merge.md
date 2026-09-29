# F-042: On pandas, a transform's merge runs about 2.8 times slower on the Arrow backed frames Ubunye hands it

**Status:** open
**Severity:** minor
**Source:** scale-runner, experiment E-06 (2026-09-29)
**Promise:** 7 (the core stays small)

## What happens
Without `--lineage`, Ubunye on pandas costs 1.36x the plain job at 1M rows, 1.25x
at 5M, 1.15x at 20M and 1.04x at 50M (E-06, GitHub `ubuntu-latest`). The fixed part
is start up (about 0.25 s). The part that grows with rows (0.90 s at 20M, 0.64 s at
50M, where plain's own spread is 2.8 s) is the transform itself: the pandas backend reads every input as an Arrow
backed frame (`table.to_pandas(types_mapper=pd.ArrowDtype)`, ADR 004), and pandas
joins on `int64[pyarrow]` keys much more slowly than on `int64`.

Each step of the E-06 transform on the same 5,000,000 rows, dev box, pandas 3.0.6,
pyarrow 25.0.1, median of 3:

| step | plain `pd.read_parquet` frames | Ubunye's Arrow backed frames |
|---|---|---|
| filter `qty > 0` | 0.201 s | 0.201 s |
| derive two columns | 0.047 s | 0.022 s |
| `merge` on `region` | 0.353 s | **0.984 s** |
| `groupby` + `agg` | 0.415 s | 0.424 s |

Read and write are the same speed both ways (a one off timing the same way: 0.17 against 0.18 s read, 1.17 against
1.15 s write of `detail`). Memory goes the other way: Arrow backed frames use less
(peak 9.1 GB against 12.2 GB at 50M rows), so this is a trade, not a plain loss.

## Repro
`python tasks/hardening/experiments/e06/e06_pandas_ops.py DATA_DIR`: the four steps
timed on frames from `pd.read_parquet` and from `ubunye.adapters.pandas_io.read_frame`.

## Expected
A user whose transform is merge heavy knows it, or does not pay for it. Options:
say so in the pandas backend docs (and that `df.astype` to NumPy types before a big
merge is the user's choice); or hand the transform NumPy backed integer columns where
no nulls make the Arrow type necessary. The second touches ADR 004's type guarantee
(a nullable int stays an int), so it is a design call, not a quick fix.
