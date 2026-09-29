# F-042: On pandas, a transform's merge runs about 2.8 times slower on the Arrow backed frames Ubunye hands it

**Status:** fixed on fix/f010-f042-docs (docs; the type guarantee is unchanged)
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

## Decision
The lead's call (2026-09-30): keep the type guarantee. The pandas backend goes on
handing the transform Arrow backed frames, so a whole number column with a null stays
a whole number column, as on Spark. The cost is documented, and the way out is the
user's, in their transform.

## Fix
`docs/deployment/anywhere.md`, new section "Big merges on Arrow columns" (linked from
`docs/backends.md`): what is slower (merge only), by how much, that memory goes the
other way, a three line `numpy_keys(frame, keys)` helper that turns whole number join
keys with no nulls into NumPy `int64` and leaves the rest alone, and why the engine
does not do it itself.

Checked again on the dev box, 5,000,000 rows (region 0 to 999) merged with 900
regions, median of 3, pyarrow 25.0.1 (`merge_bench.py`, scratch, not committed):

| merge on `region` | pandas 3.0.6 | pandas 2.3.3 |
|---|---|---|
| NumPy frames | 0.290 s | 0.971 s |
| Arrow frames (as the transform gets them) | 0.969 s | 0.930 s |
| Arrow frames, `numpy_keys` on both sides first | 0.332 s | 0.996 s |

So the gap is a pandas 3 one (3.3 times on the merge alone, 2.8 times in the E-06
transform above), and the helper removes nearly all of it; converting costs about
0.04 s. On pandas 2.3 all three are about the same, and the helper does no harm.

The snippet keeps the data: `fingerprint()` (the `rows-v1` data hash in the run record)
of a frame with an `int64[pyarrow]` key and of the same frame after `numpy_keys` is the
same digest (`sha256:865423ed...`), since both are Arrow `int64`. A merge with only
one side converted gives the right rows (checked on pandas 2.3.3 and 3.0.6).

`tests/unit/test_docs_numpy_keys.py` reads the snippet between
`<!-- numpy-keys:begin -->` and `<!-- numpy-keys:end -->` from the page and runs it:
it is three lines; a key with no nulls becomes `int64` with the same values and the
input frame is not changed; a key with a null stays `int64[pyarrow]`; pandas'
nullable `Int64` works the same (no nulls to `int64`, with a null left as `Int64`); a
merge gives the same rows either way. 4 passed on pandas 2.3.3 and on pandas 3.0.6.
Before the docs change the page has no such block, so the test cannot load it.

Left open: ADR 004 itself is about native frames versus the port; the rule that
pandas columns are Arrow backed so whole numbers with nulls stay whole numbers is
stated in the reader docs and the 0.7.0 changelog, not in an ADR. If the guarantee is
meant to be a decision, it could be written into ADR 004 as an addendum.
