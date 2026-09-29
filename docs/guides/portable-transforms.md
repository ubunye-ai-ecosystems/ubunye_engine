# Writing a transform that gives the same answer on pandas and Spark

A transform written with [Narwhals](https://narwhals-dev.github.io/narwhals/) runs on
pandas and on Spark unchanged (ADR 005). Running is not the same as agreeing. These are
the places where the same line of code gave different numbers, each found by running a
real workload on both engines and comparing the run records with `ubunye prove`. Each
has a fix that holds on both.

## Rounding: round halves yourself

`.round(4)` rounds a half **up** on Spark and to the **even** neighbour on pandas. A
monthly mean of 921.03125 became 921.0313 on Spark and 921.0312 on pandas; 43 of 6,366
monthly food prices differed this way (finding F-029).

```python
def round4(expr):
    return (expr * 10000 + 0.5).floor() / 10000   # half up, on every engine
```

`Expr.floor` needs Narwhals 2.9 or later.

## Means and sums of floats: add integers

A sum of floating-point numbers depends on the order it is added in, and engines add in
different orders (pandas pairwise, Spark partition by partition). The results differ in
their last bits, which is invisible until a value sits on a rounding boundary: after
fixing the rounding, 7 monthly prices still differed by 0.0001. Turn the values into
whole units first; an integer sum is exact in any order:

```python
micro = (nw.col("price") * 1_000_000 + 0.5).floor().cast(nw.Int64)
monthly = prices.with_columns(micro=micro).group_by(...).agg(
    total=nw.col("micro").sum(), n=nw.len(),
).with_columns(mean_price=round4(nw.col("total") / nw.col("n") / 1_000_000))
```

Money in integer cents from the start (as in the proving workload C01) avoids the
question altogether.

## Casts inside a group-by: cast after it

On pandas, Narwhals 2.26 dropped a `.cast(nw.Int64)` written inside a group-by that
also computed a mean: the counts came out as `float64`, and the schema no longer
matched Spark's `bigint` (finding F-027, reported as
[narwhals#4005](https://github.com/narwhals-dev/narwhals/issues/4005)). Cast after the aggregation:

```python
agg = df.group_by("k").agg(n=nw.len(), m=nw.col("x").mean())
agg = agg.with_columns(nw.col("n").cast(nw.Int64))
```

## Replacing text: `replace_all`, literal

`str.replace` is not implemented for Spark in Narwhals (finding F-028); it raises
`NotImplementedError`. `str.replace_all(" KG", "", literal=True)` works on both.

## Casting text to numbers: filter first, then cast

`"25 KG"` to 25 is a cast of text to a number. When some rows cannot be cast, pandas
raises, Spark 3.5 quietly makes them null, and Spark 4 (ANSI mode) raises. Keep only the
rows that can be cast, then cast:

```python
weights = df.filter(nw.col("unit").str.contains("^[0-9.]+ KG$").fill_null(False))
weights = weights.with_columns(
    kg=nw.col("unit").str.replace_all(" KG", "", literal=True).cast(nw.Float64)
)
```

## Time: say which zone

Cutting a timestamp into a day or an hour depends on a time zone. Every Ubunye backend
uses UTC unless the task sets `spark.sql.session.timeZone` (ADR 007; before that, local
Spark used the machine's zone, finding F-021). A task that means local days sets the key.

## Null group keys: keep them on purpose

Spark keeps rows whose group key is null as one group; plain pandas drops them. Narwhals
follows Spark only when asked: `group_by(..., drop_null_keys=False)`.

## How to know

Run the task on both engines with `--lineage` and compare:

```bash
ubunye run ... --backend pandas --lineage && ubunye prove observe ... --env pandas-local -o evidence
ubunye run ... --backend spark  --lineage && ubunye prove observe ... --env spark-local  -o evidence
ubunye prove report evidence --workload my-task --reference spark-local
```

A data FAIL with schema and rows PASS is usually one of the traps above: compare the two
outputs row by row on the columns that are computed.
