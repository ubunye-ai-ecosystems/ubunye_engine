# Tutorial 1: prove one workload gives the same result on pandas and Spark

You will run one task twice, on the pandas backend and on local Spark, and have Ubunye
compare the two run records. It is for anyone who wants to see, on their own machine,
that "runs the same everywhere" is checked and not just claimed. About ten minutes; no
cloud, no account.

## You need

- Python and, for the Spark half, Java, in the versions listed in
  [Installation](../getting_started/install.md). The pandas half needs no Java.
- The Ubunye repository (the workload lives in it):

```bash
git clone https://github.com/ubunye-ai-ecosystems/ubunye_engine
cd ubunye_engine/examples/proving/c01_portable_etl
python -m venv .venv
. .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install "ubunye-engine[pandas,spark]"
```

## 1. Run it on pandas

```bash
ubunye run -d pipelines -u proving -p c01 -t etl --backend pandas --lineage --var out_dir=output/pandas
ubunye prove observe --workload c01-portable-etl --env pandas-local \
  -d pipelines -u proving -p c01 -t etl -o evidence
```

`--lineage` keeps a run record: for each input and output, every row's hash, the
schema and the row count. `prove observe` files that record as evidence of this
workload in this environment.

The run prints a `UserWarning` from Narwhals, `Found complex group-by expression, which
can't be expressed efficiently with the pandas API`. That is expected: it is about speed
on pandas, not about the result. The run still ends with `[OK] Run complete for etl`.

## 2. Run the same task on Spark

```bash
ubunye run -d pipelines -u proving -p c01 -t etl --backend spark --lineage --var out_dir=output/spark
ubunye prove observe --workload c01-portable-etl --env spark-local \
  -d pipelines -u proving -p c01 -t etl -o evidence
```

Nothing in the task changed: not the config, not `transformations.py`.

## 3. Compare

```bash
ubunye prove report evidence --workload c01-portable-etl --reference spark-local
```

Expected:

```
| Environment | Verdict | Execute | Identity | Inputs | Data | Schema | Rows | Digest | ...
| spark-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | bb08a7d7a9fd | ...
| pandas-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | bb08a7d7a9fd | ...
```

Each row is one run. `spark-local` is the **reference**: the run every other run is
compared with. The columns, in plain words:

| Column | PASS means |
|---|---|
| Verdict | every check below passed |
| Execute | the run finished without an error |
| Identity | it is the same job: the same code, the same output names, and the same time zone for cutting time into days |
| Inputs | it read the same rows |
| Data | it wrote the same rows |
| Schema | the outputs have the same columns and types |
| Rows | the outputs have the same number of rows |
| Digest | a short fingerprint of all the output rows; equal digests mean equal data |
| Time, Cost, Engine | how long the task took, where the cost figure came from, and the Ubunye version and backend |

The command exits with 0 when no run disagrees, and with 1 when one does.

**Data PASS** means both engines wrote the same rows: every value, every null, every
type, compared by a hash of every row that ignores row and column order and reads
timestamps as UTC instants. It does not mean the Parquet files are byte for byte the
same; they are not, and Ubunye does not claim they are.

The digest `bb08a7d7a9fd` was first measured on Windows (Spark 4.2, pandas 3) and is
pinned in the repository's integration test, which CI runs on Linux with Spark 3.5 and
Spark 4: yours should match. If it does not, that is worth a report.

## When it disagrees

Try it: make the Spark run cut time into days in another time zone. Open
`pipelines/proving/c01/etl/config.yaml` and add this block at the end of the file, at
the top level (not indented, next to `MODEL`, `VERSION` and `CONFIG`):

```yaml
ENGINE:
  spark_conf:
    spark.sql.session.timeZone: Africa/Johannesburg
```

Then rerun only the Spark half (both commands of step 2), then the report. Expected:

```
| spark-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 09f629ed5b5f | ...
| pandas-local | **FAIL** | PASS | FAIL | PASS | FAIL | PASS | PASS | bb08a7d7a9fd | ...

Why:

- **pandas-local**: session time zone UTC, reference Africa/Johannesburg: day and hour values can differ
```

The `pandas-local` row now says `FAIL`, with `Identity` and `Data` FAIL, and `Why:`
gives the reason: the two runs cut time into days in different time zones. The report
exits with 1, so a script or a CI job stops here.

This is the first problem the proving ground found: a Spark session on a laptop outside
UTC used to cut days in the laptop's own time zone, without saying so. Ubunye now runs
every session in UTC unless you set a time zone, as you just did. Why it works this way:
[ADR 007](../architecture/adr-007-one-time-zone.md).

## Clean up

Remove the `ENGINE:` block you added to `pipelines/proving/c01/etl/config.yaml`, so the
task is back as it was (or run `git checkout pipelines/proving/c01/etl/config.yaml`).
Then delete what the runs wrote:

```bash
rm -rf output evidence pipelines/.ubunye
```

## Common problems

- `JAVA_HOME is not set`: install Java (versions in
  [Installation](../getting_started/install.md)) and point `JAVA_HOME` at it, or do only
  the pandas half.
- Windows: Spark needs `HADOOP_HOME` with `winutils.exe` to write files.
- `pyarrow` older than 24 on Windows cannot find a time zone database; upgrade it.
