# Tutorial 1: prove one workload gives the same result on pandas and Spark

You will run one task twice, on the pandas backend and on local Spark, and have Ubunye
compare the two run records. About ten minutes; no cloud, no account.

## You need

- Python 3.10 to 3.13.
- For the Spark half: Java 17 or 21 (`java -version`). The pandas half needs no Java.
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

**Data PASS** means both engines wrote the same rows: every value, every null, every
type, compared by a hash of every row that ignores row and column order and reads
timestamps as UTC instants. It does not mean the Parquet files are byte for byte the
same; they are not, and Ubunye does not claim they are.

The digest `bb08a7d7a9fd` was first measured on Windows (Spark 4.2, pandas 3) and is
pinned in the repository's integration test, which CI runs on Linux with Spark 3.5 and
Spark 4: yours should match. If it does not, that is worth a report.

## When it disagrees

Try it: make the Spark run cut time in another zone, by adding to the task's config

```yaml
ENGINE:
  spark_conf:
    spark.sql.session.timeZone: Africa/Johannesburg
```

and rerunning only the Spark half. The report now says `identity FAIL` and `data FAIL`,
with the reason: the two runs cut time into days in different zones. This is the first
thing the proving ground found (finding F-021): before ADR 007, a Spark session on a
laptop outside UTC did this silently.

## Clean up

```bash
rm -rf output evidence pipelines/.ubunye
```

## Common problems

- `JAVA_HOME is not set`: install Java 17 or 21 and point `JAVA_HOME` at it, or do only
  the pandas half.
- Windows: Spark needs `HADOOP_HOME` with `winutils.exe` to write files.
- `pyarrow` older than 24 on Windows cannot find a time zone database; upgrade it.
