# Deploying anywhere with spark-submit

If your platform can run spark-submit, it can run these pipelines. This is the
lowest common denominator that EMR, Dataproc, YARN clusters and self-managed
Spark all share, and the engine supports it directly.

## The entry point

Clouds and clusters do not give you a shell, so they cannot call the `ubunye`
command. They take a Python file. The engine ships one:

```bash
spark-submit \
  --py-files deps.zip \
  -m ubunye \
  --task-dir /path/or/bucket/to/pipelines/sales/etl/daily \
  --mode PROD --dt 2026-07-14
```

It deliberately does not create a Spark session. spark-submit already made
one, with the platform's master, executors and settings, and the engine
attaches to it. Creating a second would quietly ignore the cluster and run
everything on one machine.

## No Spark at all: the pandas backend

For small data and local development, a task can run with no Spark and no JVM:

```bash
ubunye run -d ./pipelines -u sales -p etl -t daily --backend pandas
```

The same `config.yaml` drives it. Reads and writes go through the generic `s3`
path connector for the formats pandas understands (csv, parquet, json);
lakehouse formats (delta) and managed tables need Spark, and the engine says so
rather than failing obscurely. The one caveat is your own logic: a
`transformations.py` that calls the Spark DataFrame API needs Spark, so a task
is pandas-runnable when its transform is backend-agnostic (or pandas-native).
Install the extra with `pip install 'ubunye-engine[pandas]'` (on Windows it asks
for pyarrow 24 or newer, the first that handles timezones there). Check a task can run
on it first with `ubunye validate ... --backend pandas`, and see every backend
with `ubunye backends` ([Execution backends](../backends.md)).

### It reads data the way Spark does

The same folder must give the same data on both backends, so the pandas backend
copies Spark's defaults instead of pandas' own:

| What you read | What you get (same as Spark) |
|---|---|
| CSV, no options | No header: the first line is data, columns are `_c0`, `_c1`, ... |
| CSV with `header: "true"` | Columns named from the first line, every value as text |
| CSV with `inferSchema: "true"` | `int` if every value fits, else `bigint`; `double`, `boolean`, `date`, `timestamp`; an empty column is text |
| An empty CSV field | null, not an empty string |
| Quotes in a CSV value | Spark's escape is a backslash, so `"Anna ""Annie"""` stays exactly as written, and `"a\"b"` is `a"b` (set `escape: '"'` for doubled quotes); a line of only spaces is skipped |
| JSON | One object per line (`multiLine: "true"` for one big array); columns sorted by name; whole numbers are `bigint`; dates stay text |
| A folder | Every data file in it, skipping `_SUCCESS` and other `_` or `.` files and empty files, so it reads what Spark wrote |
| A partitioned folder (`dt=2024-01-02/...`) | The partition columns, after the data columns, typed as Spark infers them (see below) |
| A glob such as `data/*.csv` | Every matching file |
| `schema: "id INT, name STRING"` | Exactly those columns and types |
| `mode: "FAILFAST"`, `"DROPMALFORMED"`, `"PERMISSIVE"` | What Spark does with a bad row: stop, skip it, or (the default) cut a row with too many fields and pad one with too few |

The frame your task gets is an ordinary pandas DataFrame whose columns are backed
by Arrow, so a whole number column with gaps stays whole numbers and a decimal
stays a decimal, just as in Spark. Write plain pandas and return plain pandas:

```python
class Enrich(Task):
    def transform(self, sources):
        orders = sources["orders"]
        return {"report": orders.assign(total=orders["price"] * orders["qty"])}
```

Timestamps written as text are read in `spark.sql.session.timeZone` from your
`ENGINE.spark_conf`, the same setting Spark uses. If it is not set, the pandas
backend uses UTC on every machine, while Spark would use the machine's own zone.
Set it once and the two backends agree.

### It writes data the way Spark does

A path the pandas backend writes looks exactly like one Spark writes: a folder
holding `part-00000-....snappy.parquet` (or `.csv`, `.json`) and a `_SUCCESS`
file. So Spark can read what pandas wrote, and pandas can read what Spark wrote.

- `overwrite` replaces the folder. The new data is written to a hidden folder
  first and swapped in only when it is complete, so a failed run never leaves
  you with half a table.
- `append` adds new part files and leaves the old ones alone.
- `overwrite_partitions` with `partitionBy` replaces only the partitions the
  new data fills (see below).
- CSV is written the Spark way: no header unless `header: "true"`, text quoted
  only when it has to be, numbers such as `2.0` and `1.0E10`, timestamps such as
  `2024-01-02T03:04:05.000+02:00`.
- JSON is one object per line, and null fields are left out.
- Parquet timestamps are stored in microseconds, which is what Spark reads.

### Partition folders

`partitionBy: [dt]` writes Spark's folders, `out/dt=2024-01-02/part-....parquet`,
with every mode, and the same task gives the same folders and rows on both
backends (the engine's tests check this against Spark 4.2). What Spark does,
this does:

- One folder level per column, in `partitionBy` order. Characters a folder name
  cannot hold become `%` codes (`a/b` is `a%2Fb`; on Windows a space is `%20`, as
  Spark writes it there). A null or empty value is `__HIVE_DEFAULT_PARTITION__`.
- The partition columns are not in the data files; reading the folder puts them
  back after the other columns.
- `overwrite_partitions` replaces each partition folder the new data fills and
  leaves every other partition alone. Each new partition is written to a hidden
  folder first, then swapped in; if a swap fails, the partitions already
  swapped are put back. Rerunning a day this way replaces that day, so it is
  safe after a crash with no other help.
- `append` claims every new file before it lands, so a failed or killed run has
  exactly its own files taken back ([ADR 008](../architecture/adr-008-rerun-safety.md)).

Reading a partitioned folder, a value becomes an `int`, `bigint`, `decimal`,
`double`, `timestamp`, `date` or text, as Spark infers it. That inference is why
some column types are refused as partition columns: Spark writes them, but reads
them back as something else (a `double` column comes back as text or a decimal,
a `decimal` as a double, `binary` as text). The pandas backend refuses
`double`, `decimal`, binary, time and nested partition columns with a message
saying so; cast the column to a string or a date first. Partition by strings,
whole numbers, dates, booleans or timestamps. As on Spark, a boolean comes back
as the text `true` or `false`, a small integer as `int`, and a timestamp with 2
to 6 decimal places of seconds makes the column text.

Three things differ from Spark, all rare:

- A partition column whose every value is null reads back as all null text;
  Spark calls its type `void`, which pandas has no match for.
- A folder value like `10:05:06` reads back as text; Spark 4.2 infers a `time`.
- Folder names that differ only in case (`p=1` and `P=2`) are one column on both;
  Spark takes the spelling from whichever folder it lists first, which is not a
  fixed order, so the name's case may differ.

`ubunye plan --backend pandas` and `ubunye validate --backend pandas` check these
options before a run, so an option the pandas backend cannot honour is found
then, not when the file is opened.

Anything the pandas backend cannot do the Spark way is refused by name, not
ignored: an unknown reader option (`dateFormat`, for example), a nested schema
type, or a cloud path such as `s3a://`. The error says which option or path and
what to do instead.

## Scheduling

Any scheduler that can run a command can own the timetable. For Airflow, the
engine generates the DAG for you:

```bash
ubunye export airflow -d ./pipelines -u sales -p etl -t daily -o dags/sales_daily.py
```

The generated file is one task calling `ubunye run` with the right flags.
Review it, commit it to your Airflow repository, done.

## The checklist for a new platform

1. Can it run spark-submit or an equivalent? Then compute is solved.
2. Where does data live? Set `UBUNYE_DATA_ROOT` to a path or bucket the
   platform can reach.
3. Where do models live? Give the registry a mounted path or an `s3://` or
   `gs://` location.
4. Do not set `spark.master` in any config. The platform owns it.

That is the whole integration. If you build one for a platform we have not
listed, the connectors, stores and backends are all pluggable entry points,
and a pull request with your runner script is very welcome.
