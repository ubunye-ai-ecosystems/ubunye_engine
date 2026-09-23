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
Install the extra with `pip install 'ubunye-engine[pandas]'`.

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
