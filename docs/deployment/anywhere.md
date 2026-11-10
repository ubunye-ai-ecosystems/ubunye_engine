# Deploying anywhere with spark-submit

If your platform can run `spark-submit`, it can run your tasks. This page is for
EMR, Dataproc, YARN clusters and self-managed Spark, where no `ubunye deploy`
command fits.

- **The problem:** a cluster runs a Python file through `spark-submit`. It has
  no shell, so it cannot call the `ubunye` command.
- **When to use it:** your platform takes `spark-submit` jobs, and you launch
  them yourself.
- **The one command:**

```bash
spark-submit --py-files deps.zip run_ubunye.py \
  --task-dir pipelines/sales/etl/daily --mode PROD --dt 2026-07-14
```

## 1. The entry point

The engine ships a small runner, `python -m ubunye`, for exactly this. Its flags:

| Flag | Default | Description |
|---|---|---|
| `--task-dir` | (required) | The task folder, the one holding `config.yaml` |
| `--mode` | `PROD` | The profile |
| `--dt` | | The `{{ dt }}` for the config |
| `--lineage` | off | Write a run record |
| `--log-level` | `INFO` | Log level |

`spark-submit` needs a file, so either point it at the engine's own
`/path/to/site-packages/ubunye/__main__.py`, or keep a two line file of your own
next to your job (this is `run_ubunye.py` above):

```python
import sys
from ubunye.__main__ import main

sys.exit(main())
```

Where you do have a shell, `python -m ubunye --task-dir ... --mode PROD --dt ...`
does the same.

It does not create a Spark session on purpose. `spark-submit` already made one,
with the platform's master, executors and settings, and the engine attaches to
it. Creating a second would quietly ignore the cluster and run everything on one
machine.

The task folder must be on the driver's own disk: baked into the image, on a
mounted shared disk, or copied down before the run. A bucket URL such as
`s3a://...` does not work as `--task-dir`. The `ubunye deploy` commands for
Glue, Dataproc and containers do all of this for you: they ship the task as a
zip with a small `ubunye_entry.py` ([One command to the cloud](serverless.md)).

## 2. Ship the engine: `--py-files deps.zip`

The cluster's Python needs the engine. The simplest way is to install it on the
cluster (a bootstrap step, or a container image):
`pip install ubunye-engine`. Its dependencies are `pyyaml`, `jinja2`,
`pydantic`, `typer` and `narwhals`.

If you cannot install packages there, ship the engine with the job.
`--py-files` puts a zip on the Python path of the driver and every executor:

```bash
pip install --target deps --no-deps ubunye-engine
cd deps && zip -qr ../deps.zip . -x '*.pyc' && cd ..
spark-submit --py-files deps.zip run_ubunye.py --task-dir pipelines/sales/etl/daily --mode PROD
```

Python cannot import compiled code from a zip. `pydantic` needs `pydantic-core`,
which is compiled, so the dependencies must already be on the cluster (install
them at cluster start, or in the image). Add
`deps.zip` only for what is plain Python, such as the engine itself and helper
modules your transforms import.

What success looks like: the run log names the task's Spark app,
`Submitted application: ubunye:sales.etl.daily`, and the job exits 0.

## 3. Leave `spark.master` to the platform

Do not set `spark.master` in any config: the platform owns it, and the engine
refuses a config that tries. Why: [Deployment](../deployment.md#where-you-can-deploy-today).

## Scheduling

Any scheduler that can run a command can own the timetable. For Airflow, the
engine generates the DAG for you:

```bash
ubunye export airflow -c pipelines/sales/etl/daily/config.yaml -o dags/sales_daily.py
```

The generated file is one DAG (Airflow 2.4+ and 3) with one `BashOperator` that
calls `ubunye run ... -m prod -dt {{ ds }}`. `--profile` sets the profile
(default `prod`, lower case: pass `--profile PROD` if your profiles are upper
case), `--usecase-dir` sets the pipelines folder as Airflow sees it, and
`--backend` and `--lineage` are passed on to the run. Review it, commit it to
your Airflow repository, done.

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

No Spark at all? A task can run on the pandas backend, with no Java:
[How the pandas backend reads and writes](../backends.md#how-the-pandas-backend-reads-and-writes).
