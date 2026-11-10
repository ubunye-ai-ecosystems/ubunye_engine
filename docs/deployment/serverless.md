# One command to the cloud: Glue, Dataproc, Kubernetes, Container Apps, EMR

This page shows how to run a task, unchanged, on AWS Glue, GCP Dataproc
Serverless, Kubernetes, Azure Container Apps or AWS EMR Serverless with one
`ubunye deploy` command. It is for anyone who has a task working locally and
an account on one of those clouds.

**The problem:** the task runs on your laptop; you want it to run on a cloud
and to know it gave the same data.
**When to use it:** any time you deploy to one of these five targets. For
Databricks, see [Databricks](databricks.md).
**The command** (Glue as the example):

```bash
ubunye deploy glue -d pipelines -u shop -p orders -t clean \
  --bucket my-bucket --role arn:aws:iam::123456789012:role/glue-job --record-out glue.json
```

"Serverless" here means the cloud starts and stops the machines for you; you
manage no cluster. Which targets are proven is on the
[deployment map](../deployment.md#where-you-can-deploy-today).

## How it works

Each command uses the cloud's own CLI (`aws`, `gcloud`, `kubectl`, `az`) and
its usual sign-in. It:

1. uploads the task folder as a zip, plus a small entry script (Glue and
   Dataproc; Kubernetes, Container Apps and EMR carry the task inside the image);
2. runs the task once through `ubunye.run_task`, writing a run record;
3. with `--wait` (the default), follows the run to the end and reads the run
   record back from the job's log.

`--dry-run` prints exactly what would be uploaded and run, and runs nothing.

Shared flags: `--env KEY=VALUE` sets the task's environment (repeatable),
`--var KEY=VALUE` a template variable, `-m` the profile and `-dt` the data
timestamp. All flags: [CLI reference](../cli.md#deploy-glue).

None of these pass `spark.master`; see
[the `spark.master` rule](../deployment.md#the-sparkmaster-rule).

## Before you start

| Target | Sign in with | You need |
|---|---|---|
| AWS Glue | `aws configure` or `aws sso login` | an S3 bucket; an IAM role the job runs as, with access to the bucket and the AWS Glue service role policy (`AWSGlueServiceRole`) |
| GCP Dataproc | `gcloud auth login` | a project with the Dataproc API on; a GCS bucket; an Artifact Registry repository for the image; Docker to build it; optionally a service account for the batch |
| Kubernetes | `kubectl` pointed at the cluster (its current context) | a registry the cluster can pull from; Docker to build the image |
| Azure Container Apps | `az login` | a resource group, a Container Apps environment, a registry, and a managed identity that may pull from it. Details: [Azure](azure.md) |
| AWS EMR Serverless | `aws configure` or `aws sso login` | an EMR Serverless application using your image, a job role, and an S3 bucket for logs. A free plan account cannot use EMR |

The examples below use the folder `ubunye init -d pipelines -u shop -p orders -t clean`
makes.

## AWS Glue

Glue 5 (Spark 3.5, Python 3.11). Glue installs the engine with pip, so you
build no image: `--engine` is the pip requirement (default `ubunye-engine`), or
`--engine-wheel` uploads a local wheel. Glue supplies Delta itself. `--role`
is the IAM role the job runs as. Workers: `--workers` (default 2) and
`--worker-type` (default `G.1X`).

```bash
ubunye deploy glue -d pipelines -u shop -p orders -t clean \
  --bucket my-bucket --role arn:aws:iam::123456789012:role/glue-job \
  --region eu-west-1 --env UBUNYE_DATA_ROOT=s3://my-bucket/data --dry-run
```

Real output, with the two long launcher scripts cut to `...`:

```
{
  "platform": "glue",
  "job": "ubunye-shop-orders-clean",
  "uploads": [
    {
      "to": "s3://my-bucket/ubunye/shop/orders/clean/bundle.zip",
      "bytes": 2596
    },
    {
      "to": "s3://my-bucket/ubunye/shop/orders/clean/ubunye_entry.py",
      "bytes": 5078
    }
  ],
  "commands": [
    ["python", "-c", "...create or update the Glue job...", "ubunye-shop-orders-clean",
     "{\"Role\": \"arn:aws:iam::123456789012:role/glue-job\", \"GlueVersion\": \"5.0\", \"WorkerType\": \"G.1X\", \"NumberOfWorkers\": 2, \"Timeout\": 60, \"MaxRetries\": 0, ...}",
     "--region", "eu-west-1"],
    ["python", "-c", "...start the run, follow it, print its log...", "ubunye-shop-orders-clean",
     "wait", "--region", "eu-west-1"]
  ]
}

[DRY RUN] glue plan for 'ubunye-shop-orders-clean'. Nothing ran.
```

## GCP Dataproc Serverless

A batch in a container image. Dataproc mounts Spark and Java; the image
carries the engine, Delta's jars and the entry script. This target builds its
image with the engine's own `ubunye deploy dockerfile`:

```bash
ubunye deploy dockerfile dataproc --out Dockerfile     # also writes ubunye_entry.py
docker build -t europe-west1-docker.pkg.dev/my-project/ubunye/job:1 .
docker push europe-west1-docker.pkg.dev/my-project/ubunye/job:1
```

The image follows Dataproc's rules (no Spark or Java inside, a `spark` user
1099:1099, `tini` and `procps`) and uses Delta built for Scala 2.13, which
runtime 2.2 runs. The task itself arrives as an archive at run time, so one
image serves every task. Other flags: `--runtime` (default `2.2`),
`--service-account`, `--batch`.

```bash
ubunye deploy dataproc -d pipelines -u shop -p orders -t clean \
  --project my-project --region europe-west1 --bucket my-bucket \
  --image europe-west1-docker.pkg.dev/my-project/ubunye/job:1 --dry-run
```

Real output, the `gcloud` command shown on fewer lines:

```
{
  "platform": "dataproc",
  "job": "ubunye-clean-1794306923",
  "uploads": [
    {
      "to": "gs://my-bucket/ubunye/shop/orders/clean/bundle.zip",
      "bytes": 2596
    },
    {
      "to": "gs://my-bucket/ubunye/shop/orders/clean/ubunye_entry.py",
      "bytes": 5078
    }
  ],
  "commands": [
    ["gcloud", "dataproc", "batches", "submit", "pyspark",
     "gs://my-bucket/ubunye/shop/orders/clean/ubunye_entry.py",
     "--project", "my-project", "--region", "europe-west1",
     "--batch", "ubunye-clean-1794306923",
     "--container-image", "europe-west1-docker.pkg.dev/my-project/ubunye/job:1",
     "--version", "2.2",
     "--archives", "gs://my-bucket/ubunye/shop/orders/clean/bundle.zip#bundle",
     "--deps-bucket", "my-bucket",
     "--properties=^#^spark.jars=file:///opt/jars/delta-spark_2.13-3.2.0.jar,...",
     "--", "--task", "shop/orders/clean", "--mode", "PROD"]
  ]
}

[DRY RUN] dataproc plan for 'ubunye-clean-1794306923'. Nothing ran.
```

The number in the batch name changes on every call, because a Dataproc batch
id can be used only once.

## Kubernetes and Azure Container Apps

For a runtime with no managed Spark, the image carries everything: Java, Spark
in local mode, Delta, the engine and your pipelines. These targets build their
image with the engine's own `ubunye deploy dockerfile container`. (The
companion repo's Dockerfiles are a different path; see
[Kubernetes](kubernetes.md).)

```bash
ubunye deploy dockerfile container --pipelines pipelines --out Dockerfile
docker build -t myregistry/ubunye-jobs:1 . && docker push myregistry/ubunye-jobs:1

# a Kubernetes Job, in kubectl's current context
ubunye deploy k8s -u shop -p orders -t clean --image myregistry/ubunye-jobs:1 \
  --namespace data --env UBUNYE_DATA_ROOT=s3a://my-bucket/data --record-out k8s.json
```

The pipelines are inside the image, so these commands take no `-d`.

The Kubernetes Job has no retries (`backoffLimit: 0`) and is cleaned up a day
after it finishes. Other flags: `--cpu`, `--memory`, `--timeout`.

`--dry-run` for Kubernetes prints the Job manifest inside its plan. Trimmed,
real output:

```
{
  "platform": "kubernetes",
  "job": "ubunye-clean-6923",
  "uploads": [],
  "commands": [
    ["python", "-c", "...kubectl apply, wait for Complete or Failed, print the logs...",
     "{\"apiVersion\": \"batch/v1\", \"kind\": \"Job\", \"metadata\": {\"name\": \"ubunye-clean-6923\", \"namespace\": \"data\", ...}, \"spec\": {\"backoffLimit\": 0, \"ttlSecondsAfterFinished\": 86400, ... \"image\": \"myregistry/ubunye-jobs:1\", \"args\": [\"--task\", \"shop/orders/clean\", \"--mode\", \"PROD\"], ... \"requests\": {\"cpu\": \"1\", \"memory\": \"2Gi\"} ...}}",
     "1800", "wait"]
  ]
}

[DRY RUN] kubernetes plan for 'ubunye-clean-6923'. Nothing ran.
```

Azure Container Apps uses the same image:

```bash
ubunye deploy container-apps -u shop -p orders -t clean --image myacr.azurecr.io/ubunye-jobs:1 \
  -g my-rg --environment my-env --registry-server myacr.azurecr.io \
  --registry-identity /subscriptions/.../userAssignedIdentities/pull --record-out aca.json
```

The Container Apps job is created the first time and updated after. Its
output, and so the run record, reaches Log Analytics a few minutes after the
job ends, and the command waits for it. Before you start, the dry run, and
what success looks like: [Azure](azure.md).

## EMR Serverless

Planned: tested as a plan, not against a live application, because an AWS
account on the free plan cannot use EMR. The image comes from the engine's own
`ubunye deploy dockerfile emr-serverless`:

```bash
ubunye deploy dockerfile emr-serverless --out Dockerfile     # push it, set it on the application
ubunye deploy emr-serverless -u shop -p orders -t clean \
  --application-id 00fabc123 --role arn:aws:iam::123456789012:role/emr-job \
  --bucket my-bucket --region eu-west-1 --dry-run
```

Real output, the Spark settings cut to `...`:

```
{
  "platform": "emr-serverless",
  "job": "ubunye-clean",
  "uploads": [],
  "commands": [
    ["aws", "emr-serverless", "start-job-run",
     "--application-id", "00fabc123",
     "--execution-role-arn", "arn:aws:iam::123456789012:role/emr-job",
     "--name", "ubunye-clean",
     "--job-driver", "{\"sparkSubmit\": {\"entryPoint\": \"local:///app/ubunye_entry.py\", \"entryPointArguments\": [\"--task\", \"shop/orders/clean\", \"--mode\", \"PROD\"], \"sparkSubmitParameters\": \"...\"}}",
     "--configuration-overrides", "{\"monitoringConfiguration\": {\"s3MonitoringConfiguration\": {\"logUri\": \"s3://my-bucket/logs/\"}}}",
     "--region", "eu-west-1"]
  ]
}

[DRY RUN] emr-serverless plan for 'ubunye-clean'. Nothing ran.
```

This command starts the run and returns; it does not wait, so it has no
`--wait` or `--record-out`. The driver's output, with the run record, lands in
`s3://<bucket>/logs/`.

## What success looks like

Without `--dry-run`, the command prints progress lines while it waits (on
Glue: `started <run id>`, then the run state every 20 seconds). At the end it
prints one line per task and one line per output. The shape:

```
[OK] glue run success: shop/orders/clean
  <output name>: <rows> rows, <data hash>
  run record: glue.json
```

If a task fails, the line says `[FAIL]` and the command exits with a non-zero
code. If you asked for `--record-out` and the record is not in the job's log,
it fails too, rather than report success without the record.

## The run record

The entry script prints the run's record between two markers at the end of
the job; `--record-out` saves it. It is the same run record a local run writes,
so `ubunye gate` and `ubunye lineage compare` work across clouds: the same task
on two platforms must write the same data.

```bash
ubunye gate --baseline glue.json --candidate dataproc.json   # the same data?
```

More: [Run records](../guides/run-records.md).

## Several tasks in one launch

Repeat `-t` to run a pipeline's tasks in that order, in one job:

```bash
ubunye deploy glue -d pipelines -u food -p prices -t clean -t monitor \
  --bucket my-bucket --role arn:aws:iam::...:role/glue-job \
  --var out_dir=s3://my-bucket/food --record-out glue.json
```

The tasks share the job, its session and its disk, so a later task can read
what an earlier one wrote, even on a container whose disk goes when it stops.
The first task that fails stops the rest, and the deploy fails if any task
failed or never ran. With several tasks, `--record-out glue.json` writes one
run record per task: `glue.clean.json`, `glue.monitor.json`, also when the job
failed.

Things to know:

- A task name is a folder name: no `/`, `\`, or `,`, and each task once. A bad
  list is refused before anything is uploaded or started.
- The default job name and bundle folder of several tasks is their names
  joined by `-` plus a short hash of the list (`clean-monitor-29df150d`), so two
  different lists never share them. Pass `--job` for a name of your own.
- The tasks share the platform's time limit: Glue's job `Timeout` (60 minutes)
  and the container `--timeout` cover all of them together, not each.
- The entry script inside an image runs several tasks only if the image was
  built from this version: rebuild a Kubernetes, Container Apps, Dataproc or
  EMR image made with an older `ubunye deploy dockerfile`. (Glue uploads the
  script on every deploy.)
