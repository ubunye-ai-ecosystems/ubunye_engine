# One command to the cloud: Glue, Dataproc, Kubernetes, Container Apps, EMR

`ubunye deploy glue` and `ubunye deploy dataproc` run a task, unchanged, on AWS
Glue or GCP Dataproc Serverless, with the cloud's own CLI (`aws`, `gcloud`) and
its usual login. `--dry-run` prints exactly what would be uploaded and run.

Each deploy uploads the task folder as a zip and a small entry script, runs the
task once through `ubunye.run_task` with a run record, and (with `--wait`, the
default) follows it to the end and reads the record back from the job's log:

```bash
ubunye deploy glue -d pipelines -u shop -p orders -t clean \
  --bucket my-bucket --role arn:aws:iam::123456789012:role/glue-job \
  --env UBUNYE_DATA_ROOT=s3://my-bucket/data --record-out glue.json

ubunye deploy dataproc -d pipelines -u shop -p orders -t clean \
  --project my-project --region europe-west1 --bucket my-bucket \
  --image europe-west1-docker.pkg.dev/my-project/ubunye/job:1 \
  --env UBUNYE_DATA_ROOT=gs://my-bucket/data --record-out dataproc.json

ubunye gate --baseline glue.json --candidate dataproc.json   # the same data?
```

`--env KEY=VALUE` sets the task's environment (repeatable), `--var KEY=VALUE` a
template variable, `-m` and `-dt` the mode and data timestamp.

## AWS Glue

Glue 5 (Spark 3.5, Python 3.11). Glue pip-installs the engine
(`--engine`, a pip requirement, default `ubunye-engine`; or `--engine-wheel` to
upload a local wheel) and supplies Delta itself. `--role` is the IAM role the job
runs as: it needs your bucket and the AWS Glue service role policy. Workers:
`--workers` (default 2) and `--worker-type` (default `G.1X`).

## GCP Dataproc Serverless

A batch in a container image. Dataproc mounts Spark and Java; the image carries
the engine, Delta's jars and the entry script. Write its Dockerfile with:

```bash
ubunye deploy dockerfile dataproc --out Dockerfile     # also writes ubunye_entry.py
docker build -t europe-west1-docker.pkg.dev/my-project/ubunye/job:1 .
docker push europe-west1-docker.pkg.dev/my-project/ubunye/job:1
```

The image follows Dataproc's rules (no Spark or Java inside, a `spark` user
1099:1099, `tini` and `procps`) and uses Delta built for Scala 2.13, which
runtime 2.2 runs. The task itself arrives as an archive at run time, so one image
serves every task. `--runtime` (default `2.2`), `--service-account`, `--batch`.

## The run record

The entry script prints the run's record between two markers at the end of the
job; `--record-out` saves it. It is the same record a local run writes, so
`ubunye gate` and `ubunye lineage compare` work across clouds: the same task on
two platforms must write the same data.

## Kubernetes and Azure Container Apps

For a runtime with no managed Spark, the image carries everything: Java, Spark in
local mode, Delta, the engine and your pipelines.

```bash
ubunye deploy dockerfile container --pipelines pipelines --out Dockerfile
docker build -t myregistry/ubunye-jobs:1 . && docker push myregistry/ubunye-jobs:1

# a Kubernetes Job, in kubectl's current context
ubunye deploy k8s -u shop -p orders -t clean --image myregistry/ubunye-jobs:1   --namespace data --env UBUNYE_DATA_ROOT=s3a://my-bucket/data --record-out k8s.json

# an Azure Container Apps job, pulling with a managed identity (no password)
ubunye deploy container-apps -u shop -p orders -t clean --image myacr.azurecr.io/ubunye-jobs:1   -g my-rg --environment my-env --registry-server myacr.azurecr.io   --registry-identity /subscriptions/.../userAssignedIdentities/pull --record-out aca.json
```

The Kubernetes Job has no retries (`backoffLimit: 0`) and is cleaned up a day
after it finishes; `--cpu`, `--memory`, `--timeout`. The Container Apps job is
created the first time and updated after; its output (and so the run record)
reaches Log Analytics a few minutes after the job ends, and the command waits
for it.

## EMR Serverless

```bash
ubunye deploy dockerfile emr-serverless --out Dockerfile     # push it, set it on the application
ubunye deploy emr-serverless -u shop -p orders -t clean   --application-id 00fabc... --role arn:aws:iam::...:role/emr-job --bucket my-bucket
```

This starts the run and returns; the driver's output (with the run record) is in
`s3://<bucket>/logs/`. An AWS account on the free plan cannot use EMR; this
command is tested as a plan, not against a live application.
