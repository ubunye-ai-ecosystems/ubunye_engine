# Deploying on GCP

This page shows the ways to run a task on Google Cloud, starting with the one
that is proven. It is for anyone with a GCP project.

| Way | Command | Status ([who says](../deployment.md#where-you-can-deploy-today)) |
|---|---|---|
| Dataproc Serverless, engine command | `ubunye deploy dataproc` | Proven: the [proving ground](../proving-ground/latest.md) runs its workloads on Dataproc Serverless and gets the same data as local Spark |
| Dataproc Serverless, companion repo scripts | `platforms/gcp/submit.sh` | Written and wired into CI, not yet run |

## Dataproc Serverless with `ubunye deploy dataproc` (proven)

**The problem:** you want a task to run on GCP with no cluster to manage.
**When to use it:** this is the default way onto GCP.
**The command:**

```bash
ubunye deploy dataproc -d pipelines -u shop -p orders -t clean \
  --project my-project --region europe-west1 --bucket my-bucket \
  --image europe-west1-docker.pkg.dev/my-project/ubunye/job:1 \
  --record-out dataproc.json
```

The image is built with `ubunye deploy dockerfile dataproc`. Before you start,
the image steps, the flags and the real `--dry-run` output are on
[One command to the cloud](serverless.md#gcp-dataproc-serverless).

## Dataproc Serverless with the companion repo's scripts (not yet run)

Dataproc Serverless runs Spark batches without a cluster to manage. Like EMR,
it takes a Python file for `spark-submit`, which `python -m ubunye` exists to
be (see [Anywhere with spark-submit](anywhere.md)).

The scripts live in the
[examples repository](https://github.com/ubunye-ai-ecosystems/ubunye-examples)
(`platforms/gcp/`), not in this one. The CI jobs there say plainly when they
were skipped.

### Before you start

- A clone of the examples repository, and `gcloud` signed in
  (`gcloud auth login`) to the project, with rights to enable APIs and create
  service accounts, for the one-time setup only.
- Four values. `setup.sh` (below) creates them and prints them; put them in
  your shell, or as GitHub secrets for the examples repo's CI:

| Variable | What it is | Required |
|---|---|---|
| `GCP_PROJECT` | the project ID | yes |
| `GCP_BUCKET` | the bucket for code, data and output | yes |
| `GCP_REGION` | the region | no, defaults to `europe-west1` |
| `GCP_SERVICE_ACCOUNT` | the service account the batch runs as | no; without it Dataproc uses the default one |

For CI, `setup.sh` also prints how to let GitHub sign in: keyless Workload
Identity (`GCP_WORKLOAD_IDENTITY_PROVIDER`, preferred) or a key file
(`GCP_SA_KEY`).

### 1. One-time setup

```bash
bash platforms/gcp/setup.sh
```

This turns on the APIs, creates a bucket, and creates a service account that
may touch that one bucket and run Dataproc, nothing more. It prints the values
above. Prefer the keyless option it suggests: a key that does not exist cannot
leak.

### 2. Submit a task

```bash
bash platforms/gcp/submit.sh examples/11_run_anywhere pipelines/portable/ingestion/document_index
```

The script copies code and data to the bucket and submits the batch. It never
passes `spark.master`; Dataproc owns that choice. See
[the `spark.master` rule](../deployment.md#the-sparkmaster-rule).

### 3. Machine learning pipelines

Dataproc Serverless accepts a custom container image. This path uses the
companion repo's Dockerfiles (`platforms/docker/Dockerfile.ml`), not
`ubunye deploy dockerfile`. Push the same ML image Kubernetes uses to Artifact
Registry and pass it with the batch. The exact commands are comments at the
top of `platforms/gcp/submit.sh`.

## What about Vertex AI?

The same answer as SageMaker on the [AWS page](aws.md#what-about-sagemaker).
Vertex AI hosts and serves models; it is not a Spark runtime, so pipelines do
not run there. A model the registry promoted could be pushed to a Vertex
endpoint for serving. That adapter does not exist yet and is listed as planned
on the [deployment map](../deployment.md#where-you-can-deploy-today).

## Model storage

The model registry writes to `gs://` paths directly (since 0.5.0) once the
`gcsfs` package is installed. So the whole lifecycle, including where the
model files live, can stay inside your GCP project.
