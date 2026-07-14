# Deploying on GCP

Same honest status as the AWS page: written, reviewed, wired into CI, and not
yet executed, because Dataproc Serverless has no free tier. The CI jobs say
plainly when they were skipped. Four secrets turn them on with no code change.

## Where a pipeline runs on GCP: Dataproc Serverless

Dataproc Serverless runs Spark batches without a cluster to manage. Like EMR,
it takes a Python file for spark-submit, which `python -m ubunye` exists to
be.

### 1. One-time setup

```bash
bash platforms/gcp/setup.sh
```

This enables the APIs, creates a bucket, and creates a service account that
may touch that one bucket and run Dataproc, nothing more. It prints the
secrets to set. Prefer the keyless option it suggests: a key that does not
exist cannot leak.

### 2. Submit a task

```bash
bash platforms/gcp/submit.sh examples/11_run_anywhere pipelines/portable/ingestion/document_index
```

The script stages code and data to the bucket and submits the batch. It never
passes `spark.master`; Dataproc owns that decision.

### 3. Machine learning pipelines

Dataproc Serverless accepts a custom container image. Push the same ML image
Kubernetes uses to Artifact Registry and pass it with the batch. The exact
commands are in `platforms/gcp/submit.sh` as comments.

## What about Vertex AI?

The same answer as SageMaker on the AWS page. Vertex AI hosts and serves
models; it is not a Spark runtime, so pipelines do not run there. A model the
registry promoted could be pushed to a Vertex endpoint for serving. That
adapter does not exist yet and is tracked as a future addition.

## Model storage

The registry writes to `gs://` paths directly since 0.5.0, once the `gcsfs`
package is installed. So the whole lifecycle, including where the model files
live, can stay inside your GCP project.
