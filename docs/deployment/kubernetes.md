# Deploying on Kubernetes

This page shows how to run pipelines on a Kubernetes cluster. Everything here
is working code from the examples repository, exercised in CI on every pull
request against a disposable cluster.

## The shape

A pipeline on Kubernetes is a container image plus a Job manifest. The image
carries Spark, Delta, the JDBC driver and the engine. Your task folders are
copied in unchanged. Nothing in any pipeline knows it is inside a container.

## 1. Build the image

```bash
docker build -f platforms/docker/Dockerfile -t ubunye-portable:latest .
```

For machine learning pipelines there is a second image that adds the open
source models with the weights already inside, so pods need no internet:

```bash
docker build -f platforms/docker/Dockerfile.ml \
  --build-arg BASE=ubunye-portable:latest -t ubunye-ml:latest .
```

Push whichever you need to the registry your cluster pulls from.

## 2. Run a task as a Job

`platforms/k8s/job-template.yaml` takes any task:

```bash
sed -e "s|{{NAME}}|my-pipeline|" \
    -e "s|{{ARGS}}|\"examples/02_ingest_rest_api\", \"weather\", \"ingestion\", \"hourly_forecast\"|" \
    platforms/k8s/job-template.yaml | kubectl apply -f -
```

The environment variables in the manifest are the whole configuration surface:
where the data lives, what kind of table to write, and any secrets your
connectors need. Mount them from Kubernetes Secrets in the usual way.

## 3. Run it on a schedule

Wrap the same spec in a CronJob:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: ubunye-nightly
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      # the same template as the Job, unchanged
```

## Two honest notes

**Storage.** A pod's disk vanishes with the pod. Point `UBUNYE_DATA_ROOT` at
object storage or a persistent volume, and give the model registry a mounted
path or an `s3://` style location, which it supports directly since 0.5.0.

**One pod or many.** The manifests run Spark in local mode inside one pod,
which is the honest shape for most scheduled jobs. To fan out across executor
pods instead, set `SPARK_MASTER` to your cluster's API address and give the
job a service account that may create pods. The task folders do not change.
