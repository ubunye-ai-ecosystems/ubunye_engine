# Deployment

This page is the map for running your tasks somewhere other than your laptop.
It says where you can deploy today, which page has the steps, and the one rule
every target shares. It is for anyone moving a task off a laptop.

Deployment means one thing here: something launches your task, in an
environment, with credentials, often on a schedule. The task folder itself
never changes.

## Which page do I need?

| You want to... | Read |
|---|---|
| run a task as a Databricks job | [Databricks](deployment/databricks.md) |
| sign in to Databricks from CI (token or service principal) | [Databricks authentication](databricks-auth.md) |
| run a task on AWS Glue, GCP Dataproc, Kubernetes, Azure or EMR with one command | [One command to the cloud](deployment/serverless.md) |
| run a task on Azure Container Apps | [Azure](deployment/azure.md) |
| use the companion repo's scripts for AWS or GCP | [AWS](deployment/aws.md), [GCP](deployment/gcp.md) |
| run on your own Kubernetes cluster | [Kubernetes](deployment/kubernetes.md) |
| launch with `spark-submit` on any Spark cluster | [Anywhere with spark-submit](deployment/anywhere.md) |
| check the same task gives the same data everywhere | [Proving ground: latest results](proving-ground/latest.md) |

## Where you can deploy today

This table is the one place that says what is proven. "Proven" means the
[proving ground](proving-ground/index.md) ran real workloads there and got the
same data as local Spark.

| Environment | Command | Status | Page |
|---|---|---|---|
| Databricks | `ubunye deploy databricks`, or Databricks Asset Bundles | Proven, and runs in CI | [Databricks](deployment/databricks.md) |
| AWS Glue 5 | `ubunye deploy glue` | Proven | [One command to the cloud](deployment/serverless.md#aws-glue) |
| GCP Dataproc Serverless | `ubunye deploy dataproc` | Proven | [One command to the cloud](deployment/serverless.md#gcp-dataproc-serverless) |
| Azure Container Apps | `ubunye deploy container-apps` | Proven | [Azure](deployment/azure.md) |
| Kubernetes | `ubunye deploy k8s` | Proven on a local kind cluster | [Kubernetes](deployment/kubernetes.md) |
| Docker images | `ubunye deploy dockerfile` | Working; the base for Kubernetes, Azure, Dataproc and EMR | [One command to the cloud](deployment/serverless.md) |
| AWS EMR Serverless | `ubunye deploy emr-serverless` | Planned: tested as a plan only. A free plan AWS account cannot use EMR | [One command to the cloud](deployment/serverless.md#emr-serverless) |
| AWS EMR Serverless, GCP Dataproc (companion repo scripts) | `platforms/aws/submit.sh`, `platforms/gcp/submit.sh` | Written, not yet run | [AWS](deployment/aws.md), [GCP](deployment/gcp.md) |
| Classic EMR on EC2 (YARN) | `SPARK_MASTER=yarn` in the companion repo's launcher | Not yet run | [AWS](deployment/aws.md#what-about-emr-on-ec2-or-glue) |
| Any Spark cluster with `spark-submit` | `python -m ubunye` | Supported | [Anywhere](deployment/anywhere.md) |
| SageMaker, Vertex AI (serving a promoted model) | none yet | Planned; these serve models, they do not run pipelines | [AWS](deployment/aws.md#what-about-sagemaker), [GCP](deployment/gcp.md#what-about-vertex-ai) |

## The `spark.master` rule

One rule applies everywhere: **the task never chooses its own cluster. Leave
`spark.master` out of your `config.yaml`.**

`spark.master` is the Spark setting that says where the work runs: one
machine (`local[*]`), YARN, Kubernetes, and so on. Whoever launches the job
owns that choice:

- Databricks, Glue, EMR Serverless and Dataproc set it themselves.
- On a laptop, the launcher sets it (the companion repo's `platforms/local/run.sh`
  sets `local[*]`).
- With `spark-submit`, the `--master` flag sets it.

If your config sets `spark.master` and the platform already chose a different
one, the engine stops with a `SparkSessionError` that names both values. It
does this on purpose. Under `spark-submit`, a config that wins this fight makes
the whole job run inside the driver: it ignores every worker, reports success,
and bills you for a cluster it never used. An error is better than that.

The same goes for other settings fixed when Spark starts, such as
`spark.sql.extensions`: see [Engine settings](config/engine.md).

## Two ways to build an image

Kubernetes, Azure, Dataproc and EMR run your task from a container image.
Each deploy page says which of these two it uses:

| Way | Command | Used by |
|---|---|---|
| The engine writes the Dockerfile | `ubunye deploy dockerfile container`, `... dataproc`, `... emr-serverless` | `ubunye deploy k8s`, `container-apps`, `dataproc`, `emr-serverless` |
| The companion repo's Dockerfiles | `docker build -f platforms/docker/Dockerfile .` (and `Dockerfile.ml` for models) | the [ubunye-examples](https://github.com/ubunye-ai-ecosystems/ubunye-examples) scripts and manifests |

## Moved from this page

The Databricks Asset Bundles flow, the GitHub Actions workflow, the Python API
on Databricks and the dev notebooks now live on the
[Databricks page](deployment/databricks.md#databricks-asset-bundles).
