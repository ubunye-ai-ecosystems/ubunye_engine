# Deploying on AWS

This page shows the ways to run a task on AWS, starting with the one that is
proven. It is for anyone with an AWS account who wants a task to run there.

| Way | Command | Status ([who says](../deployment.md#where-you-can-deploy-today)) |
|---|---|---|
| AWS Glue 5 | `ubunye deploy glue` | Proven: the [proving ground](../proving-ground/latest.md) runs its workloads on Glue and gets the same data as local Spark |
| EMR Serverless, engine command | `ubunye deploy emr-serverless` | Tested as a plan only: an AWS account on the free plan cannot use EMR |
| EMR Serverless, companion repo scripts | `platforms/aws/submit.sh` | Written and wired into CI, never run, for the same reason |
| Classic EMR on EC2 (YARN) | `SPARK_MASTER=yarn` | Not yet run |

## AWS Glue (proven)

**The problem:** you want a task to run on AWS with no cluster to manage.
**When to use it:** this is the default way onto AWS.
**The command:**

```bash
ubunye deploy glue -d pipelines -u shop -p orders -t clean \
  --bucket my-bucket --role arn:aws:iam::123456789012:role/glue-job \
  --record-out glue.json
```

Glue pre-creates a Spark session, and the engine uses it, the same way it does
on Databricks. Glue installs the engine with pip; you build no image.

Before you start, the flags, and the real `--dry-run` output are on
[One command to the cloud](serverless.md#aws-glue).

## EMR Serverless with the companion repo's scripts (not yet run)

EMR Serverless is AWS's run-Spark-without-a-cluster service. It gives you no
shell. It takes a Python file and hands it to `spark-submit`, which is why the
engine ships `python -m ubunye` as an entry point (see
[Anywhere with spark-submit](anywhere.md)).

For the engine's own command, `ubunye deploy emr-serverless`, see
[One command to the cloud](serverless.md#emr-serverless). The rest of this
section is the older script path. The scripts live in the
[examples repository](https://github.com/ubunye-ai-ecosystems/ubunye-examples)
(`platforms/aws/`), not in this one. They are written and wired into CI there,
but have not been run, because a free plan account cannot use EMR. The CI jobs
say in plain words when they were skipped rather than run.

### Before you start

- A clone of the examples repository, and the `aws` CLI signed in
  (`aws configure` or `aws sso login`) as someone with AWS admin rights, for
  the one-time setup only.
- Four values. `setup.sh` (below) creates them and prints them; put them in
  your shell, or as GitHub secrets for the examples repo's CI:

| Variable | What it is | Required |
|---|---|---|
| `AWS_S3_BUCKET` | the bucket for code, data and output | yes |
| `AWS_EMR_APPLICATION_ID` | the EMR Serverless application | yes |
| `AWS_EMR_JOB_ROLE_ARN` | the IAM role the job runs as | yes |
| `AWS_REGION` | the AWS region | no, defaults to `eu-west-1` |

For CI, `setup.sh` also prints how to let GitHub sign in to AWS: an OIDC role
(`AWS_ROLE_TO_ASSUME`) or, less safe, access keys.

### 1. One-time setup

```bash
bash platforms/aws/setup.sh
```

This creates an S3 bucket, an IAM role allowed to touch that one bucket and
nothing else, and an EMR Serverless application. It prints the four values
above. The person running it needs AWS admin rights; the values it produces do
not.

### 2. Submit a task

```bash
bash platforms/aws/submit.sh examples/11_run_anywhere pipelines/portable/ingestion/document_index
```

The script copies the code and data to S3 and starts the job. It never passes
`spark.master`, because EMR sets it; see
[the `spark.master` rule](../deployment.md#the-sparkmaster-rule).

### 3. Machine learning pipelines

EMR Serverless accepts a custom container image. This path uses the companion
repo's Dockerfiles (`platforms/docker/Dockerfile.ml`), not
`ubunye deploy dockerfile`. The same ML image Kubernetes uses, pushed to ECR,
gives EMR the open source models with the weights inside, so jobs in a private
subnet with no internet still work. The exact commands are comments at the top
of `platforms/aws/submit.sh`.

## What about SageMaker?

SageMaker is not a place these pipelines run. It trains and hosts models; it
is not a Spark runtime.

Where SageMaker fits is **after** the pipeline: a model that the registry
trained, gated and promoted could be pushed to a SageMaker endpoint for live
serving. That adapter does not exist yet. It is listed as planned on the
[deployment map](../deployment.md#where-you-can-deploy-today) so the decision
is visible rather than forgotten.

## What about EMR on EC2, or Glue?

Classic EMR clusters use YARN: set `SPARK_MASTER=yarn` in the examples
repository's launcher and the same task runs. This path has not been run yet,
and this page will say so until it has.

Glue is covered by `ubunye deploy glue`, [above](#aws-glue-proven).
