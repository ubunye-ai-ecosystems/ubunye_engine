# Deploying on AWS

This page is honest about its status: everything below is written, reviewed
and wired into CI, and none of it has been executed, because EMR Serverless is
not covered by any free tier and running it needs a paid account. The CI jobs
say in plain words when they were skipped rather than run. Four secrets turn
them on with no code change.

## Where a pipeline runs on AWS: EMR Serverless

EMR Serverless is AWS's run-Spark-without-managing-a-cluster service, and it
is the natural home for these pipelines. It does not give you a shell. It
takes a Python file and hands it to spark-submit, which is exactly why the
engine ships `python -m ubunye` as an entry point.

### 1. One-time setup

```bash
bash platforms/aws/setup.sh
```

This creates an S3 bucket, an IAM role scoped to that one bucket and nothing
else, and an EMR Serverless application. It prints the four secrets to set on
your repository. The person running it needs AWS admin rights; the secrets it
produces do not.

### 2. Submit a task

```bash
bash platforms/aws/submit.sh examples/11_run_anywhere pipelines/portable/ingestion/document_index
```

The script stages the code and data to S3 and starts the job. Note what it
never passes: `spark.master`. EMR sets that itself, and overriding it would
run the whole job inside the driver while billing you for the cluster.

### 3. Machine learning pipelines

EMR Serverless accepts a custom container image. The same ML image Kubernetes
uses, pushed to ECR, gives EMR the open source models with the weights baked
in, so jobs in a private subnet with no internet still work. The exact
commands are in `platforms/aws/submit.sh` as comments.

## What about SageMaker?

A fair question with a plain answer: SageMaker is not a place these pipelines
run. It is a service for training and hosting models, not a Spark runtime,
and pretending otherwise would mislead you.

Where SageMaker genuinely fits is **after** the pipeline: a model that the
registry trained, gated and promoted could be pushed to a SageMaker endpoint
for live serving. That adapter does not exist yet. It is tracked as a future
addition so the decision is visible rather than forgotten.

## What about EMR on EC2, or Glue?

Classic EMR clusters speak YARN: set `SPARK_MASTER=yarn` in the launcher and
the same task runs. AWS Glue pre-creates a Spark session, which the engine
attaches to automatically, the same way it attaches on Databricks. Neither
path has been executed yet, for the same account reason, and this page will
say so until one has.
