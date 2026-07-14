# Examples on object storage and clouds

This page covers the cloud side: writing to object storage, and running on AWS
and GCP. It is honest about which parts have been executed and which are
waiting for a cloud account.

## Object storage, proven

The run anywhere example writes to a real S3 compatible store through the same
connector code AWS uses. The output hash must match the laptop, Docker,
Kubernetes and Databricks runs, and the build fails if it does not. Against
real S3 you change the endpoint and supply credentials. Nothing else moves.

Model files can live on object storage too. Since version 0.5.0 the model
registry accepts `s3://` and `gs://` paths, and anyone can add a new storage
scheme with one class and one entry point.

## AWS EMR Serverless and GCP Dataproc Serverless, written and waiting

Everything needed exists in the examples repository:

- `platforms/aws/setup.sh` and `platforms/gcp/setup.sh` create the bucket, the
  scoped role and the application, then print the secrets to set.
- `platforms/aws/submit.sh` and `platforms/gcp/submit.sh` stage the code and
  submit the job.
- The build jobs exist and are gated on secrets. When the secrets are missing
  they say, in plain words, that they were skipped rather than run.

They have not been executed, because neither service is covered by any free
tier and running them needs a paid account. We say that here instead of
letting a green build imply otherwise. The moment an account exists, four
secrets turn these jobs on with no code change.
