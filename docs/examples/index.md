# Examples

Worked, deployable pipelines live in their own repository:

[**ubunye-ai-ecosystems/ubunye-examples** on GitHub](https://github.com/ubunye-ai-ecosystems/ubunye-examples){ .md-button .md-button--primary }

They install the engine from PyPI, so what you run there is what you get from
`pip install`. Every example has been run for real, with its output inspected.
The one exception is labelled clearly in its own README.

## The idea they all share

Every task is the same two files, whatever it does:

```
<usecase>/<package>/<task>/
    config.yaml          where the data comes from, where it goes, Spark settings
    transformations.py   the business rule
```

ETL or ML is not a distinction the engine makes. Training a model is a task.
Scoring with one is a task. The same task folder runs in every environment below.
Only environment variables change.

## Grouped by environment

### Databricks

A free workspace is enough. Clone the repo as a Git folder, open a notebook,
press run.

| Example | What it teaches |
|---|---|
| 01 Tables and SQL | read a table, push a join down as SQL, write with `merge` |
| 02 REST API | the `rest_api` connector against a public weather API |
| 03 Unstructured files | binary file reading, text chunking for embeddings |
| 04 The ML lifecycle | features, train, quality gate, registry, promote, score |
| 05 RAG | embed, retrieve by cosine, answer with cited sources |
| 06 Fine-tune an open LLM | a large model labels data, DistilBERT learns from it |
| 07 Data quality | a contract with severities, bad rows quarantined not dropped |
| 08 Model monitoring | drift, decay, and a rollback made on evidence |
| 11 Run anywhere | the portability spine, also run here |

### Local machine

No cloud account, no Databricks. Plain open source Spark plus a local metastore.
The runner scripts under `platforms/` set everything up.

| Example | Note |
|---|---|
| 11 Run anywhere | the baseline every other environment must match |
| 01, 03, 04, 07, 08 | run against seeded tables with the same schema as the Databricks samples |
| 02 REST API | needs only outbound internet |
| 09 JDBC | a partitioned parallel read from a real public PostgreSQL database |
| 05 RAG and 06 fine-tuning | run on local open source models, no hosted endpoint |
| 10 Three frameworks | scikit-learn vs PyTorch vs TensorFlow behind identical configs |

### Docker

One image carries the engine, Spark, Delta and the JDBC driver. A second image
adds the open source models with the weights baked in, so it works with no
internet at run time.

```bash
docker compose -f platforms/docker/compose.yml run --rm run-anywhere
```

### Kubernetes

The same images, run as Jobs. Manifests live under `platforms/k8s/`.
CI runs this on every pull request using a kind cluster.

### Object storage

The run anywhere example also runs against a real S3 compatible store through
Hadoop's S3A connector, and must produce the same output hash as every other
environment.

### AWS EMR Serverless and GCP Dataproc Serverless

The submit scripts, IAM setup scripts and CI jobs exist under `platforms/aws/`
and `platforms/gcp/`. They need a cloud account, so they have not been run yet.
The CI jobs say plainly when they were skipped rather than run. Neither service
is covered by a free tier.

## The proof, not the promise

A CI job runs the same task on local Spark, Docker, Kubernetes, object storage
and `spark-submit`, then fails the build unless all of them produced the same
output hash. Databricks produced the same hash as well. A green build in that
repository means something was executed, not merely deployed.

!!! warning "Serverless blocks most of the internet"

    Outbound access from Databricks serverless compute is restricted to a set
    of trusted domains, and which domains is not documented. Measured from a
    real cluster: `api.open-meteo.com`, `pypi.org` and `huggingface.co` are
    reachable. `raw.githubusercontent.com` is not. That is why no example
    downloads its own data at run time.
