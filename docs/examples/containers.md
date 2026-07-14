# Examples in Docker and Kubernetes

This page shows the same pipelines running inside containers. Nothing in any
pipeline is container-aware. The image supplies Spark, Delta, the JDBC driver
and the engine. The task folders are copied in unchanged.

## Docker

One command per example:

```bash
docker compose -f platforms/docker/compose.yml run --rm run-anywhere
docker compose -f platforms/docker/compose.yml run --rm rest-api
docker compose -f platforms/docker/compose.yml run --rm jdbc
```

There is a second image for the machine learning examples. It carries the open
source models with the weights already inside, so it works with no internet at
run time. A container that downloads a gigabyte on every start secretly depends
on the network, and a locked-down cluster does not have one.

## Kubernetes

The same images run as Jobs. Manifests live under `platforms/k8s/`. The build
system runs this on every pull request using a small disposable cluster, so it
cannot quietly rot.

RAG and fine-tuning both run this way too: seed the data, build the document
index, answer questions, train the student model, all inside one pod with no
Databricks anywhere.
