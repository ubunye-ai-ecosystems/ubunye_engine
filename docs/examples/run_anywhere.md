# Run anywhere, proven

These examples live in the companion repository
[ubunye-ai-ecosystems/ubunye-examples](https://github.com/ubunye-ai-ecosystems/ubunye-examples).
Run the commands from a clone of it ([setup per place](index.md#before-you-start-once-per-environment)).

This is the example the whole engine stands on. One task folder, run on six
environments, with a build that fails unless every one of them produced the
same rows (the hash ignores row order).

```
laptop        067da98ea04a7d2f...
Docker        067da98ea04a7d2f...
Kubernetes    067da98ea04a7d2f...
object store  067da98ea04a7d2f...
spark-submit  067da98ea04a7d2f...
Databricks    067da98ea04a7d2f...
```

Those are real hashes from this example's own runs, not an illustration.
(The engine's own cross-cloud results are on
[Proving ground: latest results](../proving-ground/latest.md).) The only things
that change between environments are three variables: where the compute is,
what kind of table to write, and where the data lives.

## Run it yourself

=== "Databricks"

    ```bash
    databricks bundle run run_anywhere --target dev
    ```

=== "Laptop"

    ```bash
    platforms/local/run.sh
    ```

    The last line printed is the fingerprint. Compare it with any other
    environment.

=== "Docker"

    ```bash
    docker compose -f platforms/docker/compose.yml run --rm run-anywhere
    ```

=== "Kubernetes"

    ```bash
    kubectl apply -f platforms/k8s/job.yaml
    kubectl logs -f job/ubunye-portable
    ```

## Object storage

The same task also writes to a real S3 compatible store through the same
connector code AWS uses, and must produce the same hash. Against real S3 you
change the endpoint and supply credentials. Nothing else moves. Model files
can live on object storage too: since 0.5.0 the registry accepts `s3://` and
`gs://` paths directly.

## AWS and GCP

The engine itself is proven on the clouds. The
[proving ground](../proving-ground/latest.md) runs its workloads with
`ubunye deploy glue` on AWS Glue and `ubunye deploy dataproc` on GCP Dataproc
Serverless (plus Azure, Kubernetes and Databricks), and every environment gets
the same data as local Spark. That page has the numbers; how to deploy is on
[One command to the cloud](../deployment/serverless.md).

This example's own cloud scripts are a separate path, and not yet run. The
setup scripts, submit scripts and build jobs for EMR Serverless and Dataproc
Serverless exist in the examples repo under `platforms/aws/` and `platforms/gcp/`.
The build jobs say, in plain words, that they were skipped rather than run.
Four secrets turn them on with no code change: see [AWS](../deployment/aws.md)
and [GCP](../deployment/gcp.md).

## What it caught

The check earned its keep on its first day. Databricks once produced a
different hash from everyone else. The pipeline was fine. The data was not:
git had quietly changed the line endings of the input files on one platform.
Nothing crashed anywhere. Only the hash comparison noticed. That is why this
example asserts on output, not on exit codes.
