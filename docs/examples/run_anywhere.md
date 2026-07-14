# Run anywhere, proven

This is the example the whole engine stands on. One task folder, run on six
environments, with a build that fails unless every one of them produced the
same output, byte for byte.

```
laptop        067da98ea04a7d2f...
Docker        067da98ea04a7d2f...
Kubernetes    067da98ea04a7d2f...
object store  067da98ea04a7d2f...
spark-submit  067da98ea04a7d2f...
Databricks    067da98ea04a7d2f...
```

Those are real hashes from real runs, not an illustration. The only things
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

## AWS and GCP, written and waiting

The setup scripts, submit scripts and build jobs for EMR Serverless and
Dataproc Serverless all exist under `platforms/aws/` and `platforms/gcp/`.
They have not been executed, because neither service has a free tier and
running them needs a paid account. The build jobs say, in plain words, that
they were skipped rather than run. Four secrets turn them on with no code
change.

## What it caught

The check earned its keep on its first day. Databricks once produced a
different hash from everyone else. The pipeline was fine. The data was not:
git had quietly changed the line endings of the input files on one platform.
Nothing crashed anywhere. Only the hash comparison noticed. That is why this
example asserts on output, not on exit codes.
