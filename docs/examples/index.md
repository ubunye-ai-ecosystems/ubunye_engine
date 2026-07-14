# Examples: start here

This section is organised by what you want to do: ingest data, build machine
learning, keep models honest, work with language models. Inside every example
there is a small toggle showing how to run it on each environment.

The toggle remembers your choice. Click Databricks once and every example on
every page shows you the Databricks way. Click Laptop and everything switches.
The pipelines themselves never change between tabs. That is the whole point of
the engine.

All example code lives in one repository:

[**ubunye-ai-ecosystems/ubunye-examples** on GitHub](https://github.com/ubunye-ai-ecosystems/ubunye-examples){ .md-button .md-button--primary }

## Before you start, once per environment

=== "Databricks"

    Sign up for Databricks Free Edition (no card). In the workspace choose
    Create, then Git folder, and paste the examples repository URL. It is
    public, so you need no token. Every example is then a notebook you open
    and run.

=== "Laptop"

    You need Python, Java 17 and an internet connection.

    ```bash
    git clone https://github.com/ubunye-ai-ecosystems/ubunye-examples
    cd ubunye-examples
    pip install "pyspark==3.5.3" "delta-spark==3.2.0" "ubunye-engine[spark,ml]" "requests"
    ```

    The scripts under `platforms/` handle Spark, Delta and a local metastore.
    Examples that read the Databricks sample tables need seeded stand-ins with
    the same shape, created once:

    ```bash
    . platforms/spark_env.sh && python platforms/seed.py
    ```

=== "Docker"

    You need Docker only. Build the image once:

    ```bash
    docker build -f platforms/docker/Dockerfile -t ubunye-portable:ci .
    ```

    The machine learning examples use a second image with the open source
    model weights baked in, so nothing is downloaded at run time:

    ```bash
    docker build -f platforms/docker/Dockerfile.ml --build-arg BASE=ubunye-portable:ci -t ubunye-ml:ci .
    ```

=== "Kubernetes"

    Any cluster works. For a disposable local one:

    ```bash
    kind create cluster --name ubunye
    kind load docker-image ubunye-portable:ci --name ubunye
    ```

    Job manifests live under `platforms/k8s/`.

## Why you can trust these pages

Every example has been run for real, with its output inspected. One test goes
further: it runs the same pipeline on the laptop, in Docker, on Kubernetes,
against object storage and on Databricks, then fails the build unless all of
them produced the same output, byte for byte. See [Run anywhere](run_anywhere.md).
