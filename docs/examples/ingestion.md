# Ingestion

Getting data in. Four examples: catalog tables with SQL, a public web API,
raw unstructured files, and a real relational database over JDBC.

## 01 · Tables and SQL

Read a catalog table, push a join down as SQL so the database does the work,
and write with `merge` so re-running never duplicates rows.

=== "Databricks"

    Open `examples/01_ingest_tables_sql/notebooks/run_customer_revenue.py` and
    press Run all. Or, from a terminal:

    ```bash
    databricks bundle run ingest_tables_sql --target dev
    ```

=== "Laptop"

    Seed the sample-shaped tables once (see Start Here), then:

    ```bash
    platforms/run_task.sh examples/01_ingest_tables_sql retail ingestion customer_revenue
    ```

=== "Docker"

    ```bash
    docker run --rm --entrypoint bash ubunye-portable:ci -c \
      '. platforms/spark_env.sh && python platforms/seed.py && \
       platforms/run_task.sh examples/01_ingest_tables_sql retail ingestion customer_revenue'
    ```

=== "Kubernetes"

    Use the job template with this example's arguments:

    ```bash
    sed -e "s|{{NAME}}|ingest|" \
        -e 's|{{ARGS}}|"examples/01_ingest_tables_sql", "retail", "ingestion", "customer_revenue"|' \
        platforms/k8s/job-template.yaml | kubectl apply -f -
    ```

## 02 · REST API

Pull a weather forecast from a free public API, with paging, retries and rate
limiting, then fan one JSON document out into rows.

=== "Databricks"

    Open `examples/02_ingest_rest_api/notebooks/run_hourly_forecast.py` and run
    it, or:

    ```bash
    databricks bundle run ingest_rest_api --target dev
    ```

=== "Laptop"

    ```bash
    platforms/run_task.sh examples/02_ingest_rest_api weather ingestion hourly_forecast
    ```

=== "Docker"

    ```bash
    docker compose -f platforms/docker/compose.yml run --rm rest-api
    ```

=== "Kubernetes"

    ```bash
    sed -e "s|{{NAME}}|rest|" \
        -e 's|{{ARGS}}|"examples/02_ingest_rest_api", "weather", "ingestion", "hourly_forecast"|' \
        platforms/k8s/job-template.yaml | kubectl apply -f -
    ```

## 03 · Unstructured files

Read raw text files with the binary reader, then chunk them into overlapping
windows ready for embeddings. The documents are committed to the repository,
so nothing is downloaded while running.

=== "Databricks"

    Open `examples/03_ingest_unstructured/notebooks/run_document_index.py` and
    run it, or:

    ```bash
    databricks bundle run ingest_unstructured --target dev
    ```

=== "Laptop"

    ```bash
    UBUNYE_CORPUS=file:///tmp/ubunye/corpus \
    platforms/run_task.sh examples/03_ingest_unstructured docs ingestion document_index
    ```

=== "Docker"

    Runs as the first step of the models demo:

    ```bash
    docker run --rm --entrypoint /app/platforms/models_demo.sh ubunye-ml:ci
    ```

=== "Kubernetes"

    ```bash
    kubectl apply -f platforms/k8s/job-models.yaml
    ```

## 09 · JDBC, a real database

A parallel read from RNAcentral's public PostgreSQL mirror, 54 million rows.
The lesson: a JDBC read is single threaded unless you tell it not to be, and
this example shows the four settings that split it across connections.

=== "Databricks"

    Plainly: serverless cannot run this one. It ships no JDBC drivers and
    cannot install one, so this example needs a classic cluster on a paid
    workspace, and it has its own bundle:

    ```bash
    cd examples/09_jdbc_ingest && databricks bundle run jdbc_ingest --target dev
    ```

=== "Laptop"

    Works out of the box. The driver is one Maven coordinate, which the
    platform script already passes:

    ```bash
    platforms/run_task.sh examples/09_jdbc_ingest rnacentral ingestion ingest_rna
    ```

=== "Docker"

    ```bash
    docker compose -f platforms/docker/compose.yml run --rm jdbc
    ```

=== "Kubernetes"

    ```bash
    sed -e "s|{{NAME}}|jdbc|" \
        -e 's|{{ARGS}}|"examples/09_jdbc_ingest", "rnacentral", "ingestion", "ingest_rna"|' \
        platforms/k8s/job-template.yaml | kubectl apply -f -
    ```
