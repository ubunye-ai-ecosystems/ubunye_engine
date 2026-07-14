# Keeping models honest

What happens after training. Two examples: a written contract for incoming
data, and a monitor that notices when a live model goes bad and does something
about it.

## 07 · Data quality, a contract enforced

Rules with severities, written in one readable file. Rows that break a rule
are set aside with the reason attached, never silently dropped. The run fails
loudly when the breach is structural, because quietly quarantining a third of
the data is not a fix, it is a cover-up.

=== "Databricks"

    Open `examples/07_data_quality/notebooks/run_validate_transactions.py` and
    run it, or:

    ```bash
    databricks bundle run data_quality --target dev
    ```

=== "Laptop"

    Seed once (see Start Here), then:

    ```bash
    platforms/run_task.sh examples/07_data_quality quality contracts validate_transactions
    ```

=== "Docker"

    ```bash
    docker run --rm --entrypoint bash ubunye-portable:ci -c \
      ". platforms/spark_env.sh && python platforms/seed.py && \
       platforms/run_task.sh examples/07_data_quality quality contracts validate_transactions"
    ```

=== "Kubernetes"

    ```bash
    sed -e "s|{{NAME}}|quality|" \
        -e "s|{{ARGS}}|\"examples/07_data_quality\", \"quality\", \"contracts\", \"validate_transactions\"|" \
        platforms/k8s/job-template.yaml | kubectl apply -f -
    ```

## 08 · Model monitoring and rollback

Three checks that catch three different failures: has the world changed, has
the model decayed against its own recorded baseline, and is the live model
still the best one in the registry. Then it acts. It rolls back only when a
better version actually exists, and says "retrain" with the numbers when a
rollback would be theatre. Needs example 04's model first.

=== "Databricks"

    ```bash
    databricks bundle run model_monitoring --target dev
    ```

    The notebook runs both failure stories: a drifted world, and a bad model
    promoted by hand. Read the zeros in the output. They are the lesson.

=== "Laptop"

    ```bash
    TAXI_MODEL_STORE=/tmp/ubunye/model_store \
    TAXI_MODELS_DIR=$PWD/examples/04_ml_taxi_fare/models \
    platforms/run_task.sh examples/08_model_monitoring taxi_fare mlops monitor_model
    ```

=== "Docker"

    Run example 04 in the same container first, then the monitor, so the
    registry has a model to watch:

    ```bash
    docker run --rm --entrypoint bash \
      -e TAXI_MODEL_STORE=/data/model_store \
      -e TAXI_MODELS_DIR=/app/examples/04_ml_taxi_fare/models ubunye-portable:ci -c \
      ". platforms/spark_env.sh && python platforms/seed.py && \
       platforms/run_task.sh examples/04_ml_taxi_fare taxi_fare ml \
         feature_engineering model_training batch_inference && \
       platforms/run_task.sh examples/08_model_monitoring taxi_fare mlops monitor_model"
    ```

=== "Kubernetes"

    Same chain as Docker, inside one Job, so the metastore and the registry
    survive between the tasks.
