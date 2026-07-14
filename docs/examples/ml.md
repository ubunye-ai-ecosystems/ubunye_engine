# Machine learning

Training models and using them. Two examples: the full lifecycle done
properly, and the same model built in three frameworks to prove the engine
does not care which one you pick.

## 04 · The full lifecycle

Build features with a deterministic split, train, judge the model on rows it
never saw, pass a quality gate, register, promote to production, then score
new data with whatever model is in production. A model that fails the gate is
never registered at all.

=== "Databricks"

    Open the notebooks under `examples/04_ml_taxi_fare/notebooks/` in order,
    or run the whole chain:

    ```bash
    databricks bundle run ml_taxi_fare --target dev
    ```

=== "Laptop"

    Seed once (see Start Here), then run the three tasks in order:

    ```bash
    TAXI_MODEL_STORE=/tmp/ubunye/model_store \
    platforms/run_task.sh examples/04_ml_taxi_fare taxi_fare ml \
      feature_engineering model_training batch_inference
    ```

=== "Docker"

    ```bash
    docker run --rm --entrypoint bash \
      -e TAXI_MODEL_STORE=/data/model_store ubunye-portable:ci -c \
      '. platforms/spark_env.sh && python platforms/seed.py && \
       platforms/run_task.sh examples/04_ml_taxi_fare taxi_fare ml \
         feature_engineering model_training batch_inference'
    ```

=== "Kubernetes"

    ```bash
    sed -e "s|{{NAME}}|ml|" \
        -e 's|{{ARGS}}|"examples/04_ml_taxi_fare", "taxi_fare", "ml", "feature_engineering", "model_training", "batch_inference"|' \
        platforms/k8s/job-template.yaml | kubectl apply -f -
    ```

## 10 · One model, three frameworks

The same model trained in scikit-learn, PyTorch and TensorFlow, behind three
config files that are byte for byte identical. A build check keeps them
identical. It ends in a leaderboard honest enough to say "too close to call"
instead of crowning a half-percent lead.

=== "Databricks"

    ```bash
    databricks bundle run ml_frameworks --target dev
    ```

    The three trainers run in parallel, then the judge waits for all three.

=== "Laptop"

    TensorFlow needs `numpy<2`, so install the pinned trio first:

    ```bash
    pip install torch --extra-index-url https://download.pytorch.org/whl/cpu
    pip install tensorflow "numpy<2" "protobuf<5"
    BENCH_MODEL_STORE=/tmp/ubunye/model_store \
    platforms/run_task.sh examples/10_ml_frameworks fare_bench ml \
      train_sklearn train_torch train_keras select_champion
    ```

=== "Docker"

    The ml image already carries all three frameworks, pinned correctly:

    ```bash
    docker run --rm --entrypoint bash \
      -e BENCH_MODEL_STORE=/data/model_store ubunye-ml:ci -c \
      '. platforms/spark_env.sh && python platforms/seed.py && \
       platforms/run_task.sh examples/04_ml_taxi_fare taxi_fare ml feature_engineering && \
       platforms/run_task.sh examples/10_ml_frameworks fare_bench ml \
         train_sklearn train_torch train_keras select_champion'
    ```

=== "Kubernetes"

    Same command as Docker, inside a Job using the `ubunye-ml:ci` image. Give
    the pod at least 5Gi of memory: three frameworks is a real appetite.
