# Machine learning

These examples live in the companion repository
[ubunye-ai-ecosystems/ubunye-examples](https://github.com/ubunye-ai-ecosystems/ubunye-examples).
Run the commands from a clone of it ([setup per place](index.md#before-you-start-once-per-environment)).

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

    Seed once (see [Start here](index.md#before-you-start-once-per-environment)), then run the three tasks in order:

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

    The examples repo has no ready manifest for this one, so here is the
    Docker command wrapped in a Job. It follows `platforms/k8s/job-template.yaml`,
    with the `ubunye-ml:ci` image and at least 5Gi of memory: three frameworks
    is a real appetite. Save it as `job-ml-frameworks.yaml`, then
    `kubectl apply -f job-ml-frameworks.yaml`:

    ```yaml
    apiVersion: batch/v1
    kind: Job
    metadata:
      name: ubunye-ml-frameworks
    spec:
      backoffLimit: 0
      template:
        spec:
          restartPolicy: Never
          containers:
            - name: ubunye
              image: ubunye-ml:ci
              imagePullPolicy: Never # side-loaded into kind
              command: ["bash", "-c"]
              args:
                - >-
                  . platforms/spark_env.sh && python platforms/seed.py &&
                  platforms/run_task.sh examples/04_ml_taxi_fare taxi_fare ml feature_engineering &&
                  platforms/run_task.sh examples/10_ml_frameworks fare_bench ml
                  train_sklearn train_torch train_keras select_champion
              env:
                - name: BENCH_MODEL_STORE
                  value: /data/model_store
                - name: DATA_DIR
                  value: /data
              volumeMounts:
                - name: data
                  mountPath: /data
              resources:
                requests: { memory: "5Gi", cpu: "2" }
                limits: { memory: "7Gi" }
          volumes:
            - name: data
              emptyDir: {}
    ```

    Follow it with `kubectl logs -f job/ubunye-ml-frameworks`.
