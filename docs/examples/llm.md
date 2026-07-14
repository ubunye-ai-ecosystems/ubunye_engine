# Language models

Working with LLMs inside ordinary pipelines. Two examples: retrieval augmented
answers with sources cited, and teaching a small model you own from a large
model's labels.

Both run in two ways, chosen by one environment variable. On Databricks they
use the workspace's hosted model endpoints. Everywhere else, `MODEL_BACKEND=local`
swaps in open source models that run on a CPU with no endpoint and no API key.
The pipelines do not change. Which model answers is business logic, and it
lives in one Python file.

## 05 · RAG, answers with receipts

Chunk documents, embed them, find the chunks relevant to a question, and
answer using only what was found, citing the sources. Needs example 03's
chunks first.

=== "Databricks"

    Run 03 first, then the two notebooks under
    `examples/05_rag_documents/notebooks/`, or:

    ```bash
    databricks bundle run rag_documents --target dev
    ```

=== "Laptop"

    ```bash
    pip install sentence-transformers transformers torch --extra-index-url https://download.pytorch.org/whl/cpu
    MODEL_BACKEND=local \
    platforms/run_task.sh examples/05_rag_documents rag knowledge embed_chunks answer_questions
    ```

    The first run downloads two small models from Hugging Face, about one
    gigabyte total. After that it is fully offline.

=== "Docker"

    The ml image has the weights baked in, so this works with no internet:

    ```bash
    docker run --rm --entrypoint /app/platforms/models_demo.sh ubunye-ml:ci
    ```

=== "Kubernetes"

    ```bash
    kubectl apply -f platforms/k8s/job-models.yaml
    kubectl logs -f job/ubunye-rag
    ```

## 06 · Fine-tune an open LLM

Distillation, end to end. A large model labels customer reviews once, then a
small DistilBERT you own learns from those labels, gets judged on held-out
data, and must pass a quality gate before it is registered. The result costs
nothing per call and no one can rate limit it.

=== "Databricks"

    ```bash
    databricks bundle run finetune_llm --target dev
    ```

    Fine-tuning on CPU takes about ten minutes. That is the slowest example in
    the repository.

=== "Laptop"

    ```bash
    MODEL_BACKEND=local STUDENT_MODEL_STORE=/tmp/ubunye/model_store \
    platforms/run_task.sh examples/06_finetune_llm reviews nlp label_reviews finetune_classifier
    ```

=== "Docker"

    ```bash
    docker run --rm -e WITH_DISTILLATION=true \
      --entrypoint /app/platforms/models_demo.sh ubunye-ml:ci
    ```

=== "Kubernetes"

    Set `WITH_DISTILLATION: "true"` in `platforms/k8s/job-models.yaml` and
    apply it. Give the pod time: the student trains on CPU.
