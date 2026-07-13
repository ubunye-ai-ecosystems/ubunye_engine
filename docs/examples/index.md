# Examples

Worked, deployable pipelines live in their own repository:

[**ubunye-ai-ecosystems/ubunye-examples** on GitHub](https://github.com/ubunye-ai-ecosystems/ubunye-examples){ .md-button .md-button--primary }

They install the engine from PyPI (`ubunye-engine==0.3.0`), so what you run there is
what you get from `pip install` — not an unreleased branch. **Every one of them has
been run on real Databricks**, deployed as a Databricks Asset Bundle, with the output
inspected. Not merely validated. Run.

---

## The abstraction they all share

Every task is the same two files, whatever it does:

```
<usecase>/<package>/<task>/
    config.yaml          where the data comes from, where it goes, Spark settings
    transformations.py   the business rule
```

**ETL or ML is not a distinction the engine makes.** Training a model is a task.
Scoring with one is a task. Aggregating a table is a task. Embedding a document is a
task. They differ in what `transformations.py` says, not in shape.

---

## Ingestion

| | Example | Reads | Shows |
|---|---|---|---|
| **01** | [Structured — tables + SQL](https://github.com/ubunye-ai-ecosystems/ubunye-examples/tree/main/examples/01_ingest_tables_sql) | `samples.bakehouse` | Reading a table *and* pushing a join down as SQL; `merge` (safe to re-run) and `overwrite_partitions` (safe to backfill) |
| **02** | [Structured — REST API](https://github.com/ubunye-ai-ecosystems/ubunye-examples/tree/main/examples/02_ingest_rest_api) | Open-Meteo public API | The `rest_api` connector: query params, rate limiting, retries — and fanning one JSON document out into 168 rows |
| **03** | [Unstructured — text + files](https://github.com/ubunye-ai-ecosystems/ubunye-examples/tree/main/examples/03_ingest_unstructured) | 204 real customer reviews, plus `.txt` files on a volume | Spark's `binaryFile` source; chunking text into overlapping windows, ready to embed |

## Machine learning

| | Example | Tasks | Shows |
|---|---|---|---|
| **04** | [The ML lifecycle](https://github.com/ubunye-ai-ecosystems/ubunye-examples/tree/main/examples/04_ml_taxi_fare) | `feature_engineering` → `model_training` → `batch_inference` | The split written to a table; fit; judge on unseen data; **gate**; register; promote; MLflow — then score with whatever is in production |
| **05** | [RAG](https://github.com/ubunye-ai-ecosystems/ubunye-examples/tree/main/examples/05_rag_documents) | `embed_chunks` → `answer_questions` | Open-source embeddings and an open-source LLM, both served in-workspace. Cosine retrieval, and every answer stored **with the sources that produced it** |
| **06** | [Distil an LLM](https://github.com/ubunye-ai-ecosystems/ubunye-examples/tree/main/examples/06_finetune_llm) | `label_reviews` → `finetune_classifier` | A large open-source LLM labels the corpus once (the teacher); **DistilBERT fine-tunes on CPU** (the student). A model you own — no endpoint, no per-call cost, no rate limit |

!!! note "A model that fails its gate is never registered"

    In examples 04 and 06 the model is judged on data it has never seen, and
    registered **only if it clears the bar**. A model that cannot is not an artifact
    with a warning label attached — it is a failed experiment, and it does not enter
    the registry at all, because somebody downstream will eventually load "the
    production model" without reading the label.

## MLOps

| | Example | Tasks | Shows |
|---|---|---|---|
| **07** | [Data quality — a contract, enforced](https://github.com/ubunye-ai-ecosystems/ubunye-examples/tree/main/examples/07_data_quality) | `validate_transactions` | Rules with severities; bad rows **quarantined, not dropped** — a dropped row is a silent data loss, a quarantined one is a bug report with the payload attached. The run fails when the breach is structural rather than incidental |
| **08** | [Model monitoring & rollback](https://github.com/ubunye-ai-ecosystems/ubunye-examples/tree/main/examples/08_model_monitoring) | `monitor_model` | Input drift vs **target** drift (PSI), decay against the registry baseline, and champion-vs-challengers on live data — then it **acts** |

!!! question "Why monitor at all? Isn't MLflow enough?"

    MLflow is a **logbook**. It records what happened at *training* time and then never
    looks at the model again — it will happily keep reporting `test_r2 = 0.969` while
    the model is wrong on every row, and it cannot change what is serving.

    A gate stops a bad model being **born**. Monitoring catches the one that got past,
    and the one that was fine until the world moved. Example 08 runs one task against
    both failures, and the measured numbers are the lesson:

    - **The world moved** (fares inflate 35%): the *inputs* do not move at all — PSI
      ≤ 0.005 — because inflation changes what a trip *costs*, not how far it *goes*.
      An input-drift dashboard shows **all green** while live error goes 0.45 → 4.33.
    - **A bad model was promoted**: nothing drifts, and *nothing decays* — its live
      error matches its own bad baseline, because it did not rot, it was **born
      broken**. A decay-only monitor serves it forever.

    And the two need **different remedies**. A rollback only ever fixes a model that is
    worse than one you already had; it cannot fix a changed world, because every
    registered version learned the old one. So the monitor scores every version on the
    live window and rolls back only when the evidence says it would help — and says
    *retrain*, with the numbers, when it would not.

---

## Run one in about five minutes

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition) — no credit card.
2. **Workspace → Create → Git folder**, and paste the repo URL. It is public, so you need no token.
3. Open any notebook under `examples/*/notebooks/` and press **Run all**.

The notebook installs the engine, creates the schema and volumes it needs, and runs
the task. Nothing is downloaded from the internet.

Or deploy the lot as a bundle:

```bash
databricks bundle validate --target dev
databricks bundle deploy   --target dev
databricks bundle run ml_taxi_fare --target dev
```

Everything is serverless — there is no `new_cluster` block anywhere in the bundle,
because a free workspace has no other kind of compute.

!!! warning "Serverless blocks most of the internet"

    Outbound access from serverless compute is restricted to a set of trusted
    domains, and which domains is not documented. Measured from the cluster:
    `api.open-meteo.com` and `pypi.org` are reachable; **`raw.githubusercontent.com`
    is not**. That is why none of the examples download their own data — example 03
    commits its corpus to the repo instead. If you write a pipeline that fetches
    something at runtime, run it on the cluster before you believe it.
