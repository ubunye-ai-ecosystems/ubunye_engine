# Examples on Databricks

This page lists the examples that run on Databricks. A free workspace is
enough. You do not need a card, a cluster, or any setup.

How to run them:

1. Sign up for Databricks Free Edition.
2. In the workspace choose Create, then Git folder, and paste the examples
   repository URL. It is public, no token needed.
3. Open any notebook under `examples/*/notebooks/` and press Run all.

## What each example teaches

| Example | What you learn |
|---|---|
| 01 Tables and SQL | read a table, push a join down as SQL, write safely with merge |
| 02 REST API | pull from a public web API with paging, retries and rate limits |
| 03 Unstructured files | read raw files, chunk text so it is ready for embeddings |
| 04 The ML lifecycle | build features, train, pass a quality gate, register, promote, score |
| 05 RAG | embed documents, find the relevant ones, answer with sources cited |
| 06 Fine-tune an open LLM | a big model labels data, a small model learns from it |
| 07 Data quality | a written contract for your data, bad rows kept aside, not deleted |
| 08 Model monitoring | notice drift and decay, roll back only when it actually helps |
| 11 Run anywhere | the same task that runs on five other environments, run here too |

## One warning worth knowing early

Databricks serverless compute blocks most of the internet. We measured it:
the weather API, PyPI and Hugging Face are reachable, raw.githubusercontent.com
is not. That is why no example downloads its own data while running.
