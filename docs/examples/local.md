# Examples on your laptop

This page lists the examples that run with no cloud account and no Databricks.
You need Python, Java and one script. Everything else is plain open source
Spark with a local metastore.

How to run them:

```bash
git clone https://github.com/ubunye-ai-ecosystems/ubunye-examples
cd ubunye-examples
platforms/local/run.sh
```

The runner scripts under `platforms/` set up Spark, Delta and a local metastore
for you. Your pipelines never contain that setup, because which Spark you
landed on is the platform's business, not the pipeline's.

## What runs locally

| Example | Note |
|---|---|
| 11 Run anywhere | the baseline every other environment must match, byte for byte |
| 01, 03, 04, 07, 08 | run against seeded tables with the same shape as the Databricks samples |
| 02 REST API | needs only an internet connection |
| 09 JDBC | a parallel read from a real public PostgreSQL database with 54 million rows |
| 05 RAG and 06 fine-tuning | run on local open source models, no hosted endpoint, no API key |
| 10 Three frameworks | the same model in scikit-learn, PyTorch and TensorFlow, identical configs |

## Why the data is different but the test still counts

Locally there is no `samples` catalog, so a seed script creates tables with the
same names and columns, filled with generated data. The pipelines do not change
by one character. The numbers differ, and they are supposed to. What must match
is the process: the same tasks run, the same gates fire, the same contracts
catch the same kinds of mistake.
