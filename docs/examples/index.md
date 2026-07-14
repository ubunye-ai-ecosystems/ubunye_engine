# Examples: start here

This section shows the engine doing real work, in every environment it runs in.
Pick the page that matches where you want to run things. The pipelines are the
same on every page. Only the environment changes.

All example code lives in one repository:

[**ubunye-ai-ecosystems/ubunye-examples** on GitHub](https://github.com/ubunye-ai-ecosystems/ubunye-examples){ .md-button .md-button--primary }

## The one idea every example shares

A pipeline is two files, whatever it does:

```
<usecase>/<package>/<task>/
    config.yaml          what data comes in, where results go, Spark settings
    transformations.py   your business rule
```

Moving between environments never changes these files. Three environment
variables do all the work: where the compute is, what kind of table to write,
and where the data lives.

## Why you can trust these pages

Every example has been run for real, with its output inspected. One test goes
further: it runs the same pipeline on the laptop setup, in Docker, on
Kubernetes, against object storage and on Databricks, then checks that all of
them produced the same output, byte for byte. The build fails if they ever
disagree. A green build here means something was executed, not just deployed.

The one exception is the JDBC example on Databricks itself, which needs a paid
cluster type. Its page says so plainly instead of letting you assume.
