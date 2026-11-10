# ADR 015: Job settings and the run identity come from platform plugins

**Status:** accepted, 0.9.2

## Context

A run record ([ADR 006](adr-006-run-record.md)) says what ran: the config, the
code, the inputs. It did not say who ran it or where: which job, which run of
it, which workspace, which user or service principal, which commit. Without
that, a team cannot go from a bad table to the deploy that wrote it.

The settings a job gives Ubunye (where run records go, the batch date) were
read from environment variables only. On Databricks serverless compute a job
cannot set environment variables at all. Its job parameters, which a notebook
task sees as widgets, are the only channel. Clusters, laptops and CI do use
environment variables.

Reading either from Databricks means `dbutils` and the notebook context. Those
exist only on Databricks, differ between serverless and classic compute (the
full context JSON is refused on serverless and shared compute), and must never
break a run anywhere else.

## Decision

1. **One settings reader**, `ubunye.core.settings.job_settings(explicit)`,
   returns a frozen `JobSettings`: `run_records`, `run_records_fallback`, `dt`,
   `mode`, the raw `parameters` found, and `source` (where each setting came
   from). The order is explicit, then job parameter, then environment variable.
   An empty parameter is not set. Parameter names are prefixed `ubunye_` except
   `dt`, which jobs already pass under that name. An unknown explicit key is a
   `ValueError` (a typo); nothing else raises.
2. **One identity contract**, `ubunye.core.identity`: a frozen `RunIdentity`
   (platform, workspace, job_id, job_run_id, task_key, run_by, git_commit,
   git_branch, deploy_target; a missing field is `None`, `as_dict()` leaves it
   out) and `collect_identity(settings=None, *, task_dir=None)`, which never
   raises.
3. **Platforms are plugins** in the `ubunye.run_identity` entry point group,
   like backends ([ADR 001](adr-001-backends-are-plugins.md)). A provider has a
   `name`, a `priority`, `parameters()` (its platform's job parameters) and
   `identity(parameters, task_dir)`. Each returns `{}` off its platform. The
   shipped ones: `databricks` (widgets and the notebook context), `ci` (GitHub
   Actions, GitLab, Azure Pipelines, generic `CI`) and `local` (user, host and
   `git rev-parse` in the task folder). The core never names a platform; the
   settings reader gets job parameters from the providers.
4. **The first provider that finds its platform owns the run fields.** Only
   `git_commit` and `git_branch` may come from a later provider. So a laptop's
   user name is never written onto a Databricks run, but a job that passes no
   commit still gets one from a git checkout if there is one.
5. **On Databricks a job parameter beats the context.** The bundle knows what
   it deployed (`${bundle.git.commit}`, `${bundle.git.branch}`,
   `${bundle.target}`); the context knows the job, run, task, user and host.
   The context is read with `safeToJson()` first, then `toJson()`, and only
   scalar fields are kept. `dbutils` is looked up when called, never imported
   at module import: from the IPython namespace, `builtins`, or
   `pyspark.dbutils` only when `DATABRICKS_RUNTIME_VERSION` is set.
6. **The identity lands on the run record as `environment["identity"]`.** Not
   in `variables`, which are the batch's template variables, are redacted for
   secrets and are a Delta column people filter on. Not a new top level field,
   which would need a new record version for what is a description of where the
   run happened. `environment_hash` leaves the `identity` key out, so two runs
   of the same environment still hash the same (each run has its own run id).
   The record version stays 2. `EngineContext.identity` carries it from the
   entry point; when it is empty the recorder collects it itself, so the CLI,
   `run_task`, `run_pipeline` and `notebook` all record it.

## Consequences

- A record answers "which deploy wrote this": in the Delta store,
  `get_json_object(record, '$.environment.identity.git_commit')`.
- The deployment docs say which job parameters a `databricks.yml` passes.
- A new platform (Airflow, Glue, a company's scheduler) is a plugin with no
  engine edit. A broken plugin is logged and skipped.
- The local provider runs `git` once per run record (3 s time out). Off a
  checkout it finds no commit and records none.
- The run identity can name a person (`run_by`). It is what the platform
  already shows in its job run page; it is not a secret, and nothing from the
  context other than the listed fields is kept.
