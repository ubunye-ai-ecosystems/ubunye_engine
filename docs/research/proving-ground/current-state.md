# Proving ground: the state it starts from

Audited 2026-09-28 against the code, not the docs: engine `hardening/real-world` at
fa1b456 (main is v0.7.1, the latest release and PyPI version), `ubunye-infra` main,
`ubunye-examples` main. The claim being built toward: the same logical workload runs on
different engines, machines and clouds, and Ubunye's own evidence says whether the
result stayed equivalent.

## The evidence Ubunye already produces

The run record (ADR 006, record v2) is already the right unit of proof. Per run it keeps
the engine version, backend, variables, `config_hash`, `code_hash`, `environment` and
its hash, per-step timings, and for every input and output a `data_hash`
(`rows-v1`: every row, order and timezone free, types by one set of names, the same on
Spark and pandas), a `schema_hash` from the same canonical types, and the row count.
`ubunye gate` compares two records rule by rule. The proving ground builds on these; it
does not introduce a second truth.

## What is proven, and where the proof lives

| Environment | Workload | Evidence | Machine-readable |
|---|---|---|---|
| pandas and Spark, local | integration tier: `test_the_same_task_leaves_the_same_receipt_on_both_engines`, `test_same_task_same_data_on_spark_and_pandas`; examples `titanic_local`, `titanic_multitask_local` (`same_receipt.sh`) | CI, pairwise | test result only |
| AWS Glue 5.0 | `11_run_anywhere` (examples repo) | infra `deploy-live` artifact `glue-record` (run record) | yes |
| GCP Dataproc Serverless 2.2 | same | infra `deploy-live` artifact `dataproc-record` | yes |
| Kubernetes (kind) | same | infra `containers-live` artifact `k8s-record` | yes |
| Azure Container Apps | same | infra `containers-live` artifact `aca-record` | yes |
| Databricks serverless | same | infra `databricks-rc`: hashes asserted in the log | no |
| Spark Declarative Pipelines | same | infra `sdp-live`: log | no |
| local, Docker, kind, s3a, spark-submit | same | examples `portability.yml`: a separate fingerprint (`067da98e...`), equal to each other, no reference | no |
| EMR Serverless | built, never run (the AWS free plan blocks EMR) | none | no |

All cloud runs so far used engine 0.7.0 on 2026-09-24/25 and agree: `documents`
`2200b9e5...`, `document_chunks` `95c977d2...`.

## Gaps this programme starts from

1. **No joined result.** Each workflow compares itself with a hard-coded value or one
   other environment; nothing gathers all environments into one comparison, and nothing
   says NOT RUN for an environment that did not run.
2. **Two truths.** The examples' `platforms/fingerprint.py` hashes Delta tables its own
   way; the engine's receipt hash is the one ADR 006 defends. The proving ground uses
   the receipt.
3. **Logs are not evidence.** Databricks and SDP print hashes; they should upload the
   record.
4. **One cloud workload, and it is Spark-only.** `11_run_anywhere` writes Delta; there is
   no workload proven on pandas and on the clouds alike.
5. **No teardown.** Glue and Container Apps jobs and pushed images are kept and reused
   (idle cost near zero, images cost storage); staged data is cleared before a run,
   not after.
6. **Credentials:** AWS, GCP and Azure sandboxes use OIDC; Databricks uses a stored
   token (migration path: Databricks OIDC federation for GitHub Actions); the examples
   repo's cloud jobs use stored keys and are skipped unless set.
7. **Reliability** (hardening experiments E-01 to E-05, `tasks/hardening/`): append is
   not rerun-safe (F-011, F-019), killed runs stay `running` (F-013), pandas lacks
   `overwrite_partitions` and REST (F-012, F-015), hashing costs about 4 s per million
   rows on pandas (F-014).

## Backlog hygiene done on 2026-09-28

Closed as verified done, with evidence in each: #7, #29, #31, #37, #38, #95; PRs #50 and
#51 closed as superseded by the 0.6.0 branch. Still open and still true: #5 (golden
command), #6 (`format: s3` for local paths), #8 (richer plan), #32 (streaming), #42
(registry race), #45 (serving adapters).
