# Proving ground: latest results

The same workloads, run in seven environments and compared with local Spark. Every row
below passed. How the comparison works: [The proving ground](index.md).

!!! note "Which engine version was proven"
    These results were measured with **engine 0.7.2** (the `Engine` column), on
    2026-09-30. **Engine 0.9.0 has not been re-run on the clouds yet.** Locally, with
    0.9.0, C01 gives the same digest, `bb08a7d7a9fd`, on pandas and on local Spark.

<!-- Do not edit by hand: regenerate from a run. The "## Workload" sections below are
written by `ubunye prove report --md`; this header is written by hand around them. -->

Where the numbers come from: `ubunye prove report` in `ubunye-infra` (workflow
`prove.yml`), engine branch `hardening/real-world` at commit bbb56bb, 2026-09-30, after
the fixes of that day. C01 comes from `prove-c01` run 36709022623, R1 from `prove-r1`
run 36709026536, R2 from `prove-r2` run 36709032414. The machine-readable results are
[c01-portable-etl.json](c01-portable-etl.json),
[r1-food-prices-clean.json](r1-food-prices-clean.json),
[r1-food-prices-monitor.json](r1-food-prices-monitor.json),
[r2-olist-clean.json](r2-olist-clean.json),
[r2-olist-orders_fact.json](r2-olist-orders_fact.json) and
[r2-olist-monthly.json](r2-olist-monthly.json). The run records of each environment stay
in the runs' artifacts, because they carry storage paths.

**R1** (`examples/real-world/food_prices_africa`, the committed sample) runs `clean` then
`monitor` in one launch per environment (`ubunye deploy -t clean -t monitor`). So on
Kubernetes and Container Apps, `monitor` reads what `clean` wrote to the container's own
disk. Each step is compared on its own, so three things are proven: the in-between
output (`prices_per_kg`, 2,229 rows), the monthly table (47 rows) and the alerts (2 rows:
Chad, January 2026, imported rice +59% and wheat flour +78%, 54 markets each). The
digests are the example's golden digests in
`tests/unit/examples/test_food_prices_example.py`.

**R2** (`examples/real-world/olist_ecommerce`, a synthetic sample shaped like the Olist
files) runs `clean`, `orders_fact` and `monthly` in one launch per environment, with
input contracts, quarantine and reconcile on every step. The digests are the example's
golden digests in `tests/unit/examples/test_olist_example.py`.

Databricks runs as a service principal (OAuth machine to machine), not a user token.
Cloud cost is `unknown`: no billing figure was collected yet.

## Workload `c01-portable-etl`

Reference: `spark-local`. Generated 2026-09-30T11:45:06Z from run records; an environment without evidence is NOT RUN.

| Environment | Verdict | Execute | Identity | Inputs | Data | Schema | Rows | Digest | Time | Cost | Engine |
|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | bb08a7d7a9fd | 9.0s | local | 0.7.2 spark |
| aws-glue | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | bb08a7d7a9fd | 28.4s | unknown | 0.7.2 spark |
| azure | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | bb08a7d7a9fd | 10.0s | unknown | 0.7.2 spark |
| databricks | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | bb08a7d7a9fd | 24.8s | unknown | 0.7.2 databricks |
| gcp-dataproc | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | bb08a7d7a9fd | 34.5s | unknown | 0.7.2 spark |
| kubernetes-kind | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | bb08a7d7a9fd | 7.3s | local | 0.7.2 spark |
| pandas-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | bb08a7d7a9fd | 0.5s | local | 0.7.2 pandas |

Digest: the first 12 hex of the SHA-256 over every output's `rows-v1` data hash. Time: the run record's task duration. Cost says where the figure came from; an estimate is never a bill.

## Workload `r1-food-prices-clean`

Reference: `spark-local`. Generated 2026-09-30T11:45:30Z from run records; an environment without evidence is NOT RUN.

| Environment | Verdict | Execute | Identity | Inputs | Data | Schema | Rows | Digest | Time | Cost | Engine |
|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | f61e0f0544f5 | 6.2s | local | 0.7.2 spark |
| aws-glue | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | f61e0f0544f5 | 24.4s | unknown | 0.7.2 spark |
| azure | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | f61e0f0544f5 | 7.2s | unknown | 0.7.2 spark |
| databricks | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | f61e0f0544f5 | 9.6s | unknown | 0.7.2 databricks |
| gcp-dataproc | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | f61e0f0544f5 | 18.1s | unknown | 0.7.2 spark |
| kubernetes-kind | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | f61e0f0544f5 | 6.8s | local | 0.7.2 spark |
| pandas-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | f61e0f0544f5 | 0.3s | local | 0.7.2 pandas |

Digest: the first 12 hex of the SHA-256 over every output's `rows-v1` data hash. Time: the run record's task duration. Cost says where the figure came from; an estimate is never a bill.

## Workload `r1-food-prices-monitor`

Reference: `spark-local`. Generated 2026-09-30T11:45:30Z from run records; an environment without evidence is NOT RUN.

| Environment | Verdict | Execute | Identity | Inputs | Data | Schema | Rows | Digest | Time | Cost | Engine |
|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 021cc19ca2b6 | 4.5s | local | 0.7.2 spark |
| aws-glue | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 021cc19ca2b6 | 25.8s | unknown | 0.7.2 spark |
| azure | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 021cc19ca2b6 | 7.8s | unknown | 0.7.2 spark |
| databricks | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 021cc19ca2b6 | 12.8s | unknown | 0.7.2 databricks |
| gcp-dataproc | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 021cc19ca2b6 | 23.0s | unknown | 0.7.2 spark |
| kubernetes-kind | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 021cc19ca2b6 | 5.3s | local | 0.7.2 spark |
| pandas-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 021cc19ca2b6 | 0.1s | local | 0.7.2 pandas |

Digest: the first 12 hex of the SHA-256 over every output's `rows-v1` data hash. Time: the run record's task duration. Cost says where the figure came from; an estimate is never a bill.

## Workload `r2-olist-clean`

Reference: `spark-local`. Generated 2026-09-30T11:46:13Z from run records; an environment without evidence is NOT RUN.

| Environment | Verdict | Execute | Identity | Inputs | Data | Schema | Rows | Digest | Time | Cost | Engine |
|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6affe2f5382c | 21.7s | local | 0.7.2 spark |
| aws-glue | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6affe2f5382c | 34.3s | unknown | 0.7.2 spark |
| azure | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6affe2f5382c | 26.3s | unknown | 0.7.2 spark |
| databricks | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6affe2f5382c | 58.6s | unknown | 0.7.2 databricks |
| gcp-dataproc | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6affe2f5382c | 55.9s | unknown | 0.7.2 spark |
| kubernetes-kind | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6affe2f5382c | 12.3s | local | 0.7.2 spark |
| pandas-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6affe2f5382c | 0.7s | local | 0.7.2 pandas |

Digest: the first 12 hex of the SHA-256 over every output's `rows-v1` data hash. Time: the run record's task duration. Cost says where the figure came from; an estimate is never a bill.

## Workload `r2-olist-orders_fact`

Reference: `spark-local`. Generated 2026-09-30T11:46:13Z from run records; an environment without evidence is NOT RUN.

| Environment | Verdict | Execute | Identity | Inputs | Data | Schema | Rows | Digest | Time | Cost | Engine |
|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6dcf270328d8 | 5.4s | local | 0.7.2 spark |
| aws-glue | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6dcf270328d8 | 29.1s | unknown | 0.7.2 spark |
| azure | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6dcf270328d8 | 8.0s | unknown | 0.7.2 spark |
| databricks | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6dcf270328d8 | 16.2s | unknown | 0.7.2 databricks |
| gcp-dataproc | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6dcf270328d8 | 21.4s | unknown | 0.7.2 spark |
| kubernetes-kind | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6dcf270328d8 | 3.1s | local | 0.7.2 spark |
| pandas-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | 6dcf270328d8 | 0.1s | local | 0.7.2 pandas |

Digest: the first 12 hex of the SHA-256 over every output's `rows-v1` data hash. Time: the run record's task duration. Cost says where the figure came from; an estimate is never a bill.

## Workload `r2-olist-monthly`

Reference: `spark-local`. Generated 2026-09-30T11:46:13Z from run records; an environment without evidence is NOT RUN.

| Environment | Verdict | Execute | Identity | Inputs | Data | Schema | Rows | Digest | Time | Cost | Engine |
|---|---|---|---|---|---|---|---|---|---|---|---|
| spark-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | a0bd206029ca | 3.1s | local | 0.7.2 spark |
| aws-glue | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | a0bd206029ca | 26.1s | unknown | 0.7.2 spark |
| azure | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | a0bd206029ca | 4.4s | unknown | 0.7.2 spark |
| databricks | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | a0bd206029ca | 11.8s | unknown | 0.7.2 databricks |
| gcp-dataproc | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | a0bd206029ca | 18.9s | unknown | 0.7.2 spark |
| kubernetes-kind | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | a0bd206029ca | 1.9s | local | 0.7.2 spark |
| pandas-local | **PASS** | PASS | PASS | PASS | PASS | PASS | PASS | a0bd206029ca | 0.1s | local | 0.7.2 pandas |

Digest: the first 12 hex of the SHA-256 over every output's `rows-v1` data hash. Time: the run record's task duration. Cost says where the figure came from; an estimate is never a bill.
