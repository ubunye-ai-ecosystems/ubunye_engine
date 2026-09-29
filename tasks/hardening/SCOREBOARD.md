# Scoreboard

The numbers that say whether Ubunye is getting better. Each row is measured, with its
evidence in a finding or experiment. A number without a baseline is not a result.

## Stranger friction (lower is better)

A newcomer solves a real problem from `pip install` and the public docs only.

| Date | Engine | Problem (data) | Finished | Blocker | Major | Minor | Read source |
|---|---|---|---|---|---|---|---|
| 2026-09-28 | 0.7.1 (PyPI) | Order fact table from 9 tables (Kaggle Olist) | yes | 0 | 3 | 3 | 2 |
| 2026-09-28 | 0.7.1 (PyPI) | LLM sentiment on 300 reviews, local model (Kaggle Amazon Fine Food) | yes | 2 | 1 | 5 | 2 |

## Findings

| Open | Fixed on the branch | Not a bug / won't fix |
|---|---|---|
| 10 (F-008 to F-010, F-015, F-018, F-038, F-039 (item 1 fixed), F-042, F-046, F-048) | 26 (F-001 to F-007 in PR #98, F-011, F-012, F-013, F-014, F-016, F-019, F-020, F-031, F-032, F-034 to F-037, F-045; F-040 and F-043 merged eff8ba8; F-047 merged f569e0a; F-041 merged; F-017 on fix/f017-f018-contracts) | 1 (F-033: a pyarrow bug, worked around, not reported upstream) |

## Experiments

| E | Question | Status |
|---|---|---|
| E-01 | Crash mid-write | answered: append doubled 16/16; after ADR 008 19/20; after F-031 20/20 (2026-09-29: 18 killed runs rerun once each, 2 finished runs refused on rerun); pandas `overwrite_partitions` events 10/10 (F-012) |
| E-02 | Two runs at once | answered: append doubled 7/10; after ADR 008 0/10, second run refused by name (rerun after F-031: 0/10) |
| E-03 | Silent row loss | answered: visible in the record, not enforceable (F-017). After F-017 (`reconcile`, `--contracts`): run exit 1, nothing written, "100 lost" named (was exit 0, 900 of 1000 written) |
| E-04 | Secrets in records | answered: secret:// and env safe; --var leaks (F-016); REST needs Spark (F-015) |
| E-05 | Schema drift | answered: gate catches all; a retype writes wrong data at run time (F-018) |
| E-06 | Scale ladder | answered 2026-09-29: without `--lineage` 1.01x (Spark) and 1.04x to 1.36x (pandas), falling with size: pass; with `--lineage` 1.7x to 4.4x (Spark) and 5.4x to 11.5x (pandas), rising: fail (F-038, F-039, F-041); nothing collected to the driver; Spark record can hash rows it did not write (F-040). After ADR 009 (fix/f040-spark-persist): record equals written files 3/3 (was 0/3); at 5M, `--lineage` 1.83x (was 2.32x) and expectations 1.46x (was 1.74x) on the dev box |
| E-07 | Laptop to cluster | planned |
| E-08 | Stranger rerun | planned |

## Scale (E-06): Ubunye overhead against a plain baseline

| Date | Engine | Job | Data | Compute | Plain (s) | Ubunye (s) | Overhead | Notes |
|---|---|---|---|---|---|---|---|---|
| 2026-09-29 | hardening (F-014) | E-01 task, `--lineage` vs none (1 input, 2 outputs) | 1,000,000 rows, 3 columns | pandas, dev box (Windows, 16 GB) | 1.6 | 4.4 to 4.8 (was 10.8 to 13.4) | 2.9x (was 6.8 to 8.4x) | hash 1.28 s input + 1.60 s output (incl. pandas to Arrow), now in the record; runs as in `tests/experiments/timing.py` |
| 2026-09-29 | hardening 65fb1ed | E-06 job (filter, join, group by; append + overwrite), no `--lineage` / `--lineage` | 1,000,000 rows, generated | pandas, GitHub `ubuntu-latest` (4 vCPU, 16 GB) | 0.81 | 1.10 / 4.39 | 1.36x / 5.43x | median of 3; hash 3.28 s; E-06 |
| 2026-09-29 | hardening 65fb1ed | E-06 job (filter, join, group by; append + overwrite), no `--lineage` / `--lineage` | 5,000,000 rows, generated | pandas, GitHub `ubuntu-latest` (4 vCPU, 16 GB) | 1.88 | 2.36 / 18.22 | 1.25x / 9.67x | median of 3; hash 15.87 s; E-06 |
| 2026-09-29 | hardening 65fb1ed | E-06 job (filter, join, group by; append + overwrite), no `--lineage` / `--lineage` | 20,000,000 rows, generated | pandas, GitHub `ubuntu-latest` (4 vCPU, 16 GB) | 6.09 | 6.99 / 70.14 | 1.15x / 11.52x | median of 3; hash 63.15 s; peak 4.6 GB plain, 3.8 GB Ubunye; E-06 |
| 2026-09-29 | hardening 65fb1ed | E-06 job (filter, join, group by; append + overwrite), no `--lineage` / `--lineage` | 50,000,000 rows, generated | pandas, GitHub `ubuntu-latest` (4 vCPU, 16 GB) | 16.09 | 16.73 / 171.78 | 1.04x / 10.67x | median of 3; hash 155.48 s; peak 12.2 GB plain, 9.1 GB Ubunye; no OOM; E-06 |
| 2026-09-29 | hardening 65fb1ed | E-06 job (filter, join, group by; append + overwrite), no `--lineage` / `--lineage` | 1,000,000 rows, generated | Spark 4.2 local, GitHub `ubuntu-latest` (4 vCPU, 16 GB) | 13.21 | 13.44 / 22.51 | 1.02x / 1.70x | median of 3; hash 9.37 s; E-06 |
| 2026-09-29 | hardening 65fb1ed | E-06 job (filter, join, group by; append + overwrite), no `--lineage` / `--lineage` | 5,000,000 rows, generated | Spark 4.2 local, GitHub `ubuntu-latest` (4 vCPU, 16 GB) | 17.32 | 17.56 / 38.15 | 1.01x / 2.20x | median of 3; hash 20.87 s; E-06 |
| 2026-09-29 | hardening 65fb1ed | E-06 job (filter, join, group by; append + overwrite), no `--lineage` / `--lineage` | 20,000,000 rows, generated | Spark 4.2 local, GitHub `ubuntu-latest` (4 vCPU, 16 GB) | 21.59 | 21.76 / 75.82 | 1.01x / 3.51x | median of 3; hash 54.45 s; with expectations 1.47x (F-043); E-06 |
| 2026-09-29 | hardening 65fb1ed | E-06 job (filter, join, group by; append + overwrite), no `--lineage` / `--lineage` | 50,000,000 rows, generated | Spark 4.2 local, GitHub `ubuntu-latest` (4 vCPU, 16 GB) | 27.87 | 28.03 / 123.16 | 1.01x / 4.42x | median of 3; hash 95.64 s; 147 kB to the driver, as at 5M; E-06 |
| 2026-09-29 | hardening 08fcf8b (before ADR 009) | E-06 job, no `--lineage` / `--lineage` / expectations | 5,000,000 rows, generated | Spark 4.2 local, dev box (Windows, 16 GB) | 15.46 | 17.61 / 35.88 / 26.87 | 1.14x / 2.32x / 1.74x | median of 3, noisy (Ubunye 14.95 to 26.85); jobs 7 / 18 / 18; source read 2 / 5 / 5 times; `devbox-f040-before.jsonl` |
| 2026-09-29 | fix/f040-spark-persist (ADR 009) | E-06 job, no `--lineage` / `--lineage` / expectations | 5,000,000 rows, generated | Spark 4.2 local, dev box (Windows, 16 GB) | 17.47 | 18.73 / 31.93 / 25.56 | 1.07x / 1.83x / 1.46x | median of 3; jobs 7 / 17 / 16; source read 2 / 3 / 2 times; same output digests; `devbox-f040-after.jsonl`; F-039, F-043 |

## Environments green

| Environment | Last green on this branch |
|---|---|
| Unit tier, Linux/Windows/macOS, Python 3.10 to 3.13 | PR #98 checks |
| Live Spark parity (dev box, Spark 4.2) | 2026-09-28, 49 passed; 2026-09-29 partition folders (F-012), 56 passed |

## Proving ground (generated evidence: docs/proving-ground/latest.md)

| Workload | pandas-local | spark-local | kubernetes-kind | aws-glue | gcp-dataproc | azure (Container Apps) | databricks |
|---|---|---|---|---|---|---|---|
| c01-portable-etl | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| r1-food-prices-clean | PASS | PASS | PASS | PASS | PASS | PASS | PASS |
| r1-food-prices-monitor | PASS | PASS | PASS | PASS | PASS | PASS | PASS |

C01: digest bb08a7d7a9fd in all seven, infra run 36580043728 (prove-c01, now a caller of
the reusable prove.yml), engine feat/prove-r1 at 1f5242d, 2026-09-29 (first proven in run
36497932069, engine eba4922). R1 (food prices, both steps in one launch per environment):
digests f61e0f0544f5 and 021cc19ca2b6 in all seven, infra run 36579989167, same engine.
The first R1 run (36576881079) failed on Glue, Dataproc, Container Apps and kind and gave
F-035, F-036 and F-037. Teardown checked after both runs with cloud-cli (2026-09-29): no
Glue job, S3 or GCS object, Artifact Registry or ACR image, Container Apps job, or
Databricks volume or workspace folder of either workload left.
