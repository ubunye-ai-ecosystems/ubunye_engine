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
| 9 (F-008 to F-010, F-012, F-014, F-015, F-017, F-018, F-031) | 12 (F-001 to F-007 in PR #98, F-011, F-013, F-016, F-019, F-020) | 0 |

## Experiments

| E | Question | Status |
|---|---|---|
| E-01 | Crash mid-write | answered: append doubled 16/16; after ADR 008 20/20 killed runs right (earlier runs: finished-batch reruns doubled, F-031) |
| E-02 | Two runs at once | answered: append doubled 7/10; after ADR 008 0/10, second run refused by name |
| E-03 | Silent row loss | answered: visible in the record, not enforceable (F-017) |
| E-04 | Secrets in records | answered: secret:// and env safe; --var leaks (F-016); REST needs Spark (F-015) |
| E-05 | Schema drift | answered: gate catches all; a retype writes wrong data at run time (F-018) |
| E-06 | Scale ladder | planned |
| E-07 | Laptop to cluster | planned |
| E-08 | Stranger rerun | planned |

## Scale (E-06): Ubunye overhead against a plain baseline

| Date | Engine | Job | Data | Compute | Plain (s) | Ubunye (s) | Overhead | Notes |
|---|---|---|---|---|---|---|---|---|

## Environments green

| Environment | Last green on this branch |
|---|---|
| Unit tier, Linux/Windows/macOS, Python 3.10 to 3.13 | PR #98 checks |
| Live Spark parity (dev box, Spark 4.2) | 2026-09-28, 49 passed |

## Proving ground (generated evidence: docs/proving-ground/latest.md)

| Workload | pandas-local | spark-local | kubernetes-kind | aws-glue | gcp-dataproc | azure (Container Apps) | databricks |
|---|---|---|---|---|---|---|---|
| c01-portable-etl | PASS | PASS | PASS | PASS | PASS | PASS | PASS |

Digest bb08a7d7a9fd in all seven; infra run 36497932069, engine eba4922, 2026-09-28.
Teardown verified on AWS, GCP and Azure in earlier runs of the same workflow.
