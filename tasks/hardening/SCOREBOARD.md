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
| 6 (F-008 to F-010, F-015, F-017, F-018) | 16 (F-001 to F-007 in PR #98, F-011, F-012, F-013, F-014, F-016, F-019, F-020, F-031, F-032) | 1 (F-033: a pyarrow bug, worked around, not reported upstream) |

## Experiments

| E | Question | Status |
|---|---|---|
| E-01 | Crash mid-write | answered: append doubled 16/16; after ADR 008 19/20; after F-031 20/20 (2026-09-29: 18 killed runs rerun once each, 2 finished runs refused on rerun); pandas `overwrite_partitions` events 10/10 (F-012) |
| E-02 | Two runs at once | answered: append doubled 7/10; after ADR 008 0/10, second run refused by name (rerun after F-031: 0/10) |
| E-03 | Silent row loss | answered: visible in the record, not enforceable (F-017) |
| E-04 | Secrets in records | answered: secret:// and env safe; --var leaks (F-016); REST needs Spark (F-015) |
| E-05 | Schema drift | answered: gate catches all; a retype writes wrong data at run time (F-018) |
| E-06 | Scale ladder | planned |
| E-07 | Laptop to cluster | planned |
| E-08 | Stranger rerun | planned |

## Scale (E-06): Ubunye overhead against a plain baseline

| Date | Engine | Job | Data | Compute | Plain (s) | Ubunye (s) | Overhead | Notes |
|---|---|---|---|---|---|---|---|---|
| 2026-09-29 | hardening (F-014) | E-01 task, `--lineage` vs none (1 input, 2 outputs) | 1,000,000 rows, 3 columns | pandas, dev box (Windows, 16 GB) | 1.6 | 4.4 to 4.8 (was 10.8 to 13.4) | 2.9x (was 6.8 to 8.4x) | hash 1.28 s input + 1.60 s output (incl. pandas to Arrow), now in the record; runs as in `tests/experiments/timing.py` |

## Environments green

| Environment | Last green on this branch |
|---|---|
| Unit tier, Linux/Windows/macOS, Python 3.10 to 3.13 | PR #98 checks |
| Live Spark parity (dev box, Spark 4.2) | 2026-09-28, 49 passed; 2026-09-29 partition folders (F-012), 56 passed |

## Proving ground (generated evidence: docs/proving-ground/latest.md)

| Workload | pandas-local | spark-local | kubernetes-kind | aws-glue | gcp-dataproc | azure (Container Apps) | databricks |
|---|---|---|---|---|---|---|---|
| c01-portable-etl | PASS | PASS | PASS | PASS | PASS | PASS | PASS |

Digest bb08a7d7a9fd in all seven; infra run 36497932069, engine eba4922, 2026-09-28.
Teardown verified on AWS, GCP and Azure in earlier runs of the same workflow.
