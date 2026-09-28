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
| 11 (F-008 to F-018) | 7 (F-001 to F-007, in PR #98) | 0 |

## Experiments

| E | Question | Status |
|---|---|---|
| E-01 | Crash mid-write | answered: append doubles the batch 16/16; F-011 to F-014 |
| E-02 | Two runs at once | planned |
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
