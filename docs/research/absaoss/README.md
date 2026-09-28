# AbsaOSS deep research (2026-09-25)

The full public AbsaOSS organisation (160 repositories), read-only through the
GitHub API: code, tests, docs, releases, and the most discussed issues. Every
claim links to its source; unverifiable points are marked UNVERIFIED.

| Part | Repositories | File |
|---|---|---|
| A. Lineage, control totals, conformance, comparison | Spline (all 9), Atum, Atum Service, Enceladus, hermes, dataset-comparison, datasets-similarity | A_lineage_control.md |
| B. Ingestion, pipelines, formats, streaming | Pramen, Cobrix, fixed-width, ABRiS, py2k, KafkaCase, Hyperdrive (3), Jdbc2S, JMS/IBM MQ binders, EventGate | B_ingestion_formats.md |
| C. Spark libraries, data tooling, ML, databases | spark-data-standardization, spark-commons, commons, spark-hats, spark-hofs, partition sizing, metadata tool, launcher supervisor, Spot, Rialto, mag, fa-db, balta, ultet, login-service, and others | C_spark_ml_db.md |
| D. Engineering practice and platform | living-doc family, release tooling, organizational-workflows, daggerpool, agentic-toolkit, and a classification of all other repositories (41 forks, 30 out of scope) | D_practice_platform.md |

| E. A verdict per repository: does Ubunye need it? | all 160 (list in `absaoss_repos.tsv`) | E_need_verdicts.md |

Status (2026-09-27): parked. These notes are kept as evidence, not as a plan. The
decision was to run Ubunye 0.7 on real work first and let real problems pick what
to build; when one of them points here, start from part E.
