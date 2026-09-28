# AbsaOSS family B: ingestion, pipelines, formats, streaming

Deep pass for the Ubunye Engine enterprise data problem catalogue. Research date 2026-09-25. Read only; every repo was inspected through the authenticated GitHub API (README, recursive tree, core source files, tests layout, releases, issues sorted by comments and reactions). Citations are reference links collected at the end of each section; the full link list is at the bottom. Claims about non AbsaOSS tools (Spark Declarative Pipelines, Dagster, dbt, Kafka EOS and so on) are marked EXTERNAL where they come from general knowledge and were not re-verified in this pass; anything that could not be verified is marked UNVERIFIED.

Repos covered: pramen, cobrix, fixed-width, ABRiS, py2k, KafkaCase, hyperdrive, hyperdrive-trigger, hyperdrive-hive-jobs, Jdbc2S, spring-cloud-stream-binder-jms, spring-cloud-stream-binder-ibm-mq, EventGate.

Activity snapshot (from `gh api repos/AbsaOSS/<r>`, 2026-09-25):

| Repo | Stars | Open issues | Last push | Latest release |
|---|---|---|---|---|
| pramen | 31 | 37 | 2026-09-24 | v1.15.0, 2026-09-24 (Spark 4 + JVM 17) [pr-rel] |
| cobrix | 170 | 102 | 2026-09-17 | v2.11.1, 2026-09-17 [cb-rel] |
| ABRiS | 242 | 24 | 2026-09-02 | v7.0.0-RC1, 2026-09-02 (Spark 4 only) [ab-rel7] |
| hyperdrive | 47 | 12 | 2025-07-17 | v4.7.0, 2022-08-05 [hd-rel] |
| hyperdrive-trigger | 8 | 32 | 2025-11-27 | v0.5.22, 2024-05-22 [ht-rel] |
| EventGate | 4 | 15 | 2026-09-22 | v1.4.0, 2026-07-08 [eg-rel] |
| Jdbc2S | 10 | 8 | 2024-02-19 | n/a |
| py2k | 13 | 20 | 2023-02-27 | n/a |
| fixed-width | 4 | 7 | 2023-05-05 | n/a |
| KafkaCase | 0 | 0 | 2025-01-28 | n/a |
| hyperdrive-hive-jobs | 0 | 0 | 2026-03-19 | n/a |
| spring-cloud-stream-binder-jms | 1 | 4 | 2017-04-13 | fork of spring-attic |
| spring-cloud-stream-binder-ibm-mq | 1 | 0 | 2017-04-13 | fork of spring-attic |

The picture: pramen, cobrix and ABRiS are alive and shipping in 2026; EventGate is the new, actively developed piece (2025 to 2026); the hyperdrive family is in maintenance; the rest are dormant or archival.

---

## 1. Pramen (the most important repo for Ubunye)

### Purpose and production problem

Pramen is "a framework for defining data pipelines based on Spark and a configuration driven tool to run and coordinate those pipelines" [pr-readme]. Its stated differentiators are exactly Ubunye's current gaps: "Auto-healing as much as possible. Keeping pipeline state allows quicker recovery... Jobs that already succeeded won't run again by default. Handling of late data and retrospective updates to data in data sources by re-running dependent jobs. Handling of schema changes from data sources" [pr-readme]. The production problem is a bank with "numerous heterogeneous data sources that aren't integrated" needing "automatic data loading and recovery (including missed and late data sources)", "automatic data reloading (partial or incorrect data load)", and warnings about "changes to upstream schema" and "sourcing performance thresholds" [pr-readme]. The design centre is T+1 daily batch: "Pramen is designed for updates coming from source systems daily or less frequently" [pr-readme-dates].

### Core abstraction and data model

**Three plug-in contracts** [pr-source][pr-transformer][pr-sink]:
- `Source.getRecordCount(query, infoDateBegin, infoDateEnd)`, `getData(...)`, `getDataIncremental(query, onlyForInfoDate, offsetFrom, offsetTo, columns)`, `getOffsetInfo`, `validate(...) : Reason`, `hasInfoDateColumn`, `postProcess(...)`.
- `Transformer.validate(metastore, infoDate, options) : Reason` and `run(metastore, infoDate, options) : DataFrame`. The transformer never writes; the framework does.
- `Sink.send(df, tableName, metastore, infoDate, options) : SinkResult(recordsSent, filesSent, hiveTables, warnings)` [pr-sinkresult].
- `Reason` is a five-way verdict: `Ready | Warning | NotReady | Skip | SkipOnce` [pr-reason].

**The information date** is the central idea. Every metastore table is partitioned by `info_date`, "a generalized concept that unifies snapshot date for entities and event date for events" [pr-readme-dates]. "A chunk of data in a metastore table for specific information date is considered an immutable atomic portion of data and a minimal batch" [pr-readme-infodate]. Run date is mapped to info date by a DSL expression (`info.date.expr = "@runDate - 1"`), and dependency windows are expressions over `@infoDate` (`date.from = "lastMonday(@infoDate) - 7"`). The DSL has a hand-written lexer and parser (`expr/lexer/Lexer.scala`, `expr/parser/Parser.scala`) [pr-tree] with functions such as `beginOfMonth`, `lastMonday`, `plusMonths`, `yearMonthOf` [pr-readme-expr].

**Metastore table definition** (`MetaTableDef`): `name, description, format, infoDateColumn, infoDateFormat, partitionScheme, batchIdColumn, hiveTable, hivePath, infoDateStart, tableProperties, readOptions, writeOptions` [pr-metatabledef]. Formats: `parquet`, `delta`, `iceberg`, `raw` (files kept as is), `transient` (in-run only, with `cache.policy`) [pr-readme-meta][pr-tree]. `PartitionScheme = PartitionByDay | PartitionByMonth | PartitionByYearMonth | PartitionByYear | NotPartitioned | Overwrite` [pr-partscheme]. Guard rails: `information.date.start` ("beyond which Pramen won't allow writes") and `information.date.max.days.behind` for archived tables [pr-readme-meta].

**Bookkeeping schema** (Slick table definitions, pluggable backends: JDBC/PostgreSQL recommended, MongoDB, DynamoDB, Hadoop CSV/JSON, Delta) [pr-readme-bk][pr-tree]:

| Table | Key fields | Role |
|---|---|---|
| `bookkeeping` | `watcher_table_name, info_date, info_date_begin, info_date_end, input_record_count, output_record_count, appended_record_count, job_started, job_finished, batch_id` | One row per successful (table, info date) chunk. The source of truth for "already ran" [pr-bkrec]. |
| `schemas` | `pramenTableName, infoDate, schemaJson` | Last schema per table and date, used for drift detection [pr-schemarec]. |
| `offsets` | `table_name, info_date, data_type, min_offset, max_offset, batch_id, created_at, committed_at (nullable)` | Two-phase incremental offsets; unique on (table, info_date, created_at) [pr-offsettable]. |
| `metadata` | `pramenTableName, infoDate, key, value, lastUpdated` | Per-partition key/value metadata [pr-metarec]. |
| `journal` | `job_name, watcher_table_name, period_begin/end, information_date, input_record_count(_old), output_record_count(_old), appended_record_count, output_size, started_at, finished_at, status, failure_reason, spark_application_id, pipelineId, pipelineName, environmentName, tenant, country, batch_id` | Append-only task history [pr-journal]. |
| `executions` | `pipeline_id, pipeline_definition_id, batch_id, spark_application_id, compute_engine_id, run_date_from/to, executors min/max, executor_type, status, is_rerun, attempt_number, number_of_attempts, number_of_records_ingested, max_number_of_columns, additional_options` | Pipeline level journal, added in #741 [pr-exec][pr-741]. |
| `lock_tickets` | `token (PK), owner, expires, created_at` | Distributed lease locks [pr-locktable]. |
| `bulk_loads` | per (table, output info date) phase | Resume state for bulk history loads [pr-readme-bulk]. |

Note the `_old` counts: the journal records the previous record count next to the new one for every rerun, which is a built-in before/after reconciliation trail [pr-journal][pr-taskcompleted].

**Run status vocabulary** is rich and typed: `Succeeded(recordCountOld, recordCount, recordsAppended, sizeBytes, reason, filesRead, filesWritten, hiveTablesUpdated, warnings) | ValidationFailed | Failed | MissingDependencies | FailedDependencies | NoData(isFailure) | InsufficientData(actual, expected, recordCountOld) | NotRan | Skipped(msg, isWarning)` [pr-runstatus]. The run reason is also typed: `New | Update | Rerun | Late | Skip | OnRequest` [pr-runreason]. `Succeeded` carries `filesRead`, `filesWritten` and `hiveTablesUpdated`, so lineage facts travel with the status.

### Run modes, backfill, late data and retrospective change detection

Run modes: `fill_gaps` (SkipAlreadyRan), `check_updates`, `force`, `bulk` [pr-runmode]; plus CLI flags `--rerun`, `--date-from/--date-to`, `--inverse-order`, `--dry-run`, `--check-late-only`, `--check-new-only`, `--undercover` ("will not update bookkeeper so any changes caused by the pipeline won't be recorded. Useful for re-running historical transformations without triggering execution of the rest of the pipeline"), `--use-lock`, `--skip-locked`, `--attempt/--max-attempts` [pr-readme-cli].

The normal run planner (`ScheduleStrategySourcing.getDaysToRun`) unions four date sets, deduplicates by info date and sorts them [pr-strat]:
1. **Backfill days**: dates in the `backfill.days` window with no bookkeeping chunk (`getDataAvailability`), reason `Late`.
2. **Tracked days**: every date in the `track.days` window, reason `Late`, re-checked for changes.
3. **Late days**: from the next expected info date after `getLatestProcessedDate` up to yesterday (or from `initial.sourcing.date.expr` on first run).
4. **New day**: today's info date if the schedule is enabled.

**Retrospective change detection for sources is a record count comparison.** In `IngestionJob.preRunCheckJob` the job calls `source.getRecordCount` for the window and compares it with the bookkept `inputRecordCount`: equal means `AlreadyRan`, different means `NeedsUpdate` and a re-source, below `minimum.records` means `InsufficientData` [pr-ingestion]. The README describes the same: "For this check Pramen will query sources for record counts for each of previous days... If a mismatch is found... the data is reloaded and dependent transformers are recomputed" [pr-readme-default].

**Retrospective change propagation for transformations is a timestamp comparison** (make style). `JobBase.getOutdatedTables` checks, for the same info date, whether any `trigger.updates` dependency chunk has `jobFinished >= ` this job's own `jobFinished`; if so it returns `NeedsUpdate` with the warning "Based on outdated tables: ..." [pr-jobbase].

**Historical ranges** (`getHistorical`) use `getDataChunksCount` per date: `fill_gaps` skips dates with chunks, `force` reruns with reason `Rerun`, `check_updates` marks `Update` [pr-stratutils]. **Bulk mode** (2026, #788) loads monthly/quarterly/yearly chunks into one info date, tracks each period in `bulk_loads` with a `Pending` phase, refuses to continue if the stored date range differs from the requested one, and "if a job is interrupted and later restarted, Pramen resumes processing from where it left off" [pr-stratutils][pr-788][pr-readme-bulk].

### Incremental ingestion, offsets and at-least-once semantics

Enabled with `schedule = "incremental"` and `offset.column { name, type }` where type is `integral | datetime | string` (plus a Kafka partition/offset map type in code) [pr-readme-incr][pr-offsetvalue]. `KafkaValue.compareTo` throws if partitions differ or if "some offsets are bigger, some are smaller", i.e. it refuses to order non-comparable vector offsets instead of guessing [pr-offsetvalue].

The write protocol in `IncrementalIngestionJob.save` [pr-incr]:
1. `om.startWriteOffsets(table, infoDate, type)` inserts an uncommitted offset row (`committed_at = null`).
2. Append (or overwrite on rerun) the data with a `pramen_batchid` column.
3. Re-read the written batch, compute min/max offset **from the data actually written**, then `commitOffsets` (or `commitRerun`); on any exception `rollbackOffsets`.

**Crash recovery is self-healing from the data.** Before the next run, `validateUncommittedOffsets` finds uncommitted rows; `handleUncommittedOffsetsForDay` reads the output partition: if it is empty or missing it rolls the offsets back; otherwise it recomputes min/max from the table, commits a fresh offset and deletes the stale uncommitted rows [pr-incr]. Offsets are therefore derived from the sink, not trusted from the process that crashed. The README is honest that this is still "AT LEAST ONCE" for incremental transformers and sinks: "if update failed in the middle, duplicates are possible on next runs" and "for incremental sinks, such as Kafka sink duplicates still might happen" [pr-readme-incr]. Retrospective incremental runs without an info date column are refused: `Reason.Skip("Incremental ingestion cannot be retrospective")` [pr-incr].

### Dependencies, parallelism and locking

Dependencies are declarative per operation: `tables, date.from, date.to, trigger.updates, optional, passive` [pr-readme-deps][pr-metadep]. A "strict dependency management" mode makes dependencies passive by default [pr-readme-strict]. Parallelism: `pramen.parallel.tasks`, per operation `allow.parallel=false` for self-dependent transformations and `consume.threads` as a weight ("does not really run on 3 threads... gives an indication... that it is a resource-intensive task") [pr-readme-par].

Locking: one lease per **(output table, info date)**, token `s"${outputTable.name}_${infoDate}"` [pr-taskrunner]. `TokenLockBase` acquires with 3 retries, holds a 10 minute ticket, runs a daemon watcher that renews every TTL/5, and registers a JVM shutdown hook to release [pr-lockbase]. If the lock is held, the task fails with "Another instance is already running" or is skipped with `--skip-locked` [pr-taskrunner]. Transient tables bypass locking. Backends: JDBC, MongoDB, DynamoDB, Hadoop path [pr-tree].

### Schema change detection and notifications

`TaskRunnerBase.handleSchemaChange` loads the last saved schema for the table (`bookkeeper.getLatestSchema`), diffs it with the new DataFrame schema and, if different, logs "SCHEMA CHANGE", saves the new schema and returns `SchemaDifference(tableName, infoDateOld, infoDateNew, changes)` with `FieldChange = NewField | DeletedField | ChangedType` [pr-taskrunner][pr-schemadiff][pr-fieldchange]. It runs twice, before and after `schemaTransformations`, and forces a Hive table refresh when anything changed. Raw tables and `ignoreSchemaChange` operations skip it. The diffs go into the email notification; pipeline and job notification targets are pluggable interfaces [pr-readme-notif].

### Python side (pramen-py)

pramen-py is a CLI and library: transformers subclass `pramen_py.Transformation`, are discovered through a `transformations` namespace package and are exposed as `pramen-py transformations run <Name>` commands; a pytest plugin ships fixtures [pr-py-readme]. The Scala `PythonTransformationJob` renders a metastore YAML, then either shells out through `pramen.py.cmd.line.template` or creates a transient Databricks job; afterwards it **reads the output table stats itself** and enforces `minimum.records` before writing bookkeeping ("Data already saved by Pramen-Py. Just loading the table and getting stats") [pr-pyjob]. The trust boundary is the metastore, not the Python process's claims.

### Failure modes it suffered (issues)

- Empty partition left after a JDBC failure mid write [pr-170]; the Parquet writer now has `writeAndCleanOnFailure` and deletes the partition directory on failure, but overwrite of a partition directory is still not atomic on object stores [pr-parquet].
- Bookkeeping DB connection loss: "If PostgreSQL connection is broken, Slick does not recover it, at least it has no retries" [pr-699].
- Lock takeover bug: "When a job is killed, Pramen fails to re-acquire the expired token lock when JDBC is used", because the release SQL only deleted rows owned by the current process; fixed in v1.14.9 with conditional deletes of still-expired tickets [pr-786][pr-rel].
- Lock semantics diverged per backend until a common base class was extracted [pr-599]; locking for Delta bookkeeping had to be added separately [pr-595].
- Hangs with `parallel.tasks > 1` "especially on big pipelines", not reproducible, still open [pr-302].
- A cluster of 2026 fixes about closing resources opened in other threads (v1.14.5 to v1.14.8) [pr-rel].
- `disable.count.query = true` silently changes ingestion behaviour (track.days forced to 0) [pr-539][pr-ingestion].
- Open wish: "Stateful data checks... number of records in the ingested snapshot today does not deviate too much from the one ingested for the previous day", "configuration-only" [pr-220].

### 2026 coverage vs what is still unique

EXTERNAL: Dagster partitions and backfills, Airflow 3 backfills, dbt microbatch incremental models and Spark Declarative Pipelines all cover "run for a date range" and "skip already materialised partitions". What remains unusual in Pramen: (a) source side count probing to detect retrospective changes without CDC, (b) make style upstream `jobFinished` comparison that re-triggers downstream for the same partition, (c) the typed run reason (`Late/Update/Rerun/New`) recorded per task, (d) `undercover` reruns that do not propagate, (e) offset commit repaired from written data, (f) the `_old` count pairs in the journal.

### Lessons for Ubunye

- **Recovery/resume**: adopt "partition is the unit of idempotency; bookkeeping is written last". A crash between write and bookkeeping simply causes a rerun of that partition. Add a two-phase offset table with `committed_at` and a repair step that recomputes offsets from the sink.
- **Backfill**: the four-set planner (backfill gaps, tracked window, late catch-up, new) is a compact, testable spec. Put it behind `ubunye plan` so the plan is visible before execution (Pramen's `--dry-run`).
- **Reconciliation**: record `input_record_count`, `output_record_count`, `appended_record_count` and the previous counts in the run receipt; treat source count drift as a first-class event.
- **Schema evolution**: store a schema per (dataset, partition) and emit a typed diff (`NewField/DeletedField/ChangedType`) into the receipt and OpenLineage run facets.
- **Lineage/impact**: `trigger.updates` edges are an impact graph; Ubunye can answer "which downstream partitions are stale" by comparing receipt timestamps or, better, content hashes.
- **Delivery semantics**: state the guarantee per connector as Pramen does ("AT LEAST ONCE") instead of implying exactly once.
- **Do not copy**: count equality as the only change signal (an update that keeps the count identical is invisible, and a count query per tracked day is expensive, which is why `disable.count.query` exists [pr-ingestion]); wall clock `jobFinished` comparison (clock skew, and it re-triggers on no-op reruns; Ubunye has content hashes and should compare those); a hand-written date DSL (use a small, sandboxed expression language or plain Python functions with tests); T+1 daily assumption baked into scheduling; per-backend lock implementations that drift [pr-599].

[pr-readme]: https://github.com/AbsaOSS/pramen/blob/main/README.md
[pr-readme-dates]: https://github.com/AbsaOSS/pramen/blob/main/README.md#dates
[pr-readme-infodate]: https://github.com/AbsaOSS/pramen/blob/main/README.md#output-information-date-expression
[pr-readme-expr]: https://github.com/AbsaOSS/pramen/blob/main/README.md#date-functions
[pr-readme-meta]: https://github.com/AbsaOSS/pramen/blob/main/README.md#metastore
[pr-readme-bk]: https://github.com/AbsaOSS/pramen/blob/main/README.md#bookkeeping
[pr-readme-incr]: https://github.com/AbsaOSS/pramen/blob/main/README.md#incremental-ingestion-experimental
[pr-readme-deps]: https://github.com/AbsaOSS/pramen/blob/main/README.md#dependencies
[pr-readme-par]: https://github.com/AbsaOSS/pramen/blob/main/README.md#parallelism
[pr-readme-strict]: https://github.com/AbsaOSS/pramen/blob/main/README.md#new-dependency-management-mode
[pr-readme-bulk]: https://github.com/AbsaOSS/pramen/blob/main/README.md#experimental-bulk-load-of-historical-data
[pr-readme-cli]: https://github.com/AbsaOSS/pramen/blob/main/README.md#command-line-arguments
[pr-readme-default]: https://github.com/AbsaOSS/pramen/blob/main/README.md#default-pipeline-run
[pr-readme-notif]: https://github.com/AbsaOSS/pramen/blob/main/README.md#pipeline-notifications
[pr-tree]: https://github.com/AbsaOSS/pramen/tree/main/pramen/core/src/main/scala/za/co/absa/pramen/core
[pr-source]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/Source.scala
[pr-transformer]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/Transformer.scala
[pr-sink]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/Sink.scala
[pr-sinkresult]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/SinkResult.scala
[pr-reason]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/Reason.scala
[pr-metatabledef]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/MetaTableDef.scala
[pr-partscheme]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/PartitionScheme.scala
[pr-runmode]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/RunMode.scala
[pr-runstatus]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/status/RunStatus.scala
[pr-runreason]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/status/TaskRunReason.scala
[pr-metadep]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/status/MetastoreDependency.scala
[pr-schemadiff]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/SchemaDifference.scala
[pr-fieldchange]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/FieldChange.scala
[pr-offsetvalue]: https://github.com/AbsaOSS/pramen/blob/main/pramen/api/src/main/scala/za/co/absa/pramen/api/offset/OffsetValue.scala
[pr-bkrec]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/bookkeeper/model/BookkeepingTable.scala
[pr-schemarec]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/bookkeeper/model/SchemaRecord.scala
[pr-offsettable]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/bookkeeper/model/OffsetTable.scala
[pr-metarec]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/bookkeeper/model/MetadataRecord.scala
[pr-journal]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/journal/model/JournalTable.scala
[pr-taskcompleted]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/journal/model/TaskCompleted.scala
[pr-exec]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/journal/model/ExecutionsTable.scala
[pr-locktable]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/lock/model/LockTicketTable.scala
[pr-lockbase]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/lock/TokenLockBase.scala
[pr-strat]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/runner/splitter/ScheduleStrategySourcing.scala
[pr-stratutils]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/runner/splitter/ScheduleStrategyUtils.scala
[pr-ingestion]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/pipeline/IngestionJob.scala
[pr-incr]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/pipeline/IncrementalIngestionJob.scala
[pr-jobbase]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/pipeline/JobBase.scala
[pr-pyjob]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/pipeline/PythonTransformationJob.scala
[pr-taskrunner]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/runner/task/TaskRunnerBase.scala
[pr-parquet]: https://github.com/AbsaOSS/pramen/blob/main/pramen/core/src/main/scala/za/co/absa/pramen/core/metastore/peristence/MetastorePersistenceParquet.scala
[pr-py-readme]: https://github.com/AbsaOSS/pramen/blob/main/pramen-py/README.md
[pr-rel]: https://github.com/AbsaOSS/pramen/releases
[pr-170]: https://github.com/AbsaOSS/pramen/issues/170
[pr-220]: https://github.com/AbsaOSS/pramen/issues/220
[pr-302]: https://github.com/AbsaOSS/pramen/issues/302
[pr-539]: https://github.com/AbsaOSS/pramen/issues/539
[pr-595]: https://github.com/AbsaOSS/pramen/issues/595
[pr-599]: https://github.com/AbsaOSS/pramen/pull/599
[pr-699]: https://github.com/AbsaOSS/pramen/issues/699
[pr-741]: https://github.com/AbsaOSS/pramen/pull/741
[pr-786]: https://github.com/AbsaOSS/pramen/issues/786
[pr-788]: https://github.com/AbsaOSS/pramen/issues/788

---

## 2. Cobrix (COBOL / EBCDIC data source)

### Purpose and production problem

"Lack of expertise in the Cobol ecosystem... The overwhelming majority (if not all) of tools to cope with this domain are proprietary... Mainframe data can only take part in data science activities through very expensive investments" [cb-readme]. Cobrix reads (and since 2026 writes) mainframe files described by COBOL copybooks as Spark DataFrames. It is the most starred repo in this family (170) and still releases monthly [cb-rel].

### Core abstraction and data model

- **Two modules, parser independent of Spark**: "The COBOL copybooks parser doesn't have a Spark dependency and can be reused for integrating into other data processing engines" [cb-readme]. `cobol-parser` holds an ANTLR grammar (`copybookLexer.g4`, `copybookParser.g4`), an AST (`Group`, `Primitive`, `CobolType`, `Usage`), a chain of AST transformers (`BinaryPropertiesAdder`, `DependencyMarker`, `SegmentRedefinesMarker`, `GroupFillersRemover`...), decoders and encoders, and ~40 EBCDIC code pages [cb-tree]. `spark-cobol` is a thin `DefaultSource`/`CobolRelation` adapter [cb-tree]. There is also a Spark-free `CobolProcessor` for in-place byte rewriting [cb-readme-proc].
- **Record formats** (`record_format`): `F`, `FB`, `V` (RDW), `VB` (BDW+RDW), `D`/`D2` (ASCII text), plus custom record extractors [cb-readme-opts].
- **Extractor contract**: `RawRecordExtractor extends Iterator[Array[Byte]] { def offset: Long; def canSplitHere: Boolean }` with a `RawRecordContext(startingRecordNumber, inputStream, headerStream, copybook, rdwDecoder, bdwDecoder, additionalInfo, options)` [cb-extractor][cb-context].
- **Sparse index splitting**: variable length records cannot be split blindly, so a first pass (`IndexGenerator.sparseIndexGenerator`) walks the file and emits `SparseIndexEntry(offsetFrom, offsetTo, fileId, recordIndex)` every N records or N MB, only at points where `canSplitHere` is true and, for hierarchical data, only at root segments [cb-index][cb-indexentry]. Entries are then distributed with HDFS block locality (`improve_locality`, `optimize_allocation`) [cb-readme-locality]. It validates that a custom extractor's `offset` points at the next record, and throws otherwise [cb-index].
- **Forensics columns**: `generate_record_id` (`File_Id`, `Record_Id`, `Record_Byte_Length`), `generate_record_bytes` (raw bytes), `with_input_file_name_col`, and since 2.9.8 `generate_corrupt_fields` producing `_corrupt_fields: array<struct<field_name, raw_value>>` with optional hex encoding [cb-readme-opts][cb-changelog][cb-corrupt]. `debug_layout_positions` prints the byte layout; `pedantic` fails on unknown options; `enable_self_checks` validates custom extractor index compatibility [cb-readme-debug].
- **Writer** (experimental, 2025 to 2026): `df.write.format("cobol")` for F, V, VB; `strict_schema` default true; explicit null policies (`write_null_strings_as_spaces`, `write_null_display_numbers_as_zeros`, `write_null_comp3_numbers_as_zeros`); `write_strict_redefines` [cb-readme-writer][cb-415].

### Testing

`spark-cobol/src/test` holds numbered integration specs (`Test1FixedLengthRecordsSpec`, `Test2RecordOffsetsSpec`, `Test5MultisegmentSpec`...) run against 177 committed sample files under `data/testN_data` with golden outputs in `data/testN_expected` [cb-tree][cb-test1]. Synthetic generators (`TestDataGen6TypeVariety`, `TestDataGen3Companies`) produce the 40 GB performance datasets documented in the README [cb-readme-perf].

### Design decisions and failure modes

- **Silent NULL on decode failure** is the dominant user pain. #813 (23 comments): `PIC 9(9)V99` decoded as `110323.11` on Scala 2.11 but `NULL` on 2.12 [cb-813]; #701 (34 comments, open) COMP-3 without sign nibble returns NULL [cb-701]; #372 (35 comments) blank vs null indistinguishable [cb-372]. The `_corrupt_fields` column (#723, 2026) is the forensic answer: keep the raw bytes of every field that failed [cb-changelog].
- **Index cache false hits**: cached VRL indexes were reused even when reader options changed; fixed by keying the cache with "read properties hash code" (#811), after which caching became default on [cb-811][cb-changelog].
- **Single-threaded index build** for 300 GB ASCII VRL files: "indexBuilder stage is running in single partition... taking more than 2 hours" [cb-543].
- **Race condition** when several threads or notebooks share a driver for fixed-length files (#714) [cb-714].
- **Platform permissions**: Unity Catalog volumes fail when the cluster creator differs from the caller; workaround is passing `copybook_contents` inline [cb-665].
- Copybook syntax variety drives many long threads (#96, 42 comments) [cb-96].

### 2026 coverage vs unique

EXTERNAL: commercial mainframe offload tools and some cloud migration services decode copybooks, but an open, Spark-free copybook parser with VRL split indexing and a writer remains rare. Unique: sparse index plus locality, `_corrupt_fields` forensics, copybook-driven writer.

### Lessons for Ubunye

- **Connectors / capability vocabulary**: Cobrix shows the capabilities a file connector must declare: splittable or not, needs index pass, supports push-down limit (`record_limit` is explicitly "a source-level global cap: df.limit(N) does not push down" [cb-readme-opts]), supports streaming, write support.
- **Reconciliation**: a decoded dataset must report "fields that failed to decode" as a count in the receipt; a NULL introduced by decoding is a data quality event, not a value. Ubunye's quarantine should keep raw bytes plus field name, like `_corrupt_fields`.
- **Caching and receipts**: #811 is a textbook argument for content hashing: any derived artefact (index, cache, plan) must be keyed by a hash of inputs and options. Ubunye already hashes receipts; extend that to caches.
- **Conformance kits**: golden sample files with expected outputs per numbered scenario are a cheap, durable conformance kit for format connectors.
- **Do not copy**: `pedantic=false` default (unknown options only logged) [cb-readme-debug]; the fixed width Scala version behaviour drift shows the need for cross-version golden tests before upgrade.

[cb-readme]: https://github.com/AbsaOSS/cobrix/blob/master/README.md
[cb-readme-opts]: https://github.com/AbsaOSS/cobrix/blob/master/README.md#summary-of-all-available-options
[cb-readme-debug]: https://github.com/AbsaOSS/cobrix/blob/master/README.md#debug-helper-options
[cb-readme-locality]: https://github.com/AbsaOSS/cobrix/blob/master/README.md#locality-optimization-for-variable-length-records-parsing
[cb-readme-writer]: https://github.com/AbsaOSS/cobrix/blob/master/README.md#ebcdic-writer
[cb-readme-proc]: https://github.com/AbsaOSS/cobrix/blob/master/README.md#ebcdic-processor
[cb-readme-perf]: https://github.com/AbsaOSS/cobrix/blob/master/README.md#performance-analysis
[cb-changelog]: https://github.com/AbsaOSS/cobrix/blob/master/README.md#changelog
[cb-tree]: https://github.com/AbsaOSS/cobrix/tree/master/cobol-parser/src/main/scala/za/co/absa/cobrix/cobol
[cb-index]: https://github.com/AbsaOSS/cobrix/blob/master/cobol-parser/src/main/scala/za/co/absa/cobrix/cobol/reader/index/IndexGenerator.scala
[cb-indexentry]: https://github.com/AbsaOSS/cobrix/blob/master/cobol-parser/src/main/scala/za/co/absa/cobrix/cobol/reader/index/entry/SparseIndexEntry.scala
[cb-extractor]: https://github.com/AbsaOSS/cobrix/blob/master/cobol-parser/src/main/scala/za/co/absa/cobrix/cobol/reader/extractors/raw/RawRecordExtractor.scala
[cb-context]: https://github.com/AbsaOSS/cobrix/blob/master/cobol-parser/src/main/scala/za/co/absa/cobrix/cobol/reader/extractors/raw/RawRecordContext.scala
[cb-corrupt]: https://github.com/AbsaOSS/cobrix/blob/master/cobol-parser/src/main/scala/za/co/absa/cobrix/cobol/reader/extractors/record/CorruptField.scala
[cb-test1]: https://github.com/AbsaOSS/cobrix/blob/master/spark-cobol/src/test/scala/za/co/absa/cobrix/spark/cobol/source/integration/Test1FixedLengthRecordsSpec.scala
[cb-rel]: https://github.com/AbsaOSS/cobrix/releases
[cb-96]: https://github.com/AbsaOSS/cobrix/issues/96
[cb-372]: https://github.com/AbsaOSS/cobrix/issues/372
[cb-415]: https://github.com/AbsaOSS/cobrix/issues/415
[cb-543]: https://github.com/AbsaOSS/cobrix/issues/543
[cb-665]: https://github.com/AbsaOSS/cobrix/issues/665
[cb-701]: https://github.com/AbsaOSS/cobrix/issues/701
[cb-714]: https://github.com/AbsaOSS/cobrix/issues/714
[cb-811]: https://github.com/AbsaOSS/cobrix/issues/811
[cb-813]: https://github.com/AbsaOSS/cobrix/issues/813

---

## 3. fixed-width

**Purpose**: a Spark data source for flat files "where each column has a fixed width... specified in a schema" [fw-readme]. **Abstraction**: widths live in Spark `StructField` metadata (`putLong("width", 10)`); options `trimValues` and `charset` [fw-readme]. Parse modes copy Spark CSV: `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`, but an invalid mode string silently falls back to permissive ("We default to permissive is the mode string is not valid") [fw-parsemodes]. **Failure suffered**: "'width' metadata is not recognized if integer" [fw-4]. **Maturity**: dormant since 2023, v0.2.0, Scala 2.11/2.12 only [fw-readme]. **2026**: largely subsumed by Cobrix `record_format=D` for ASCII and by plain `substring` projections. **Lesson for Ubunye**: schema-as-metadata is a neat way to keep layout next to types; the bad-record policy should be an explicit, validated enum (Ubunye expectations and quarantine already do this). **Do not copy**: silent fallback to the most permissive mode on a typo.

[fw-readme]: https://github.com/AbsaOSS/fixed-width/blob/master/README.md
[fw-parsemodes]: https://github.com/AbsaOSS/fixed-width/blob/master/src/main/scala/za/co/absa/fixedWidth/util/ParseModes.scala
[fw-4]: https://github.com/AbsaOSS/fixed-width/issues/4

---

## 4. ABRiS (Avro Bridge for Spark)

### Purpose and core model

Spark `from_avro`/`to_avro` expressions with Confluent Schema Registry, "all available naming strategies and schema evolution" [ab-readme]. Fluent config: `AbrisConfig.fromConfluentAvro.downloadReaderSchemaByLatestVersion.andTopicNameStrategy("topic123").usingSchemaRegistry(url)` [ab-readme-usage]. Subjects: `usingTopicNameStrategy(topic, isKey)` gives `topic-key`/`topic-value`; `usingRecordNameStrategy` gives the record full name; `usingTopicRecordNameStrategy` gives `topic-fullname`; non-RECORD schemas are rejected [ab-subject]. Coordinates are either `IdCoordinate(schemaId)` or `SubjectCoordinate(subject, version)` [ab-coord].

### Evolution handling

Decoding uses standard Avro resolution with a fixed **reader schema** and a per-record **writer schema**: the Confluent path reads the 4-byte schema id, downloads that writer schema once and caches a `GenericDatumReader(writerSchema, readerSchema)` per id per executor [ab-a2c]. The Spark output type is derived from the reader schema (`dataType = toSqlType(readerSchema)`) [ab-a2c]. Inference (from code, not documented): a reader schema resolved as "latest" at plan time stays fixed for the life of the query, so fields added in newer writer versions are dropped until restart. Mixed schemas in one topic (RecordName strategies) cannot be decoded into one DataFrame; "you must first sort them out to several dataframes" [ab-readme-multi]. Error handling is pluggable: `FailFast` default, `SpecificRecordExceptionHandler(default)`, `PermissiveRecordExceptionHandler` (null record plus a log warning) [ab-readme-err].

### Failure modes

- Registry flooding: "uses uncached call to get the latest version id... results in a huge amount of http requests to schema registry" [ab-105].
- Static, non thread-safe schema manager [ab-109].
- Dependency hell: `NoClassDefFoundError ... ConfigException` (23 comments) [ab-103]; v7.0.0-RC1 "is not compatible with Spark 3.5.x and previous" [ab-rel7].
- Enum and union mapping problems (#19, #125) [ab-19][ab-125].

### 2026 coverage and lessons

EXTERNAL: Spark 3.x+ ships `from_avro` and Databricks has registry-aware Avro functions, so ABRiS's unique value is shrinking to the Confluent wire format plus naming strategies. **Lessons for Ubunye**: a schema reference should be a typed coordinate (id or subject+version), never "latest" silently; record in the receipt which writer schema ids were seen and which reader schema was applied (that is a schema evolution audit for free); cache registry lookups with TTL; the error-handler trio maps neatly onto Ubunye's quarantine policy. **Do not copy**: `Permissive` handler that turns bad records into all-null rows without a counter; a null row is indistinguishable from real data downstream.

[ab-readme]: https://github.com/AbsaOSS/ABRiS/blob/master/README.md
[ab-readme-usage]: https://github.com/AbsaOSS/ABRiS/blob/master/README.md#usage
[ab-readme-err]: https://github.com/AbsaOSS/ABRiS/blob/master/README.md#de-serialisation-error-handling
[ab-readme-multi]: https://github.com/AbsaOSS/ABRiS/blob/master/README.md#multiple-schemas-in-one-topic
[ab-subject]: https://github.com/AbsaOSS/ABRiS/blob/master/src/main/scala/za/co/absa/abris/avro/registry/SchemaSubject.scala
[ab-coord]: https://github.com/AbsaOSS/ABRiS/blob/master/src/main/scala/za/co/absa/abris/avro/registry/SchemaCoordinate.scala
[ab-a2c]: https://github.com/AbsaOSS/ABRiS/blob/master/src/main/scala/za/co/absa/abris/avro/sql/AvroDataToCatalyst.scala
[ab-rel7]: https://github.com/AbsaOSS/ABRiS/releases/tag/v7.0.0-RC1
[ab-19]: https://github.com/AbsaOSS/ABRiS/issues/19
[ab-103]: https://github.com/AbsaOSS/ABRiS/issues/103
[ab-105]: https://github.com/AbsaOSS/ABRiS/issues/105
[ab-109]: https://github.com/AbsaOSS/ABRiS/issues/109
[ab-125]: https://github.com/AbsaOSS/ABRiS/issues/125

---

## 5. py2k (pandas to Kafka)

**Purpose**: "A high level Python to Kafka API with Schema Registry compatibility and automatic avro schema creation" from pandas DataFrames and Pydantic models [py-readme]. **Model**: `PandasToRecordsTransformer(df, record_name).from_pandas()` then `KafkaWriter(topic, schema_registry_config, producer_config).write(records)` [py-readme]; `KafkaRecord` builds an Avro record schema from the Pydantic JSON schema, splitting key and value fields [py-record]. **Failure suffered**: the schema was inferred from the first row, which "caused errors when null values occurred in the first row"; fixed by scanning for the first non-null value per column and raising if all are null [py-76]. The iterator path still takes the schema from the first element (`schema_from_iter` uses `islice(iterator, 1)`) [py-record]. **Maturity**: dormant since 2023. **Lesson for Ubunye**: schema inference from data is a trap; the LLM port or a config generator may propose a schema, but the pipeline must pin it in config and diff against it. **Do not copy**: inferring a contract from a sample.

[py-readme]: https://github.com/AbsaOSS/py2k/blob/main/README.md
[py-record]: https://github.com/AbsaOSS/py2k/blob/main/py2k/record.py
[py-76]: https://github.com/AbsaOSS/py2k/pull/76

---

## 6. KafkaCase

**Purpose**: "Adapter for communicating from Scala with Kafka via case classes"; JSON via circe, reader is an `Iterator`, writer is fire and forget with optional `flush`/`WriteSync` [kc-readme]. **What makes it interesting** is the `models` module: "case classes intended to be used by the team as contractual classes" [kc-readme], namely `EdlaChange(event_id, tenant_id, source_app, source_app_version, environment, timestamp_event, catalog_id, operation ∈ {overwrite, append, archive, delete}, location, format, formatOptions)` and `Run(event_id, job_ref, tenant_id, ..., jobs: Seq[Job(catalog_id, status ∈ {succeeded, failed, killed, skipped}, ...)])` [kc-edla][kc-run]. These are the same contracts as EventGate's `dlchange` and `runs` topics (section 13), i.e. a bank-wide "data lake changed" and "pipeline ran" event. **Design flaw**: the enum decoders use a partial match (`case "overwrite" => ...` with no default), so an unknown value throws `MatchError` instead of a decode failure [kc-edla]; `ReaderNeverEnding.hasNext` is always `true` [kc-reader]. **Lesson for Ubunye**: a tiny "dataset changed" event (catalog id, operation, location, format) is the minimal integration point enterprises already consume; Ubunye can emit it next to OpenLineage. **Do not copy**: closed enums without an `unknown` branch in a cross-team contract.

[kc-readme]: https://github.com/AbsaOSS/KafkaCase/blob/master/README.md
[kc-edla]: https://github.com/AbsaOSS/KafkaCase/blob/master/models/src/main/scala/za/co/absa/kafkacase/models/topics/EdlaChange.scala
[kc-run]: https://github.com/AbsaOSS/KafkaCase/blob/master/models/src/main/scala/za/co/absa/kafkacase/models/topics/Run.scala
[kc-reader]: https://github.com/AbsaOSS/KafkaCase/blob/master/reader/src/main/scala/za/co/absa/kafkacase/reader/ReaderNeverEnding.scala

---

## 7. Hyperdrive (streaming ingestion)

### Purpose and component contract

"A configurable and scalable ingestion platform... with exactly-once fault-tolerance semantics by using Apache Spark Structured Streaming", because "exactly-once fault-tolerance in streaming processing is an intricate problem and cannot be solved with the same strategies that exist for batch processing" [hd-readme]. The contract is three abstract classes: `StreamReader.read(spark): DataFrame`, `StreamTransformer.transform(df): DataFrame`, `StreamWriter.write(df): StreamingQuery` [hd-reader][hd-transformer][hd-writer]. `SparkIngestor.ingest` folds transformers over the reader output and then either `processAllAvailable` or `awaitTermination` [hd-ingestor]. Config is flat Apache Commons properties: `component.reader`, `component.transformer.id.{order}`, `component.transformer.class.{id}`, `component.writer`, with `${...}` interpolation [hd-readme-conf]. Secrets: `secretsprovider.secrets.<id>.options.secretname` resolved into `${secretsprovider.secrets.<id>.secretvalue}` [hd-readme-secrets]. Components share state through a global mutable `HyperdriveContext` map [hd-context].

### Exactly once and idempotency

- The ingestor distinguishes two failure classes in its exceptions: `IngestionStartException` ("NOT STARTED") versus `IngestionException` ("PROBABLY FAILED INGESTION... the query has been started, which might lead to duplicate data... a possible course of action is to replay this ingestion and overwrite the destination") [hd-ingestor].
- **Kafka to Kafka dedup on retry** (`DeduplicateKafkaSinkTransformer`): if the checkpoint offset log is ahead of the commit log, the previous micro-batch was partial. It then reads the source records of that batch, reads at least that many latest records per partition from the sink topic, intersects user-defined composite ids (`source.id.columns` vs `destination.id.columns`, e.g. `offset, partition` vs `value.src_offset, value.src_partition`) and filters them out [hd-dedup][hd-readme-dedup]. Stated limits: one source, one destination, a single writer, and "no records must have been written to the destination topic after the partial run" [hd-readme-dedup].
- **Parquet sinks rely on `_spark_metadata`**; readers that bypass it see duplicates from incomplete micro-batches, so tools were requested to delete orphaned files [hd-226] and to rewrite absolute paths after moving folders to S3 [hd-235]; `writer.parquet.metadata.check` compares files on disk to the log, "very expensive" for big tables [hd-readme-parquet][hd-metalog].
- **CDC writers**: `DeltaCDCToSnapshotWriter`, `DeltaCDCToSCD2Writer`, `HudiCDCToSCD2Writer`, configured by `key.column`, `operation.column`, `operation.deleted.values`, `precombineColumns` and a custom order for operation codes (example `ENTTYP` with `PT,FI,RR,UB,UP,DL,FD`, i.e. IBM i journal codes) [hd-readme-cdc].
- Trigger semantics are documented as a matrix (Once vs ProcessingTime by termination method), including the warning that a timeout on `Once` "won't be completed and no data will be committed... we don't recommend it" [hd-readme-trig].

### Failure modes

OOM after several micro-batches, suspected leak [hd-231]; nullability loss from Avro to Catalyst [hd-137]; classpath errors [hd-61]; "Delete destination directory when exception occurred during ingestion" [hd-50]. Code smell: `MetadataLogUtil.getFileSystemFiles` constructs `Failure(new IllegalStateException("Parquet file paths on filesystem are not unique"))` but never returns it, so the check is a no-op [hd-metalog].

### 2026 coverage and lessons

EXTERNAL: Kafka transactions (EOS), Delta/Iceberg MERGE, Change Data Feed and Spark Declarative Pipelines' `AUTO CDC` now cover most of what the CDC writers and dedup transformer do. **Lessons for Ubunye**: classify failures as "not started" vs "started, outcome unknown" in the receipt; a partial-batch detector (offset log vs commit log) is the right primitive for recovery; declare delivery semantics per sink; CDC-to-SCD2 is a configurable pattern worth a plugin with precombine ordering. **Do not copy**: global mutable context; dedup that depends on reading the tail of the sink topic (fragile with multiple writers); flat class-name based config.

[hd-readme]: https://github.com/AbsaOSS/hyperdrive/blob/develop/README.md
[hd-readme-conf]: https://github.com/AbsaOSS/hyperdrive/blob/develop/README.md#pipeline-settings
[hd-readme-dedup]: https://github.com/AbsaOSS/hyperdrive/blob/develop/README.md#deduplicatekafkasinktransformer
[hd-readme-parquet]: https://github.com/AbsaOSS/hyperdrive/blob/develop/README.md#parquetstreamwriter
[hd-readme-cdc]: https://github.com/AbsaOSS/hyperdrive/blob/develop/README.md#deltacdctoscd2writer
[hd-readme-trig]: https://github.com/AbsaOSS/hyperdrive/blob/develop/README.md#behavior-of-triggers
[hd-readme-secrets]: https://github.com/AbsaOSS/hyperdrive/blob/develop/README.md#secrets-providers
[hd-reader]: https://github.com/AbsaOSS/hyperdrive/blob/develop/api/src/main/scala/za/co/absa/hyperdrive/ingestor/api/reader/StreamReader.scala
[hd-transformer]: https://github.com/AbsaOSS/hyperdrive/blob/develop/api/src/main/scala/za/co/absa/hyperdrive/ingestor/api/transformer/StreamTransformer.scala
[hd-writer]: https://github.com/AbsaOSS/hyperdrive/blob/develop/api/src/main/scala/za/co/absa/hyperdrive/ingestor/api/writer/StreamWriter.scala
[hd-context]: https://github.com/AbsaOSS/hyperdrive/blob/develop/api/src/main/scala/za/co/absa/hyperdrive/ingestor/api/context/HyperdriveContext.scala
[hd-ingestor]: https://github.com/AbsaOSS/hyperdrive/blob/develop/driver/src/main/scala/za/co/absa/hyperdrive/driver/SparkIngestor.scala
[hd-dedup]: https://github.com/AbsaOSS/hyperdrive/blob/develop/ingestor-default/src/main/scala/za/co/absa/hyperdrive/ingestor/implementation/transformer/deduplicate/kafka/DeduplicateKafkaSinkTransformer.scala
[hd-metalog]: https://github.com/AbsaOSS/hyperdrive/blob/develop/ingestor-default/src/main/scala/za/co/absa/hyperdrive/ingestor/implementation/utils/MetadataLogUtil.scala
[hd-rel]: https://github.com/AbsaOSS/hyperdrive/releases
[hd-50]: https://github.com/AbsaOSS/hyperdrive/issues/50
[hd-61]: https://github.com/AbsaOSS/hyperdrive/issues/61
[hd-137]: https://github.com/AbsaOSS/hyperdrive/issues/137
[hd-226]: https://github.com/AbsaOSS/hyperdrive/issues/226
[hd-231]: https://github.com/AbsaOSS/hyperdrive/issues/231
[hd-235]: https://github.com/AbsaOSS/hyperdrive/issues/235

---

## 8. hyperdrive-trigger (workflow manager)

### Model

A Scala + Angular workflow manager [ht-tree]. `Workflow(name, isActive, project, version, schedulerInstanceId)` uses optimistic locking (`OptimisticLockingEntity`) [ht-workflow]. Each workflow has one `Sensor` whose properties are a sealed union: `KafkaSensorProperties(topic, servers, matchProperties)`, `AbsaKafkaSensorProperties(topic, servers, ingestionToken)`, `RecurringSensorProperties`, `TimeSensorProperties(cronExpression)` [ht-sensor]. A `DagDefinition` is an ordered list of `JobDefinition`s; a run is a `DagInstance(status, triggeredBy, workflowId, started, finished)` with `JobInstance(jobName, jobParameters, jobStatus, diagnostics, executorJobId, applicationId, stepId, order, dagInstanceId)` [ht-dagdef][ht-daginst][ht-jobinst]. The "DAG" is therefore a linear chain ordered by `order`, with a `FailedPreviousJob` status for downstream jobs [ht-jobstatus]. Sensor events are persisted: `Event(sensorEventId, sensorId, payload, dagInstanceId)` [ht-event].

Job status vocabulary: `InQueue, Submitting, Running, Lost, Succeeded, Failed, Killed, SubmissionTimeout, InvalidExecutor, FailedPreviousJob, Skipped, NoData`, each tagged `isFinalStatus/isFailed/isRunning` [ht-jobstatus].

### Notable mechanisms

- **Skip when nothing new**: before submitting a Hyperdrive job, `HyperdriveOffsetService.isNewJobInstanceRequired` compares the Spark checkpoint's latest committed offsets with Kafka end offsets; equal means the job is recorded as `NoData` and not submitted [ht-exec][ht-offset]. The same service computes "messages to ingest" (lag) and handles checkpoint offsets that are ahead of the topic end or behind the beginning (retention) [ht-offset][ht-791].
- **Reattach after restart**: if a `JobInstance` already has an `executorJobId`, the executor only polls status instead of resubmitting [ht-exec].
- **Recurring sensor with a rate limiter**: fires only if no DAG instance is running and fewer than `maxJobsPerDuration` were created in the window [ht-recurring].
- **HA scheduling**: multiple scheduler instances heartbeat and a `WorkflowBalancer` partitions workflows across live instances [ht-balancer].

### Failure modes and lessons

Open memory leak investigation [ht-673]; notifications not sent [ht-699]; property curly brace loss [ht-677]. EXTERNAL: Airflow 3 asset-driven scheduling and Dagster sensors now cover this. **Lessons for Ubunye**: (a) "is there new input?" should be a cheap pre-check recorded as `NoData`, not a failed or empty run; (b) persist the executor job id before polling so a restarted controller reattaches rather than double submits; (c) status enums should carry `isFinal/isFailed` flags so consumers never hard-code lists. **Do not copy**: building a bespoke scheduler UI; Ubunye's deploy/export to Airflow is the right call.

[ht-tree]: https://github.com/AbsaOSS/hyperdrive-trigger/tree/develop/src/main/scala/za/co/absa/hyperdrive/trigger
[ht-workflow]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/models/Workflow.scala
[ht-sensor]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/models/SensorProperties.scala
[ht-dagdef]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/models/DagDefinition.scala
[ht-daginst]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/models/DagInstance.scala
[ht-jobinst]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/models/JobInstance.scala
[ht-event]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/models/Event.scala
[ht-jobstatus]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/models/enums/JobStatuses.scala
[ht-exec]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/scheduler/executors/spark/HyperdriveExecutor.scala
[ht-offset]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/api/rest/services/HyperdriveOffsetService.scala
[ht-recurring]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/scheduler/sensors/recurring/RecurringSensor.scala
[ht-balancer]: https://github.com/AbsaOSS/hyperdrive-trigger/blob/develop/src/main/scala/za/co/absa/hyperdrive/trigger/scheduler/cluster/WorkflowBalancer.scala
[ht-rel]: https://github.com/AbsaOSS/hyperdrive-trigger/releases
[ht-673]: https://github.com/AbsaOSS/hyperdrive-trigger/issues/673
[ht-677]: https://github.com/AbsaOSS/hyperdrive-trigger/issues/677
[ht-699]: https://github.com/AbsaOSS/hyperdrive-trigger/issues/699
[ht-791]: https://github.com/AbsaOSS/hyperdrive-trigger/pull/791

---

## 9. hyperdrive-hive-jobs

A single job, `RepairHiveTable`, that runs `MSCK REPAIR TABLE <table>` given `hive.metastore.uris` and `table` key=value args [hhj-readme][hhj-src]. The table name is string-interpolated into SQL without validation [hhj-src]. It exists because streaming writes add partitions the metastore does not know about. Pramen hit the same problem from the other side and added `neverRepairPartitions` to skip `MSCK REPAIR` and partition-level schema replacement in 2026 [pr-rel]. **Lesson for Ubunye**: catalogue registration is a post-write step with its own receipt entry (which partitions were registered); prefer explicit `ADD PARTITION` or table formats that do not need repair. **Do not copy**: interpolating identifiers into SQL.

[hhj-readme]: https://github.com/AbsaOSS/hyperdrive-hive-jobs/blob/develop/README.md
[hhj-src]: https://github.com/AbsaOSS/hyperdrive-hive-jobs/blob/develop/src/main/scala/za/co/absa/hyperdrive/hive/jobs/RepairHiveTable.scala

---

## 10. Jdbc2S (JDBC streaming source)

**Purpose**: stream from RDBMS without CDC for legacy cases, "e.g. data ingested from mainframes into databases in an hourly fashion" [j2s-readme]. **Model**: a DataSource V1 streaming source that wraps a batch JDBC RDD; options `offset.field`, `offset.field.date.format`, optional `offset.start` [j2s-readme]. Queries are `offsetField >= start AND <= end` for the first batch and `> start AND <= end` afterwards [j2s-readme]. **Design caveats stated by the authors**: "updates and deletions will only be identified if they also advance the offset field"; offsets are compared with `!=`, not `<`; it lives in Spark's package to call the package-private `internalCreateDataFrame`; on restart from checkpoint it returns an empty batch once to avoid reprocessing [j2s-readme][j2s-15]. **Failure modes (inferred)**: a high-watermark source misses rows that commit late with an offset below the watermark (long transactions, clock skew); this is the same class of risk as Pramen's incremental offsets. **Maturity**: dormant since 2024; PySpark usage issue open [j2s-23]. **Lesson for Ubunye**: an incremental connector must declare its change-capture capability (`append_only_watermark` vs `cdc` vs `full_snapshot`) and the receipt should carry min/max watermark; add an optional lookback overlap plus dedup key to catch late commits. **Do not copy**: living inside another project's private packages.

[j2s-readme]: https://github.com/AbsaOSS/Jdbc2S/blob/master/README.md
[j2s-15]: https://github.com/AbsaOSS/Jdbc2S/pull/15
[j2s-23]: https://github.com/AbsaOSS/Jdbc2S/issues/23

---

## 11 and 12. spring-cloud-stream-binder-jms and spring-cloud-stream-binder-ibm-mq

Both are 2017 forks of `spring-attic` projects [scs-jms-meta][scs-mq-readme]. The JMS binder notes that JMS queues and topics do not map onto Spring Cloud Stream's "persistent publish-subscribe with consumer groups", so provisioning is delegated to a broker-specific `QueueProvisioner` SPI [scs-jms-readme]. The IBM MQ module implements it with PCF commands: a Topic per destination, and per consumer group a Subscription delivering into a Queue named after the group [scs-mq-readme]. **The transferable idea is the conformance kit**: `EndToEndIntegrationTests` in `spring-cloud-stream-binder-jms-common-test-support` is an abstract test class a new provider extends to prove compliance, with cases such as `scs_whenMultipleConsumerGroups_eachGroupGetsAllMessages`, `scs_whenMultipleMembersOfSameConsumerGroup_groupOnlySeesEachMessageOnce`, `scs_whenRequiredGroupsAreSpecified_messagesArePersistedWithoutConsumers`, `scs_whenMessageIsSentToDLQ_stackTraceAddedToHeaders`, `scs_whenConsumerFails_retriesTheSpecifiedAmountOfTimes`, `scs_maxAttempts1_preventsRetry`, `scs_whenAPartitioningKeyIsConfigured_messagesAreRoutedToTheRelevantPartition` [scs-jms-tck]. **Lesson for Ubunye**: ship a connector conformance kit as an abstract pytest suite: a new connector subclasses it and inherits tests for schema round-trip, idempotent rerun, partition overwrite, watermark resume and quarantine. **Do not copy**: proprietary jars that must be installed by hand to compile [scs-mq-readme].

[scs-jms-readme]: https://github.com/AbsaOSS/spring-cloud-stream-binder-jms/blob/master/README.md
[scs-jms-meta]: https://github.com/AbsaOSS/spring-cloud-stream-binder-jms
[scs-jms-tck]: https://github.com/AbsaOSS/spring-cloud-stream-binder-jms/blob/master/spring-cloud-stream-binder-jms-common-test-support/src/main/java/org/springframework/cloud/stream/binder/test/integration/EndToEndIntegrationTests.java
[scs-mq-readme]: https://github.com/AbsaOSS/spring-cloud-stream-binder-ibm-mq/blob/master/README.md

---

## 13. EventGate (schema-governed event ingestion)

### Purpose and design

"Python AWS Lambda that exposes a simple HTTP API... for validating and forwarding well-defined JSON messages to multiple backends (Kafka, EventBridge, Postgres). Designed for centralized, schema-governed event ingestion with pluggable writers"; status "Internal prototype / early version" [eg-readme]. Flow: JWT (RS256, keys fetched remotely) then per-topic JSON Schema validation then fan-out to all writers; `202` only if all writers succeed, otherwise `500` "with per-writer error list" [eg-readme]. Access control is a JSON map per topic and service account, optionally with **field-level constraints** (`source_app`, `environment`, `tenant_id` regex) so a producer can only publish events about itself [eg-readme]. Schemas and access rules can live on S3 "to allow runtime evolution without code changes", with a `/terminate` endpoint that forces a cold start to reload config [eg-readme]. Observability: structured Powertools logs with a validated `X-Correlation-ID` echoed back, exactly one ERROR line per failed request, `TRACE` payload logging with key redaction and a size cap [eg-readme].

### The topic contracts are a bank-wide pipeline telemetry model

- `dlchange`: `event_id, tenant_id, source_app, source_app_version, environment, timestamp_event, country, catalog_id, operation ∈ {overwrite, append, archive, delete}, location, format, format_options, additional_info` [eg-dlchange].
- `runs`: finished runs with nested jobs per `catalog_id` [eg-ddl].
- `status_change` (ADR 000): lifecycle events `JobCreatedEvent, JobStartedEvent, JobCreatedAndStartedEvent, JobUpdatedEvent, JobFinishedEvent`, with `job_id, parent_job_id, job_group_id, initial_job_id, attempt_number, platform, platform_metadata, input_arguments, definition_id, status_type, status_subtype (e.g. NO_DATA), status_detail`. Retries are siblings linked by `initial_job_id`; "many fields are intentionally optional... progressive disclosure over time" [eg-adr0]. The ADR's worked example is literally "IngestApp Ingestion > Pramen Glue Job > Land / Standardize / Publish to Hive / Add control metrics" [eg-adr0]. Unknown country codes "should be a warning, not an error" [eg-adr0].
- Postgres keeps an aggregated job view with `ON CONFLICT (job_id) DO UPDATE` [eg-inserts], and Kafka messages for `status_change` are keyed by `job_id` [eg-keys].

### Failure modes it has or invites

- Fan-out is not atomic: a partial failure returns 500 after some writers already succeeded; a client retry will re-deliver to those writers. The `runs` and `dlchange` tables have no primary key or unique constraint on `event_id` [eg-ddl], so replays duplicate (inferred from DDL; no outbox or dedup found in the tree [eg-tree]).
- Postgres connection reuse (#131) and token rotation with "previous public token" (#83) were real operational issues [eg-131][eg-83].

### 2026 coverage and lessons

EXTERNAL: OpenLineage already standardises run start/complete/fail events with parent run facets, and CloudEvents standardises envelopes; EventGate's status model is essentially a bank-specific OpenLineage with platform metadata. **Lessons for Ubunye**: (a) Ubunye's receipt plus OpenLineage emission is exactly what Absa built EventGate to collect; offering an adapter that maps a Ubunye receipt to `status_change` and `dlchange` shapes would make Ubunye drop-in for this kind of enterprise; (b) model retries as siblings with `initial_job_id` and `attempt_number`, and nesting with `parent_job_id`/`job_group_id`; (c) typed terminal status with a subtype (`FAILED/NO_DATA`); (d) field-level producer authorisation is a good security pattern for the MCP server (an agent may only emit events about its own pipeline). **Do not copy**: synchronous multi-sink fan-out without an outbox or idempotency key.

[eg-readme]: https://github.com/AbsaOSS/EventGate/blob/master/README.md
[eg-tree]: https://github.com/AbsaOSS/EventGate/tree/master/src
[eg-adr0]: https://github.com/AbsaOSS/EventGate/blob/master/adr/000-status-change/000-status-change.md
[eg-dlchange]: https://github.com/AbsaOSS/EventGate/blob/master/conf/topic_schemas/dlchange.json
[eg-ddl]: https://github.com/AbsaOSS/EventGate/blob/master/database/migrations/V1.4.0.2__initial_schema.ddl
[eg-inserts]: https://github.com/AbsaOSS/EventGate/blob/master/src/writers/sql/inserts.sql
[eg-keys]: https://github.com/AbsaOSS/EventGate/blob/master/conf/topic_keys.json
[eg-rel]: https://github.com/AbsaOSS/EventGate/releases
[eg-83]: https://github.com/AbsaOSS/EventGate/pull/83
[eg-131]: https://github.com/AbsaOSS/EventGate/issues/131

---

## 14. Cross-repo synthesis

### (1) Problem matrix

| Problem | Evidence | AbsaOSS repo | Ubunye relevance | Leaning |
|---|---|---|---|---|
| Resume after crash mid-run | [pr-taskrunner] bookkeeping written after save; [pr-incr] uncommitted offsets repaired from data; [pr-readme-bulk] bulk resume | pramen | Direct gap (recovery/resume) | **Build** (partition ledger + two-phase offsets) |
| Backfill and gap filling over date ranges | [pr-strat][pr-stratutils][pr-runmode] | pramen | Direct gap (partition/backfill) | **Build** planner; **Integrate** with Airflow backfill on export |
| Late data and retrospective source changes | [pr-ingestion] count probe; [pr-readme-default] | pramen | Gap (reconciliation) | **Build** as optional probe, prefer hash/watermark |
| Downstream re-trigger on upstream change | [pr-jobbase] `getOutdatedTables` | pramen | Gap (lineage impact) | **Build** using receipt hashes, not timestamps |
| Input vs output record reconciliation | [pr-journal] counts and `_old` counts; [pr-sinkresult]; [pr-pyjob] | pramen | Direct gap | **Build** into receipt |
| Minimum records / insufficient data | [pr-runstatus] `InsufficientData`; [pr-220] | pramen | Expectations already exist | **Build** small extension |
| Schema drift detection per partition | [pr-taskrunner] `handleSchemaChange`; [pr-fieldchange] | pramen | Direct gap (schema diff) | **Build** |
| Schema registry evolution, reader vs writer schema | [ab-a2c][ab-subject][ab-105] | ABRiS | Gap (compatibility) | **Integrate** registry client; record ids in receipt |
| Schema inferred from sample data | [py-76][py-record] | py2k | Anti-pattern | **Ignore** (pin schemas) |
| Silent NULL on decode failure | [cb-813][cb-701][cb-372] | cobrix | Reconciliation / quality | **Build** decode-failure counters + raw capture |
| Corrupt record forensics | [cb-changelog] `_corrupt_fields`; `Record_Id`, `Record_Bytes` | cobrix | Quarantine enrichment | **Build** |
| Splitting unsplittable files | [cb-index][cb-readme-locality][cb-543] | cobrix | Performance, connectors | **Plugin** (Cobrix as Spark connector) |
| Mainframe EBCDIC read/write | [cb-readme][cb-readme-writer] | cobrix | Connector | **Plugin** |
| Cache keyed by options | [cb-811] | cobrix | Receipts/caches | **Build** (hash all cache keys) |
| Concurrent writers to one partition | [pr-lockbase][pr-786][pr-599] | pramen | Gap (safety) | **Build** lease lock per (dataset, partition) |
| Partial micro-batch duplicates | [hd-dedup][hd-226] | hyperdrive | Delivery semantics | **Integrate** (Kafka EOS, table MERGE) |
| CDC to SCD2 / snapshot | [hd-readme-cdc] | hyperdrive | Common enterprise pattern | **Plugin** |
| Skip run when no new input | [ht-offset][ht-exec] | hyperdrive-trigger | Cost, receipts | **Build** pre-check, status `NoData` |
| Reattach to running job after controller restart | [ht-exec] `executorJobId` | hyperdrive-trigger | Recovery | **Build** in deploy adapters |
| Watermark JDBC incremental | [j2s-readme] | Jdbc2S | Connectors | **Build** capability flags + lookback |
| Partition registration in catalogue | [hhj-src][pr-rel] | hyperdrive-hive-jobs, pramen | Catalogue | **Integrate** (Unity/Glue/HMS) |
| Connector conformance | [scs-jms-tck] | spring binder jms | Direct gap (conformance kits) | **Build** abstract pytest suite |
| Pipeline status and dataset change events | [eg-adr0][eg-dlchange][kc-edla] | EventGate, KafkaCase | Lineage / observability | **Integrate** (adapter from receipt) |
| Producer field-level authorisation | [eg-readme] access.json | EventGate | Security for MCP | **Build** (small) |
| Secrets referenced in config | [hd-readme-secrets] | hyperdrive | Already has `secret://` | **Ignore** (covered) |

### (2) Top 10 transferable ideas, ranked

1. **Partition ledger with "bookkeeping last" semantics** (pramen `bookkeeping` table, [pr-bkrec][pr-taskrunner]). One row per (dataset, partition) written only after a successful write gives resume, `fill_gaps` and "already ran" for free. It is the foundation for Ubunye's missing recovery, backfill and reconciliation, and it fits naturally beside the content-hashed receipt (store the receipt hash in the row).
2. **Two-phase offsets repaired from the sink** ([pr-incr][pr-offsettable]). `start` row with null `committed_at`, write, recompute min/max from what was actually written, commit; on restart, repair or roll back from the data. It turns crashes into a deterministic repair instead of guesswork, and it is portable across Spark and pandas.
3. **Typed run reason and typed status** ([pr-runreason][pr-runstatus][ht-jobstatus][eg-adr0]). `New/Late/Update/Rerun/OnRequest` and `Succeeded/NoData/InsufficientData/FailedDependencies/Skipped` with `isFinal/isFailed` flags. Cheap to add to the receipt and OpenLineage facets, and it makes backfill behaviour explainable.
4. **Four-set run planner with a dry run** ([pr-strat][pr-readme-cli]). Backfill gaps plus tracked window plus late catch-up plus new, deduplicated and sorted, exposed through `plan --dry-run`. A clear, testable spec for Ubunye's partition/backfill feature, and exportable as Airflow backfill ranges.
5. **Per-partition schema snapshots with typed diffs** ([pr-taskrunner][pr-fieldchange]). Store schema JSON per partition, emit `NewField/DeletedField/ChangedType` in the receipt and as an OpenLineage schema facet; later add compatibility rules (backward/forward) borrowed from registry practice ([ab-a2c]).
6. **Reconciliation counts in every receipt, including "old" counts on rerun** ([pr-journal][pr-sinkresult][pr-pyjob]). Input count, output count, appended count, quarantined count, previous output count. Also count decode failures ([cb-813]). The framework, not user code, must measure the output (pramen re-reads pramen-py output).
7. **Connector conformance kit as an abstract test class** ([scs-jms-tck]) plus golden sample files ([cb-test1]). Directly fills the "conformance kits beyond backends" gap; every connector inherits the same delivery, rerun and schema tests.
8. **Corrupt record forensics** ([cb-changelog][cb-corrupt]). Quarantine rows keep field name, raw bytes (or hex), file id and record id. Makes Ubunye's quarantine actionable for audits.
9. **Lease lock per (dataset, partition) with heartbeat renewal and expired-ticket takeover** ([pr-lockbase][pr-786]). Prevents two runs, or an agent and a human, writing the same partition. Learn from the bug: takeover must be a conditional delete of still-expired tickets.
10. **Status-change event shape for nested jobs and retries** ([eg-adr0]). `parent_job_id`, `job_group_id`, `initial_job_id`, `attempt_number`, `status_subtype`. Map Ubunye receipts onto it (and onto OpenLineage parent facets) so enterprises like Absa can ingest Ubunye runs without custom code.

Honourable mentions: skip-if-no-new-input pre-check recorded as `NoData` ([ht-offset]); "not started" vs "started, outcome unknown" failure classes ([hd-ingestor]); `undercover` reruns that do not propagate downstream ([pr-readme-cli]); options-hash cache keys ([cb-811]).

### (3) Anti-patterns observed

- **Silent degradation to NULL or to the most permissive mode**: Cobrix decode failures become NULL [cb-813][cb-701]; fixed-width falls back to `PERMISSIVE` on an invalid mode [fw-parsemodes]; ABRiS permissive handler emits all-null rows [ab-readme-err]; Cobrix `pedantic=false` only logs unknown options [cb-readme-debug]. Ubunye rule: every degradation increments a counter in the receipt, and unknown config keys fail.
- **Schema inferred from data samples** [py-76][py-record]. Pin schemas in config.
- **Record count equality as the only change signal** [pr-ingestion]. Misses updates that keep counts equal, and the count probes are costly enough to need an off switch [pr-539].
- **Wall-clock timestamp comparison for staleness** [pr-jobbase]. Sensitive to clock skew and no-op reruns; compare content hashes instead.
- **Non-atomic multi-sink fan-out without idempotency keys** [eg-readme][eg-ddl]. Use an outbox or require `event_id` uniqueness per sink.
- **Closed enums in cross-team contracts with partial decoders** [kc-edla]. Always include an `unknown` branch; EventGate's own ADR says unknown country codes should warn, not fail [eg-adr0].
- **Global mutable context shared by components** [hd-context]; **static, non thread-safe clients** [ab-109].
- **Swallowed failures in code**: `Failure(...)` constructed and discarded in `MetadataLogUtil` [hd-metalog].
- **Per-backend reimplementation of the same protocol** (locks drifted until unified) [pr-599][pr-786].
- **Identifiers interpolated into SQL** [hhj-src].
- **Reaching into another framework's private packages** [j2s-readme]; **manual install of proprietary jars to build** [scs-mq-readme].
- **Retry-by-rereading-the-sink dedup that assumes a single writer** [hd-readme-dedup].

### What a 2026 stack now covers vs what remains distinctive (summary)

EXTERNAL, not re-verified in this pass: table formats (Delta, Iceberg) give atomic partition overwrite and MERGE, which removes most of Pramen's `writeAndCleanOnFailure` and Hyperdrive's `_spark_metadata` pain; Kafka transactions give exactly-once for Kafka to Kafka; orchestrators (Airflow 3, Dagster) give partitioned backfills; OpenLineage gives run events and schema facets; registries give compatibility checks. What these do not give out of the box, and what AbsaOSS built by hand, is the **per-partition operational ledger that ties together counts, schemas, offsets, run reasons and locks in one place and drives automatic late-data and retrospective handling**. That ledger, implemented portably and anchored on content hashes rather than timestamps, is the single most valuable thing Ubunye can take from this family.

UNVERIFIED items: Pramen hang root cause [pr-302] (open, unreproduced); the exact ABRiS behaviour when a newer writer schema adds fields mid-query (inferred from code, not tested); whether EventGate has any dedup outside the files inspected (none found in `src/`).
