# AbsaOSS family A: lineage, control totals, conformance, comparison

Research date: 2026-09-25. Read-only pass over 15 AbsaOSS repositories using the authenticated GitHub API (source trees, code, DDL, config, docs branches, releases, top issues by comments and reactions). Every claim links to its source. Items I could not confirm from a primary source are marked UNVERIFIED. Statements marked "Analysis:" are my own reading of the code, not something the maintainers said.

Target reader: the Ubunye Engine maintainers deciding what goes into the next version. Ubunye today has a content-hashed run receipt, OpenLineage 2-0-2 and OpenTelemetry emission, output expectations with quarantine, `secret://` refs, Airflow and Spark Declarative Pipelines export, an LLM port with replay and budget, and an MCP server. It lacks recovery/resume, partition/backfill, input-vs-output reconciliation, schema diff/compatibility, semantic mapping, lineage impact queries, a connector capability vocabulary, and conformance kits beyond backends.

## Snapshot of activity (as of 2026-09-25)

| Repo | Default branch | Archived | Last push | Latest release | Open issues |
|---|---|---|---|---|---|
| spline (server) | develop | no | 2026-09-18 | release/1.0.0-RC3, 2026-06-02 | 47 |
| spline-spark-agent | develop | no | 2026-09-07 | release/2.4.0, 2026-08-31 | 76 |
| spline-ui | develop | no | 2026-09-10 | release/1.0.0-RC3, 2026-06-24 | 51 |
| spline-getting-started | main | no | 2026-06-24 | none | 7 |
| spline-openlineage | master | no | 2022-09-25 | none | 2 |
| spline-producer-proxy | master | no | 2023-09-26 | tag release/0.1.0 only | 0 |
| spline-python-agent | master | no | 2024-09-27 | release/0.1.2, 2023-08-10 | 5 |
| spline-misc | main | no | 2021-08-03 | none | 0 |
| spline-root-pom | main | no | 2021-05-06 | none | 0 |
| atum | master | no | 2026-05-13 | v3.10.0, 2024-11-04 | 19 |
| atum-service | master | no | 2026-09-25 | v0.8.0, 2026-07-23 | 27 |
| enceladus | develop | no | 2026-03-26 (last code commit 2024-04-11) | tags up to v3.0.0, no GitHub releases | 392 |
| hermes | develop | YES | 2023-05-05 | none | 15 |
| dataset-comparison | master | no | 2026-05-07 | 1.0.0, 2025-05-27 | 9 |
| datasets-similarity | master | YES | 2026-04-23 | none | 12 |

Sources: `gh api repos/AbsaOSS/<repo>` metadata, releases and commits endpoints, e.g. https://github.com/AbsaOSS/spline/releases, https://github.com/AbsaOSS/spline-spark-agent/releases/tag/release/2.4.0, https://github.com/AbsaOSS/atum-service/releases/tag/v0.8.0, https://github.com/AbsaOSS/enceladus/commits/develop, https://github.com/AbsaOSS/hermes (archived banner), https://github.com/AbsaOSS/datasets-similarity (README: "This repository is being archived due to no future practical use and low capacity").

The picture: Spline and Atum Service are the two living lines. Enceladus (the conformance engine) is effectively frozen with 392 open issues. Hermes and datasets-similarity are archived. The small Spline satellites are PoCs or one-off utilities.

---

## 1. Spline (server): https://github.com/AbsaOSS/spline

### Purpose and production problem
Spline captures lineage from data jobs (mainly Spark) and stores it as a graph for lineage and impact queries ("Data Lineage Tracking And Visualization Solution", repo description). It is split into agent, REST/Kafka gateway (producer and consumer APIs), ArangoDB storage and UI (https://github.com/AbsaOSS/spline-getting-started/blob/main/README.md).

### Core abstraction and data model
- Two producer entities: an immutable **ExecutionPlan** (how data moves: `operations` = one `write`, many `reads`, `other`; `attributes`; `expressions`; `systemInfo`; `agentInfo`; `extraInfo`) and an **ExecutionEvent** (when it ran: `planId`, `timestamp`, `durationNs`, `discriminator`, `error`, `extra`) (https://github.com/AbsaOSS/spline/blob/develop/producer-model/src/main/scala/za/co/absa/spline/producer/model/v1_1/executionPlan.scala, https://github.com/AbsaOSS/spline/blob/develop/producer-model/src/main/scala/za/co/absa/spline/producer/model/v1_1/ExecutionEvent.scala). The plan validates unique attribute, operation and expression IDs at construction.
- Attributes carry `childRefs` pointing to other attributes or expressions, which is how column lineage is encoded (https://github.com/AbsaOSS/spline/blob/develop/producer-model/src/main/scala/za/co/absa/spline/producer/model/v1_1/Attribute.scala).
- ArangoDB graph schema: node collections `dataSource, executionPlan, operation, progress, schema, attribute, expression`; edge collections `follows, writesTo, readsFrom, executes, depends, affects, progressOf, emits, produces, consistsOf, computedBy, derivesFrom, takes, uses`; aux collections `dbVersion, counter, txInfo` (https://github.com/AbsaOSS/spline/blob/develop/arangodb-foxx-services/src/main/persistence/model.ts).
- Every document carries `_created` and `_belongsTo` (the owning plan), so a plan and all its sub-graph can be deleted as a unit. Edges carry `index` (sibling order) and `path` (a JSONPath into the `_from` document) (https://github.com/AbsaOSS/spline/blob/develop/arangodb-foxx-api/src/main/scala/za/co/absa/spline/persistence/model/entities.scala).
- `Progress` (the stored event) denormalises plan details (`ExecPlanDetails`: data source URI, type, `append` flag, app name) "for performance optimization"; `DataSource` keeps `lastWriteDetails` (same file).
- `derivesFrom` is attribute-to-attribute dependency skipping expressions; `computedBy` points to the exact expression; `uses` is operation-level (filter, sort) expressions, i.e. control lineage rather than data lineage (maintainer explanation: https://github.com/AbsaOSS/spline/issues/1088).

### Design decisions and trade-offs visible in code
1. **Content-addressed plan IDs and idempotent ingest.** The agent computes the plan ID as a UUIDv5 over the plan JSON (see agent section). The server checks whether that plan already exists, skips re-insertion, and uses `discriminator` to detect a genuine UUID collision (`ensureNoExecPlanIDCollision`) (https://github.com/AbsaOSS/spline/blob/develop/producer-services/src/main/scala/za/co/absa/spline/producer/service/repo/ExecutionProducerRepositoryImpl.scala). Event keys are `planId` + base-36 timestamp, so redelivery of the same event is also a no-op (https://github.com/AbsaOSS/spline/blob/develop/producer-services/src/main/scala/za/co/absa/spline/producer/service/model/ExecutionEventKeyCreator.scala).
2. **Time-aware lineage, not static DAG lineage.** The backward-lineage primitive `observedWritesByRead` returns, for each data source a run read, the most recent successful non-append write *before the read time* plus all successful appends after it and before the read. Failed events (`p.error != null`) are excluded (https://github.com/AbsaOSS/spline/blob/develop/arangodb-foxx-services/src/main/services/observed-writes-by-read.ts). This answers "which specific upstream runs did this run actually consume", which a static job graph cannot.
3. **Home-grown logical transactions over ArangoDB.** A global counter issues sequential tx numbers with optimistic retries (50 attempts), and every document carries `_txInfo`; reads filter by live tx IDs to get isolation (https://github.com/AbsaOSS/spline/blob/develop/arangodb-foxx-services/src/main/services/txm/tx-manager-impl.ts). Analysis: this is the cost of choosing a store without multi-document transactional guarantees at the scale they needed.
4. **Immutable model, post-hoc capture.** The maintainer states that the Spline model "is immutable and it doesn't have a notion of a job state (started, finished etc)", data is sent after completion, which is the "fundamental difference" from OpenLineage (https://github.com/AbsaOSS/spline/issues/1033).
5. **Versioned storage migrations** as JS scripts per version pair `0.4.0-0.5.0.js` through `0.7.0-1.0.0.js` (https://github.com/AbsaOSS/spline/tree/develop/persistence/src/main/resources/migration-scripts).

### Failure modes prevented and suffered
- Prevented: lineage pointing at a failed upstream write. Filtering failed executions landed via PR #988 in 1.0.0-RC2 and a follow-up fix to the impact query for failed jobs in RC3 (https://github.com/AbsaOSS/spline/releases/tag/release/1.0.0-RC2, https://github.com/AbsaOSS/spline/releases/tag/release/1.0.0-RC3).
- Suffered: **graph query blow-up**. `lineage-overview` returned "598 Network read timeout" with ArangoDB at 100% CPU on large plans with self-dependencies (time t+1 depends on time t); 65 comments (https://github.com/AbsaOSS/spline/issues/738).
- Suffered: **unbounded growth**. "Is there any easy way to configure automatic retention?" (34 comments) led to a two-stage prune: delete old `progress` events, then delete orphan plans and every collection element whose `_belongsTo` is an orphan plan (https://github.com/AbsaOSS/spline/issues/684, https://github.com/AbsaOSS/spline/blob/develop/arangodb-foxx-services/src/main/services/prune-database.ts).
- Suffered: **cross-job column lineage never shipped.** Column lineage works within one plan; the API and UI for end-to-end attribute lineage remain open since early days (https://github.com/AbsaOSS/spline/issues/113, https://github.com/AbsaOSS/spline/issues/1088).
- Suffered: **database licence risk.** ArangoDB moved to BSL; maintainers say compliance "fully lays on the customer" (https://github.com/AbsaOSS/spline/issues/1356).
- Suffered: deployment friction; the most-commented issue is Azure Databricks configuration (87 comments) (https://github.com/AbsaOSS/spline/issues/524).

### Maturity
Active but slow: 1.0.0 has been in RC since at least 2026-05 (RC2 2026-05-01, RC3 2026-06-02); recent commits are mostly dependency bumps (https://github.com/AbsaOSS/spline/commits/develop).

### Superseded vs unique in 2026
- Superseded: the wire format. The Spline agent itself now emits OpenLineage 2-0-2 with SchemaDatasetFacet 1-1-1 and ColumnLineageDatasetFacet 1-2-0 (https://github.com/AbsaOSS/spline-spark-agent/releases/tag/release/2.4.0). Marquez and other OpenLineage backends cover the collection role.
- Still unique and worth copying: (a) content-addressed plan identity separated from run events; (b) the "observed writes by read" temporal join, respecting append vs overwrite and failure; (c) `_belongsTo` aggregate ownership for clean retention.

### Lessons for Ubunye
- Lineage/impact: implement impact queries over Ubunye's own receipts, not a graph DB. A receipt already has content hashes; add a `reads[]` list of `(dataset_uri, observed_write_receipt_id)` computed with Spline's rule (last successful overwrite before read start, plus later successful appends). That is a precise "which upstream run versions fed this output" answer that OpenLineage events alone do not give.
- Recovery/idempotency: copy the plan-hash plus event-key design: same config hash + same inputs means the same plan ID; event key = plan hash + start time. Makes replays and double-emission harmless.
- Catalogue/governance: adopt retention by aggregate ownership (every artifact row points at its receipt; prune receipts, then orphans).
- Do NOT copy: a graph database dependency (ArangoDB BSL, CPU blow-ups, custom tx layer). Ubunye's receipts are files; SQLite or DuckDB over receipts is enough for impact queries at single-team scale.

---

## 2. spline-spark-agent: https://github.com/AbsaOSS/spline-spark-agent

### Purpose and production problem
A Spark `QueryExecutionListener` that turns each write action's logical plan into a Spline plan plus event, "codeless" via Spark config or programmatic init (https://github.com/AbsaOSS/spline-spark-agent/blob/develop/README.md).

### Core abstraction
- `LineageHarvester.harvest(result: Either[Throwable, Duration])` extracts the write command, walks the logical plan into operation builders (topologically sorted), collects read metrics from leaf exec nodes and write metrics from the root, applies post-processing filters, *then* hashes the plan to produce its ID, then builds the event with `error = stack trace` on failure (https://github.com/AbsaOSS/spline-spark-agent/blob/develop/core/src/main/scala/za/co/absa/spline/harvester/LineageHarvester.scala).
- ID scheme: plan ID is UUIDv5 (SHA-1) over the plan JSON in a fixed namespace; attributes, expressions, operations get sequential IDs `attr-N`, `expr-N`, `op-N`; the UUID version is configurable but the config warns "DON'T MODIFY UNLESS YOU UNDERSTAND THE IMPLICATIONS" (https://github.com/AbsaOSS/spline-spark-agent/blob/develop/core/src/main/scala/za/co/absa/spline/harvester/idGenerators.scala, https://github.com/AbsaOSS/spline-spark-agent/blob/develop/core/src/main/resources/spline.default.yaml).
- Failed-run policy is explicit: `sql.failure.capture: NONE | NON_FATAL | ALL` (default NON_FATAL) (same yaml).
- Ignored-write detection: for `SaveMode.Ignore` writes, lineage is skipped if metrics show nothing was written, with `onMissingMetrics: IGNORE_LINEAGE | CAPTURE_LINEAGE` (same yaml and harvester).
- Dispatchers are named, config-declared components: `http, kafka, console, logging, composite (failOnErrors), fallback (primary/fallback), hdfs (_LINEAGE file next to data), httpOpenLineage` (same yaml).
- Post-processing filters: a default `dsPasswordReplace` filter masks passwords in URIs by name regex and value regex; `userExtraMeta` attaches user metadata by JSON rules; overriding the root filter silently drops defaults unless you chain `default` explicitly (same yaml).
- Plugin API with capability traits: `DataSourceFormatNameResolving`, `ReadNodeProcessing`, `RddReadNodeProcessing`, `WriteNodeProcessing`, `BaseRelationProcessing`, `RelationProviderProcessing`; plugins are auto-discovered by classpath scan or registered explicitly (https://github.com/AbsaOSS/spline-spark-agent/blob/develop/core/src/main/scala/za/co/absa/spline/harvester/plugin/pluginModel.scala).
- The README publishes a coverage list of Spark commands in three buckets: Implemented, To be implemented (warned at runtime), Ignored (https://github.com/AbsaOSS/spline-spark-agent/blob/develop/README.md#spark-features-coverage).

### Design decisions and trade-offs
- SemVer is defined over entry points, extension APIs, config properties and supported Spark versions, and agent/server compatibility is deliberately decoupled because agents are embedded in jobs that are "only rarely if ever updated" (README, Versioning section). This is a mature statement of what "public API" means for an embedded component.
- Analysis: hashing after post-processing means masking or metadata rules change plan identity. That is correct (what was recorded changed), but it means a filter rollout splits history.

### Failure modes suffered (issues)
The agent's pain is almost entirely coupling to Spark and vendor internals:
- Databricks Delta MERGE not captured, 81 comments (https://github.com/AbsaOSS/spline-spark-agent/issues/570); saveAsTable/SQL on Azure Databricks, 50 comments (https://github.com/AbsaOSS/spline-spark-agent/issues/107).
- Attribute dependency cycles (https://github.com/AbsaOSS/spline-spark-agent/issues/125), window functions breaking harvesting on Spark 3 (https://github.com/AbsaOSS/spline-spark-agent/issues/262), duplicated attribute IDs (https://github.com/AbsaOSS/spline-spark-agent/issues/272), Delta errors (https://github.com/AbsaOSS/spline-spark-agent/issues/609), Delta overwrite lineage missing (https://github.com/AbsaOSS/spline-spark-agent/issues/507), and a 2026-07 crash on `Column.withField/dropFields` (commit message "issue #922 Fix lineage crash on Column.withField/dropFields").
- Version treadmill: most-reacted requests are Delta >= 3.2.1 (https://github.com/AbsaOSS/spline-spark-agent/issues/848), Java 17 (https://github.com/AbsaOSS/spline-spark-agent/issues/782), Spark 3.5 (https://github.com/AbsaOSS/spline-spark-agent/issues/770); Spark 4.0 support is still open since 2024-09 (https://github.com/AbsaOSS/spline-spark-agent/issues/831).
- Security: plaintext JDBC passwords in captured URIs (https://github.com/AbsaOSS/spline-spark-agent/issues/69), which produced the `dsPasswordReplace` filter.
- Checkpoint lineage (Spark `checkpoint()` breaks the source chain) still open (https://github.com/AbsaOSS/spline-spark-agent/issues/546).

### Superseded vs unique
- Superseded: OpenLineage's own Spark integration covers the same capture job, and Spline 2.4.0 now speaks OpenLineage 2-0-2 natively (release note above).
- Unique: the explicit failure-capture policy, ignored-write detection, the fallback dispatcher, and the published coverage table.

### Lessons for Ubunye
- Connectors: publish a **capability vocabulary** per connector the way the agent publishes Implemented / To be implemented / Ignored commands and uses narrow capability traits. For Ubunye: `read`, `write_append`, `write_overwrite`, `merge`, `partition_overwrite`, `schema_evolve`, `emits_row_count`, `transactional_commit`. Unimplemented capabilities should warn, like the agent does.
- Security/governance: ship a default redaction filter on everything that leaves the process (URIs, options, receipts, OL events), and make "replace root filter" chain defaults rather than silently drop them (the agent's documented footgun).
- Recovery: add a fallback emitter (primary OL endpoint, fallback local file) and make "emission failure never fails the job" the default, with an opt-in fail-fast, as `composite.failOnErrors: false` does.
- Receipt semantics: record `write_mode` (append vs overwrite) and `rows_written`; skip or flag no-op writes. These are the inputs to the temporal lineage rule in section 1.
- Do NOT copy: lineage by reverse-engineering the engine's logical plan. Ubunye is config-first, so its declared inputs and outputs are the lineage source of truth. Plan parsing is where Spline spent most of its issue budget.

---

## 3. spline-ui: https://github.com/AbsaOSS/spline-ui
Angular UI for lineage overview, execution plan detail, and forward impact ("High level data impact (forward lineage) overview", https://github.com/AbsaOSS/spline-ui/pull/205). Top issues are UX (loading/failure indicators, https://github.com/AbsaOSS/spline-ui/issues/114) and dependency bumps; build breakage from Node downloads (https://github.com/AbsaOSS/spline-ui/issues/213). The open "API :: new API swagger definition" (https://github.com/AbsaOSS/spline-ui/issues/6) shows UI and server contract drifted.
Lesson for Ubunye: do not build a UI. Impact answers belong in the CLI and MCP server ("what depends on dataset X", "which runs consumed receipt Y"), where agents and humans both reach them.

## 4. spline-getting-started: https://github.com/AbsaOSS/spline-getting-started
Docker Compose for server, UI, ArangoDB and seeded sample jobs, plus Databricks and AWS how-tos (README). The long-lived issues are all "Server is unavailable" and compose errors (https://github.com/AbsaOSS/spline-getting-started/issues/48, https://github.com/AbsaOSS/spline-getting-started/issues/47, https://github.com/AbsaOSS/spline-getting-started/issues/23) and docs/version mismatch (https://github.com/AbsaOSS/spline-getting-started/issues/11). The README names three extension levels: config, extension module, fork.
Lesson: every multi-service dependency multiplies first-run failure. Ubunye's "runs anywhere with zero services" is a real advantage worth protecting; the lineage backend must stay optional.

## 5. spline-openlineage: https://github.com/AbsaOSS/spline-openlineage
PoC bridge: a REST proxy puts OpenLineage events on Kafka; a Kafka Streams aggregator merges all events of one run in a session window and emits one Spline plan plus event when `COMPLETE` arrives (https://github.com/AbsaOSS/spline-openlineage/blob/master/README.md, https://github.com/AbsaOSS/spline-openlineage/blob/master/aggregator/src/main/scala/za/co/absa/spline/ol/aggregator/AggregatorApp.scala).
The converter shows the semantic gap concretely: pinned to OpenLineage 0.3.1 models; runs without outputs are dropped; `append = false` is hard-coded; `error = None` always (FAIL and ABORT events are ignored); no attributes, so no column lineage; facets stuffed into `extra` (https://github.com/AbsaOSS/spline-openlineage/blob/master/aggregator/src/main/scala/za/co/absa/spline/ol/aggregator/conversion/OpenLineageToSplineConverter.scala). Tests do not run under Maven (https://github.com/AbsaOSS/spline-openlineage/issues/7). Dormant since 2022.
Lesson for Ubunye: when Ubunye emits OpenLineage 2-0-2, it must emit what this bridge could not reconstruct: the write mode (overwrite vs append, e.g. via a lifecycle or custom facet), FAIL events with error facets, and column lineage. Otherwise downstream temporal lineage is impossible. Treat this repo as the list of fields a consumer needs.

## 6. spline-producer-proxy: https://github.com/AbsaOSS/spline-producer-proxy
A tiny TypeScript server exposing the same Producer API, appending payloads to a file while the Spline server is under maintenance, with a shell script to replay them later (https://github.com/AbsaOSS/spline-producer-proxy/blob/master/README.md). No releases beyond one tag; dormant since 2023.
Lesson: this is exactly Ubunye's outbox problem. Emit OL and receipts to a local durable outbox first, then ship; replay on reconnect. Build it into the engine rather than as a sidecar.

## 7. spline-python-agent: https://github.com/AbsaOSS/spline-python-agent
Decorators `@track_lineage`, `@inputs(...)`, `@output(url, WriteMode.APPEND)` declare I/O for plain Python functions; parameters can be referenced with `"{param}"` templates (https://github.com/AbsaOSS/spline-python-agent/blob/master/README.md). The harvester builds a coarse plan (Write, Reads, one "Python script" op), embeds the function's **source code, module and file** in the plan, and derives the plan ID as UUIDv5 over the plan JSON, so a code change creates a new plan identity (https://github.com/AbsaOSS/spline-python-agent/blob/master/src/spline_agent/harvester.py). Issues show sound lessons: exceptions must propagate unchanged (https://github.com/AbsaOSS/spline-python-agent/issues/17), inputs must not be deduplicated (https://github.com/AbsaOSS/spline-python-agent/issues/14). Last release 0.1.2 in 2023.
Lesson: this is the closest AbsaOSS analogue to Ubunye (declared I/O, not inferred). It validates Ubunye's direction and suggests including a code fingerprint (task module hash) in the receipt's plan hash. Do not embed full source in lineage events (size, leakage); hash it.

## 8. spline-misc and spline-root-pom
spline-misc holds only a Spline 0.3 UI adapter for Menas (https://github.com/AbsaOSS/spline-misc/blob/main/README.md). spline-root-pom is a shared Maven parent last released 0.6.1 (https://github.com/AbsaOSS/spline-root-pom). No lessons beyond "legacy adapters accumulate".

---

## 9. Atum (library): https://github.com/AbsaOSS/atum

### Purpose and production problem
Regulated banking needs to prove no records were added or lost and critical fields were not modified; the README names BCBS, inner-join loss and outer-join explosion, and casting corruption as the concrete failure classes (https://github.com/AbsaOSS/atum/blob/master/README.md#motivation).

### Core abstraction and file format
- `ControlMeasure { metadata, runUniqueId, checkpoints[] }`; `ControlMeasureMetadata { sourceApplication, country, historyType, dataFilename, sourceType, version, informationDate, additionalInfo: Map }`; `Checkpoint { name, software, version, processStartTime, processEndTime, workflowName, order, controls[] }`; `Measurement { controlName, controlType, controlCol, controlValue }`; `RunStatus { status: allSucceeded|stageSucceeded|running|failed, error: RunError{job, step, description, technicalDetails} }` (https://github.com/AbsaOSS/atum/tree/master/model/src/main/scala/za/co/absa/atum/model).
- The JSON is stored as an `_INFO` file beside each batch's data; every job appends its checkpoints to the inherited list, so the file carries the full chain from source to output (README, Features).
- Control types: `count, distinctCount, aggregatedTotal (SUM), absAggregatedTotal (SUM(ABS)), aggregatedTruncTotal, absAggregatedTruncTotal, hashCrc32 (SUM(CRC32))` (https://github.com/AbsaOSS/atum/blob/master/atum/src/main/scala/za/co/absa/atum/core/ControlType.scala). Default strategy: `SUM(ABS(x))` for numerics, `SUM(CRC32(x))` otherwise (README).

### Design decisions visible in code
- Values are stored as strings; long sums are cast to `Decimal(38,0)` to avoid overflow, string columns to `Decimal(38,18)`, BigDecimals normalised with `stripTrailingZeros().toPlainString()` because "different serializers generate different JSONs for BigDecimal" (https://github.com/AbsaOSS/atum/blob/master/atum/src/main/scala/za/co/absa/atum/core/MeasurementProcessor.scala).
- `SUM(CRC32)` is an order-independent content fingerprint of a column, cheap to compute and compare across engines (same file).
- Rename and drop tracking is explicit: `registerColumnRename(old, new)` rewrites the measure's `controlCol`; `registerColumnDrop(col)` removes the measure (same file, README routines table).
- Checkpoints are eager: each `setCheckpoint()` triggers one Spark action per measure and caches the DataFrame by default, with knobs to unpersist or change storage level (README routines table).

### Failure modes prevented and suffered
- Prevented: silent row loss or explosion across stages, detected by comparing counts and totals between checkpoints; Enceladus fails on empty output when earlier checkpoints were non-zero (section 11).
- Suffered: **global mutable state**. Atum is attached to the Spark session, so jobs with multiple DataFrames get measured wrongly; the "Atum redesign" issue asks for per-DataFrame contexts and immutable, functional design (open since 2020) (https://github.com/AbsaOSS/atum/issues/28).
- Suffered: **serialisation drift** between json4s and Jackson for enums (https://github.com/AbsaOSS/atum/issues/83).
- Analysis: an unknown control type silently yields `"N/A"` rather than failing (MeasurementProcessor `getMeasurementFunction`), and a registered drop silently removes the control, so a pipeline can stop reconciling a column without anyone noticing.
- Limitations stated by maintainers: only one input may carry an `_INFO` file; multiple batch blocks cannot be processed together (README, Limitations).

### Maturity
Last release v3.10.0 (2024-11-04); 2026-05 commits added Scala 2.13 and release automation (https://github.com/AbsaOSS/atum/commits/master). The atum-service README says its agent "is intended to replace the current Atum repository" (https://github.com/AbsaOSS/atum-service/blob/master/README.md#agent-agent).

### Lessons for Ubunye
- Reconciliation: this is the missing Ubunye feature, and Atum gives a minimal, proven measure set. Implement `count`, `sum`, `abs_sum`, `distinct_count`, `sum_crc32` as backend-neutral measures (Spark and pandas both have them), computed at declared checkpoints (at least `input` and `output`), stored as strings with canonical decimal formatting, inside the receipt.
- Schema evolution: carry explicit rename and drop maps in task config so reconciliation survives renames. Unlike Atum, make a dropped measured column a reported event, not a silent removal.
- Performance: compute all measures in one aggregation pass per checkpoint rather than one action per measure (Analysis of MeasurementProcessor).
- Do NOT copy: session-global state, sidecar `_INFO` files as the source of truth (they break on object stores and multi-input jobs), silent "N/A".

---

## 10. Atum Service: https://github.com/AbsaOSS/atum-service

### Purpose
The centralised successor: agents compute measures and push them to a server backed by PostgreSQL; the server "never receives any real data" and "does not implement any checks or validations against these control measures", it only captures them (https://github.com/AbsaOSS/atum-service/blob/master/README.md).

### Data model (actual DDL)
- `runs.partitionings(id_partitioning, partitioning JSONB UNIQUE, created_by, created_at)` (https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/runs/V0.1.0.35__partitionings.ddl). Partitioning JSON is `{"version":1, "keys":[ordered list], "keysToValuesMap":{...}}`, validated by a DB function; key order matters; a non-strict mode allows patterns with NULL values (https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/validation/V0.2.0.6__validate_partitioning.sql).
- `runs.measure_definitions(fk_partitioning, measure_name, measured_columns TEXT[], UNIQUE(...))` (https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/runs/V0.1.0.33__measure_definitions.ddl).
- `runs.checkpoints(id_checkpoint UUID, fk_partitioning, checkpoint_name, process_start_time, process_end_time, measured_by_atum_agent, ...)`, with the comment "The UUID is coming from outside world to distinguish repeated entry from a re-run checkpoint" (https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/runs/V0.1.0.32__checkpoints.ddl).
- `runs.measurements(fk_measure_definition, fk_checkpoint, measurement_value JSONB, UNIQUE(fk_checkpoint, fk_measure_definition))` (https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/runs/V0.1.0.34__measurements.ddl).
- `runs.additional_data(fk_partitioning, ad_name, ad_value, UNIQUE)` with a history table (https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/runs/V0.1.0.31__additional_data.ddl, V0.2.0.17__additional_data_history.ddl in the same folder); `runs.checkpoint_properties(name, value)` indexed for lookup (https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/runs/V0.5.1.1__checkpoint_properties.ddl).
- `flows.flows` and `flows.partitioning_to_flow` link partitionings into data flows; partitionings have parent links and an ancestors query (https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/flows/V0.1.0.41__flows.ddl, https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/runs/V0.4.0.3__get_partitioning_ancestors.sql).
- Measure results are typed: `MeasureResultDTO { mainValue: TypedValue{value: String, valueType}, supportValues: Map }` (https://github.com/AbsaOSS/atum-service/blob/master/model/src/main/scala/za/co/absa/atum/model/dto/MeasureResultDTO.scala). `UnknownMeasure` lets applications supply their own values (README table; https://github.com/AbsaOSS/atum-service/blob/master/agent/src/main/scala/za/co/absa/atum/agent/model/Measure.scala).
- API: v1 `createCheckpoint`, `createPartitioning`; v2 resources `partitionings, checkpoints, additional-data, flows, measures, main-flow, ancestors`, plus health, readiness, liveness, ZIO and Hikari metrics (https://github.com/AbsaOSS/atum-service/blob/master/model/src/main/scala/za/co/absa/atum/model/ApiPaths.scala).

### Design decisions and trade-offs
- Logic lives in versioned Postgres functions managed by Flyway (66 SQL files; e.g. `V0.5.1.3__write_checkpoint_with_properties_using_partitioning.sql`) (https://github.com/AbsaOSS/atum-service/tree/master/database/src/main/postgres/runs).
- Idempotent re-submission via client-generated checkpoint UUID (DDL comment above).
- v0.8.0 (2026-07-23) added an HTTP retry in the agent, DB timeout config, optional parent-key merge for sub-partitions, and a guarantee that the parent-child flow link exists even when a shared child partitioning already exists (https://github.com/AbsaOSS/atum-service/releases/tag/v0.8.0).
- Database password from AWS Secrets Manager (`PostgresDataSourceWithPasswordFromSecretsManager.scala`, https://github.com/AbsaOSS/atum-service/tree/master/server/src/main/scala/za/co/absa/atum/server/api/database).

### Failure modes suffered
- Partition key order differed between Scala 2.12 and 2.13 because of `ListMap ++` semantics, which matters because partitioning identity is order-sensitive (https://github.com/AbsaOSS/atum-service/issues/261).
- A 2026-08 issue from an internal consumer ("Unify") shows the read side is the weak side: filtering checkpoints by `executionID` sets or name, and "latest checkpoint of a fixed name" force workarounds (https://github.com/AbsaOSS/atum-service/issues/478). The Reader module is marked "not yet implemented to an operational abilities" (README). A GraphQL retrieval spike is open (https://github.com/AbsaOSS/atum-service/issues/436).
- Analysis: because the service never compares measures, every consumer has to write its own reconciliation logic. Capture without verdicts moves the hard part downstream.

### Superseded vs unique
- Partly overlapping standards: OpenLineage has dataset-level data quality metrics and assertions facets (https://openlineage.io/docs/spec/facets/dataset-facets/data_quality_metrics, https://openlineage.io/docs/spec/facets/dataset-facets/data_quality_assertions). They carry metrics, but not the checkpoint chain or partition identity.
- Still unique: ordered-key partitioning as identity, checkpoint UUIDs for re-run dedup, per-partition additional data with history, flows linking partitionings across applications.

### Lessons for Ubunye
- Backfill/partition: adopt a canonical **partition identity** in receipts: ordered keys plus values, versioned, canonically serialised (sorted by declared order, never by map iteration order). Backfill = rerun for a set of partition identities; receipts are keyed by `(task, partition_identity, attempt)`.
- Reconciliation: store measures per checkpoint in the receipt, and unlike Atum Service, **evaluate** them: `output.count == input.count - quarantined - filtered` style rules declared in config, producing a pass/fail verdict in the receipt.
- Recovery: client-generated attempt IDs so resubmitted evidence is deduplicated, as Atum does for checkpoints.
- Interop: map measures into OpenLineage `dataQualityMetrics` input/output facets so any OL consumer sees them, and keep the richer chain in the receipt.
- Do NOT copy: a mandatory central Postgres service for single-team use; putting business logic in DB functions (66 migration files for a small domain is a maintenance tax).

---

## 11. Enceladus (Menas, Standardization, Conformance): https://github.com/AbsaOSS/enceladus

### Purpose
A "Dynamic Conformance Engine": standardise any input format to Parquet with a registered schema, then conform to group-wide reference values (e.g. `DE` and `Deutschland` both become `Germany`) (https://github.com/AbsaOSS/enceladus/blob/develop/README.md).

### Entities and versioning (Menas / REST API)
- All entities extend `VersionedModel { name, version: Int, description, dateCreated, userCreated, lastUpdated, userUpdated, disabled (+date, user), locked (+date, user), parent: Reference }` (https://github.com/AbsaOSS/enceladus/blob/develop/data-model/src/main/scala/za/co/absa/enceladus/model/versionedModel/VersionedModel.scala).
- `Dataset { hdfsPath (raw), hdfsPublishPath, schemaName, schemaVersion, conformance: List[ConformanceRule], properties, propertiesValidation, schedule }` pins an exact schema version (https://github.com/AbsaOSS/enceladus/blob/develop/data-model/src/main/scala/za/co/absa/enceladus/model/Dataset.scala).
- `MappingTable { hdfsPath, schemaName, schemaVersion, defaultMappingValue[], filter }` (https://github.com/AbsaOSS/enceladus/blob/develop/data-model/src/main/scala/za/co/absa/enceladus/model/MappingTable.scala).
- Dataset properties are "not free-form, they are bound by system-wide property definitions" (README; https://github.com/AbsaOSS/enceladus/blob/develop/data-model/src/main/scala/za/co/absa/enceladus/model/properties/PropertyDefinition.scala).
- `Run { uniqueId, runId, dataset, datasetVersion, splineRef, startDateTime, runStatus, controlMeasure }`: a run embeds its Atum control measure and a Spline reference, tying lineage and control totals to one run record (https://github.com/AbsaOSS/enceladus/blob/develop/data-model/src/main/scala/za/co/absa/enceladus/model/Run.scala).
- Model migrations copy and version collections so old and new Menas can coexist and roll back without restoring dumps (https://github.com/AbsaOSS/enceladus/issues/311).

### Conformance execution
- Eleven rule types (Casting, Concatenation, Drop, Literal, Mapping, Negation, SingleColumn, SparkSessionConf, Uppercase, FillNulls, Coalesce) plus `ExtensibleConformanceRule`/`CustomConformanceRule`; every rule has `order`, `outputColumn`, `controlCheckpoint` (https://github.com/AbsaOSS/enceladus/blob/develop/data-model/src/main/scala/za/co/absa/enceladus/model/conformanceRule/package.scala).
- `DynamicInterpreter` writes a "Start" checkpoint, folds rules in order, writes a checkpoint after each rule flagged `controlCheckpoint`, then "End". It **refuses rules that overwrite original columns** unless a feature switch allows it ("immutability pattern"). It groups mapping rules on the same array to explode once, and picks a mapping strategy per rule: broadcast (at most 10 join keys and table under `max.broadcast.size.mb`), group-explode, or explode-join. It also carries a workaround for a Catalyst optimiser freeze on long plans (https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/scala/za/co/absa/enceladus/conformance/interpreter/DynamicInterpreter.scala, https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/resources/reference.conf). User docs: "We never override a column. Each rule produces a new column" (https://github.com/AbsaOSS/enceladus/blob/gh-pages/_docs/3.0.0/usage/menas-conformance-rules.md).
- Mapping semantics: `attributeMappings` (mapping-table column to dataset column), `targetAttribute`, optional `additionalColumns`, `isNullSafe`, `mappingTableFilter` with `overrideMappingTableOwnFilter` (rule model above). A null result where the join key was non-null appends `confMapError` to `errCol`; defaults can be set per target or with `*` (https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/scala/za/co/absa/enceladus/conformance/interpreter/rules/mapping/MappingRuleInterpreter.scala, https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/scala/za/co/absa/enceladus/conformance/interpreter/rules/mapping/CommonMappingRuleInterpreter.scala).
- **Point-in-time reference data**: the mapping table is read from a date-partitioned path chosen by the run's report date (`DataSource.getDataFrame(path, reportDate, filter)`), so reprocessing an old date uses that date's reference data (https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/scala/za/co/absa/enceladus/conformance/datasource/DataSource.scala).

### errCol (row-level error channel)
`errCol` is an array of `ErrorMessage { errType, errCode (E#####), errMsg, errCol, rawValues[], mappings[] }` accumulated across standardisation and conformance; types include `stdCastError, stdNullError, stdTypeError, stdSchemaError, confMapError, confCastErr, confNegErr, confLitErr` (https://github.com/AbsaOSS/enceladus/blob/develop/utils/src/main/scala/za/co/absa/enceladus/utils/error/ErrorMessage.scala, https://github.com/AbsaOSS/enceladus/blob/gh-pages/_docs/3.0.0/usage/errcol.md). Standardisation errors cite the `sourcecolumn` metadata, i.e. the original source column, not the renamed one (errcol.md). Rows stay in the output; errors travel with them.

### Run tracking, restatement and reconciliation
- Output path is `hdfsPublishPath/enceladus_info_date=<date>/enceladus_info_version=<n>`, and the job refuses to run if the output path exists: "Increment the run version, or delete ..." (https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/scala/za/co/absa/enceladus/common/CommonJobExecution.scala). If no version is supplied it infers `latest + 1` and warns that this is EXPERIMENTAL and unsafe under concurrency or version gaps (same file).
- `ControlInfoValidation` requires `raw` and `source` count checkpoints and records them into metadata, with `control.info.validation = strict | warning | none` (https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/scala/za/co/absa/enceladus/common/ControlInfoValidation.scala, reference.conf default `warning`).
- Empty output fails the run unless all previous count checkpoints were also zero (`handleEmptyOutput`, CommonJobExecution).
- `enceladus_record_id` with strategy `uuid | stableHashId | none` so DQ errors sent to Kafka can be traced to the source row (https://github.com/AbsaOSS/enceladus/issues/1294, reference.conf).
- Plugins: control-metrics plugins fire on every checkpoint and status change; built-ins push control info and errors to Kafka in Avro; plugin order via numeric suffixes; plugin exceptions are logged and never fail the job (https://github.com/AbsaOSS/enceladus/blob/gh-pages/_docs/3.0.0/plugins.md, https://github.com/AbsaOSS/enceladus/blob/gh-pages/_docs/3.0.0/plugins-built-in.md).

### Failure modes suffered
- Performance of arrays via explode and collapse (https://github.com/AbsaOSS/enceladus/issues/110) and the Catalyst freeze needing a workaround switch (reference.conf).
- Eager retries against Menas overloading it; exponential backoff requested and still open (https://github.com/AbsaOSS/enceladus/issues/1943).
- Tight coupling to a runtime metadata service (jobs query Menas at start), HDFS, MongoDB, Tomcat, Oozie (README "How to run"; Oozie epic https://github.com/AbsaOSS/enceladus/pull/434).
- 392 open issues and no code commits since 2024-04 (snapshot table).

### Superseded vs unique
- Superseded: the Menas UI/REST registry (catalogues such as Unity Catalog now hold schemas and properties; UNVERIFIED that Absa migrated), Oozie scheduling, HDFS-first paths. Standardisation was extracted into a separate library, spark-data-standardization (active 2026-09, https://github.com/AbsaOSS/spark-data-standardization).
- Still unique and valuable: rule-level checkpoints, the immutability pattern, errCol with source-column attribution, point-in-time mapping tables, `info_date`/`info_version` restatement partitions, and strict/warning/none validation levels.

### Lessons for Ubunye
- Semantic conformance: add a declarative `mapping` step (reference table by versioned name, join keys, target, default, null-safety, filter) that emits unmapped keys as row errors rather than dropping rows. Read reference data as of the run's logical date.
- Quarantine: Ubunye's quarantine is row-level already; adopt errCol's structure (type, code, column, raw values) and attribute errors to the source column after renames.
- Backfill/restatement: model outputs as `(logical_date, version)` and refuse to overwrite an existing version by default. Do not infer versions by listing storage (Enceladus itself flags the race).
- Reconciliation: validation strictness as a config enum `strict | warning | none`; "empty output while inputs were non-empty" as a built-in rule.
- Do NOT copy: a runtime dependency on a metadata server during every job; UI-authored rules stored in MongoDB rather than reviewable config; the engine-internals workarounds.

---

## 12. Hermes (archived): https://github.com/AbsaOSS/hermes
E2E test runner and dataset comparison built for Enceladus (README; archived 2023). `DatasetComparator` checks schemas (ignoring metadata; or against a provided subset schema to exclude noisy columns such as timestamps), builds a key column as `md5(concat_ws("|", keys or all columns as string))`, counts duplicates and fails unless `allowDuplicates`, computes `except` both ways, then full-joins the two diffs and writes a per-row error column listing differing flattened fields (https://github.com/AbsaOSS/hermes/blob/develop/datasetComparison/src/main/scala/za/co/absa/hermes/datasetComparison/DatasetComparator.scala). `InfoFileComparison` diffs two Atum `_INFO` files while ignoring run-specific keys (application IDs, directory sizes) and treating version keys separately (https://github.com/AbsaOSS/hermes/blob/develop/infoFileComparison/src/main/resources/reference.conf). A later fix addressed wrong error columns when the reference array is shorter (https://github.com/AbsaOSS/hermes/pull/128).
Analysis: `concat_ws` skips NULLs, so rows `(NULL, "x")` and `("x", NULL)` produce the same key; and `except` is set-based, so duplicate counts are only caught by the separate duplicate check.
Lesson for Ubunye: a **golden-output conformance kit** needs exactly these parts: schema check with an explicit ignore list, duplicate policy, keyed or keyless diff, and a comparison of control totals with a noise-key ignore list. Encode NULLs explicitly in any row hash.

## 13. dataset-comparison: https://github.com/AbsaOSS/dataset-comparison
Built to prove a migration from a legacy Crunch implementation to Spark produced the same outputs (README). Algorithm: exclude columns, hash each row, `exceptAll` on hashes both ways (multiset), join back to rows; then, if both sides differ and are below a threshold (200 by default), a keyless best-match pairing by Hamming distance over columns (https://github.com/AbsaOSS/dataset-comparison/blob/master/bigfiles/core/src/main/scala/za/co/absa/Comparator.scala, https://github.com/AbsaOSS/dataset-comparison/blob/master/bigfiles/core/src/main/scala/za/co/absa/analysis/RowByRowAnalysis.scala, README). The README states noise removal was deferred and proposes detecting noise columns by comparing two legacy runs against each other (README, "Removing noise"). Open issues: fail on duplicates, primary key option, run arguments and source path in metrics JSON (https://github.com/AbsaOSS/dataset-comparison/issues/4, /7, /5, /6).
Analysis, a real defect: the row hash is `row.mkString.hashCode`, a 32-bit Java string hash with no separator or NULL marker (https://github.com/AbsaOSS/dataset-comparison/blob/master/bigfiles/core/src/main/scala/za/co/absa/hash/HashUtils.scala). `("ab","c")` and `("a","bc")` collide by construction, and 32-bit hashes collide by chance once tables reach tens of thousands of rows (birthday bound). A collision can hide a real difference, which is the worst failure for a migration-equivalence tool. The best-match loop runs `head()` and `filter` per candidate row on Spark, which is driver-bound (same file).
Lesson for Ubunye: the "run twice to learn the noise columns" idea is excellent for a conformance kit (run the same task twice on the same input; columns that differ are nondeterministic and get excluded or flagged). Use a 128-bit or wider hash over a canonical encoding with typed NULL markers and separators, and put the comparison arguments into the output metrics (their own open issues).

## 14. datasets-similarity (archived): https://github.com/AbsaOSS/datasets-similarity
Python thesis-style project: table similarity as "at least k similar columns", using metadata (types, kinds, completeness), column-name embeddings and Column2Vec (README). Archived for "no future practical use and low capacity" (README). Lesson: semantic column matching by embeddings did not survive contact with production here; Ubunye's semantic mapping should be explicit declared mappings first, with LLM-suggested mappings only as reviewable proposals through its LLM port (replayed and budgeted), never auto-applied.

---

## Matrix

| Problem | Evidence links | AbsaOSS repo | Ubunye relevance | Leaning |
|---|---|---|---|---|
| Which upstream run versions fed this output (temporal lineage) | https://github.com/AbsaOSS/spline/blob/develop/arangodb-foxx-services/src/main/services/observed-writes-by-read.ts | spline | High: impact queries are missing | Build (over receipts) |
| Idempotent lineage ingest, dedup of identical plans | https://github.com/AbsaOSS/spline-spark-agent/blob/develop/core/src/main/scala/za/co/absa/spline/harvester/idGenerators.scala, https://github.com/AbsaOSS/spline/blob/develop/producer-services/src/main/scala/za/co/absa/spline/producer/service/repo/ExecutionProducerRepositoryImpl.scala | spline, agent | High: receipts already hashed | Build (small) |
| Failed runs polluting lineage | https://github.com/AbsaOSS/spline/releases/tag/release/1.0.0-RC2, https://github.com/AbsaOSS/spline/releases/tag/release/1.0.0-RC3 | spline | High | Build |
| Lineage store growth and retention | https://github.com/AbsaOSS/spline/issues/684 | spline | Medium | Build (aggregate-owned prune) |
| Graph query blow-up on big plans | https://github.com/AbsaOSS/spline/issues/738 | spline | Medium (warning) | Ignore graph DB |
| Cross-job column lineage | https://github.com/AbsaOSS/spline/issues/113, https://github.com/AbsaOSS/spline-spark-agent/releases/tag/release/2.4.0 | spline, agent | Medium | Integrate (OL ColumnLineage facet) |
| Secrets leaking into lineage | https://github.com/AbsaOSS/spline-spark-agent/issues/69 | agent | High | Build (default redaction) |
| Lineage emission when backend is down | https://github.com/AbsaOSS/spline-producer-proxy/blob/master/README.md, spline.default.yaml fallback | producer-proxy, agent | High | Build (outbox) |
| Connector coverage drift | https://github.com/AbsaOSS/spline-spark-agent/blob/develop/README.md#spark-features-coverage | agent | High | Build (capability vocabulary) |
| Engine-internals parsing fragility | https://github.com/AbsaOSS/spline-spark-agent/issues/570, /125, /262, /831 | agent | High (warning) | Ignore (stay declarative) |
| OL to other models lose write mode and failure | https://github.com/AbsaOSS/spline-openlineage/blob/master/aggregator/src/main/scala/za/co/absa/spline/ol/aggregator/conversion/OpenLineageToSplineConverter.scala | spline-openlineage | Medium | Build (emit write mode, FAIL) |
| Code change should change plan identity | https://github.com/AbsaOSS/spline-python-agent/blob/master/src/spline_agent/harvester.py | spline-python-agent | Medium | Build (code hash in receipt) |
| Record loss or explosion across steps | https://github.com/AbsaOSS/atum/blob/master/README.md#motivation | atum | Very high | Build (measures + verdicts) |
| Numeric control totals that serialise stably | https://github.com/AbsaOSS/atum/blob/master/atum/src/main/scala/za/co/absa/atum/core/MeasurementProcessor.scala | atum | High | Build |
| Renames breaking reconciliation | same file, README routines | atum | High | Build (rename map) |
| Global state in instrumentation | https://github.com/AbsaOSS/atum/issues/28 | atum | Medium (warning) | Ignore pattern |
| Partition identity for backfill | https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/validation/V0.2.0.6__validate_partitioning.sql, https://github.com/AbsaOSS/atum-service/issues/261 | atum-service | Very high | Build |
| Re-run dedup of evidence | https://github.com/AbsaOSS/atum-service/blob/master/database/src/main/postgres/runs/V0.1.0.32__checkpoints.ddl | atum-service | High | Build |
| Querying measures later | https://github.com/AbsaOSS/atum-service/issues/478 | atum-service | Medium | Build (MCP query tools) |
| Central measurement store for many teams | https://github.com/AbsaOSS/atum-service/blob/master/README.md | atum-service | Low now | Plugin (optional sink) |
| Reference-data conformance (code mapping) | https://github.com/AbsaOSS/enceladus/blob/develop/data-model/src/main/scala/za/co/absa/enceladus/model/conformanceRule/package.scala | enceladus | High | Build (declarative mapping step) |
| Point-in-time reference data | https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/scala/za/co/absa/enceladus/conformance/datasource/DataSource.scala | enceladus | High | Build |
| Row-level error channel with source attribution | https://github.com/AbsaOSS/enceladus/blob/gh-pages/_docs/3.0.0/usage/errcol.md | enceladus | High | Build (extend quarantine) |
| Restatement without overwrite | https://github.com/AbsaOSS/enceladus/blob/develop/spark-jobs/src/main/scala/za/co/absa/enceladus/common/CommonJobExecution.scala | enceladus | Very high | Build |
| Empty-output guard | same file, `handleEmptyOutput` | enceladus | High | Build |
| Traceable row identity | https://github.com/AbsaOSS/enceladus/issues/1294 | enceladus | Medium | Build (optional) |
| Governed, versioned dataset metadata | https://github.com/AbsaOSS/enceladus/blob/develop/data-model/src/main/scala/za/co/absa/enceladus/model/versionedModel/VersionedModel.scala | enceladus | Medium | Integrate (catalogue) |
| Migration equivalence testing | https://github.com/AbsaOSS/dataset-comparison/blob/master/README.md | dataset-comparison, hermes | High | Build (conformance kit) |
| Nondeterministic column noise | dataset-comparison README "Removing noise"; hermes reference.conf ignored keys | dataset-comparison, hermes | High | Build (run-twice noise detection) |
| Weak row hashing in diffs | https://github.com/AbsaOSS/dataset-comparison/blob/master/bigfiles/core/src/main/scala/za/co/absa/hash/HashUtils.scala | dataset-comparison | High (warning) | Build correctly |
| Embedding-based table similarity | https://github.com/AbsaOSS/datasets-similarity | datasets-similarity | Low | Ignore |

## Top 10 transferable ideas, ranked by value to Ubunye

1. **Declared control measures with verdicts, stored in the receipt** (Atum + Enceladus). `count, sum, abs_sum, distinct_count, sum_crc32` at `input` and `output` checkpoints, canonical decimal strings, one aggregation pass, plus declared reconciliation rules (`output = input - quarantined - filtered`) evaluated to pass/fail with `strict|warning|none`. This closes Ubunye's biggest gap (input vs output reconciliation) with a design banks already run in production, and it is backend-neutral. Rationale: highest regulatory value, small code, fits the receipt.
2. **Partition identity plus restatement versioning** (Atum Service partitioning, Enceladus `info_date`/`info_version`). Ordered keys, canonical serialisation, `(logical_date, version)` outputs, refuse overwrite by default, never infer versions by listing storage. Rationale: unlocks backfill and resume, both missing.
3. **Temporal "observed writes by read" lineage over receipts** (Spline). For every input, record the receipt of the last successful overwrite before read start plus later successful appends. Rationale: gives exact impact and blast-radius queries through the MCP server without a graph DB.
4. **Content-addressed plan identity and idempotent event keys** (Spline agent, Python agent, Atum Service checkpoint UUID). Plan hash = config + code fingerprint + declared I/O; event key = plan hash + start time; attempt UUIDs generated client side. Rationale: makes replay, retries and double emission harmless; foundation for resume.
5. **Declarative reference-data mapping step with point-in-time tables** (Enceladus mapping rule). Join keys, target, defaults, null safety, table filter; reference table resolved as of the run's logical date; unmapped rows go to the error channel. Rationale: semantic conformance is on the missing list and this is a proven minimal spec.
6. **Structured row error channel with source attribution** (Enceladus errCol). Extend quarantine records to `{type, code, column, source_column, raw_values}`. Rationale: makes quarantine actionable and aggregatable; cheap.
7. **Connector capability vocabulary with a published coverage table** (Spline agent plugin traits and command coverage lists). Rationale: lets `plan`/dry-run fail early ("merge not supported on this connector") and gives a conformance kit something to test against.
8. **Golden-output conformance kit with noise learning** (Hermes, dataset-comparison). Schema check with ignore list, duplicate policy, keyed or keyless diff, 128-bit canonical row hash, control-total diff with ignored keys, and run-twice detection of nondeterministic columns. Rationale: turns "runs anywhere" into "gives the same answer anywhere", which is Ubunye's core claim.
9. **Emission resilience: outbox, fallback and never-fail-the-job** (producer-proxy, fallback and composite dispatchers). Rationale: lineage and telemetry backends go down; the job must not.
10. **Default redaction filter on everything emitted** (agent `dsPasswordReplace`). Name and value regexes applied to URIs, options, receipts and OL facets; custom filters chain onto defaults. Rationale: `secret://` refs protect config, but connection strings built at runtime still leak; Spline learned this from a 22-comment issue.

## Anti-patterns observed

1. **Reverse-engineering an engine's internals for lineage.** The Spark agent's issue list is dominated by Delta, Databricks, window functions, attribute ID collisions and every new Spark or Java version (https://github.com/AbsaOSS/spline-spark-agent/issues/570, /107, /125, /262, /272, /831). Declared I/O avoids the whole class.
2. **Heavy mandatory infrastructure.** ArangoDB (BSL licence, CPU blow-ups, a home-made transaction manager), MongoDB plus Tomcat plus HDFS plus Oozie for Enceladus, Postgres for Atum Service. Getting-started issues are mostly "server unavailable" (https://github.com/AbsaOSS/spline-getting-started/issues/48).
3. **Global mutable state in instrumentation** (Atum bound to the Spark session, https://github.com/AbsaOSS/atum/issues/28), open for six years.
4. **Silent degradation.** Unknown control types become "N/A", dropped columns silently remove their controls (MeasurementProcessor), overriding the root filter silently drops password masking (agent yaml). Reconciliation that quietly stops reconciling is worse than none.
5. **Capture without judgement.** Atum Service explicitly never evaluates measures (README), pushing reconciliation logic into every consumer (https://github.com/AbsaOSS/atum-service/issues/478).
6. **Weak hashing in equality tools.** 32-bit `String.hashCode` with no separators (dataset-comparison HashUtils) and `concat_ws` NULL skipping (Hermes key column) can hide real differences.
7. **Inferring versions from storage listings** under concurrency, flagged by Enceladus itself as experimental and unsafe (CommonJobExecution `getReportVersion`).
8. **Serialisation not pinned.** Enum JSON differs between json4s and Jackson (https://github.com/AbsaOSS/atum/issues/83); partition key order differs between Scala 2.12 and 2.13 (https://github.com/AbsaOSS/atum-service/issues/261). Anything hashed or used as identity must have one canonical encoder with tests.
9. **Long-lived release candidates and satellite PoCs.** Spline 1.0.0 in RC for months; spline-openlineage, producer-proxy and python-agent stalled after first release; Enceladus frozen with 392 open issues. Scope spread across many repos outpaced a small team.
10. **UI-first rule authoring.** Enceladus conformance rules live in Menas/MongoDB, versioned by integer, rather than in reviewable config; model migrations needed a dedicated framework (https://github.com/AbsaOSS/enceladus/issues/311). Ubunye's config-first stance is the better choice; keep rules in version control.

## UNVERIFIED items
- Whether Absa production still runs Enceladus, or has moved standardisation and conformance to another stack. The spark-data-standardization repo is active, but the Enceladus successor is not documented in the repos read.
- "Unify" in https://github.com/AbsaOSS/atum-service/issues/478 appears to be an internal Absa platform consuming Atum Service; its scope is not public.
- Adoption numbers for Spline outside Absa were not measured.
