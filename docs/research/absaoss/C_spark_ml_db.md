# AbsaOSS family C: Spark libraries, data tooling, ML, DB tooling, service tooling

Research date: 2026-09-25. Method: read-only `gh api` over README, source trees, core source files, releases, commits, open issues and the most-commented issues/PRs of each repo. Every claim carries a URL. Items that could not be checked from source are marked UNVERIFIED.

Scope: spark-data-standardization, spark-commons, commons, spark-hats, spark-hofs, spark-functions-ex, spark-partition-sizing, spark-metadata-tool, spark-launcher-supervisor, spot, rialto, mag, fa-db, balta, ultet, login-service, simba-athena-login-service-support, StatusBoard, OTEL-example, scalatest-extras, rest-api-doc-generator, springdoc-openapi-scala, absa-shaded, absa-shaded-jackson-module-scala, spark-hadoop2.

A general observation first: community engagement on these repos is low. The most-commented issue in the whole family has 12 comments (login-service PR #81, https://github.com/AbsaOSS/login-service/pull/81) and reactions are almost always zero. So "most reacted" is not a useful signal here. The evidence is mainly in the code, the READMEs, the changelogs and the open issue backlog.

---

## 1. spark-data-standardization

**Maturity.** Active. Last release v0.5.1 on 2026-09-10, last commit 2026-09-09 fixing "incorrect minimum date conversion" (https://github.com/AbsaOSS/spark-data-standardization/releases, https://github.com/AbsaOSS/spark-data-standardization/issues/94). Spark 3.5.x only per README (https://github.com/AbsaOSS/spark-data-standardization#readme); Spark 4.1 support is an open issue (#122, https://github.com/AbsaOSS/spark-data-standardization/issues/122). 55 test files in the tree. It is consumed by Pramen: the stack trace in issue #86 shows `pramen-components` calling it (https://github.com/AbsaOSS/spark-data-standardization/issues/86).

**Purpose and production problem.** "Dataframe in, Standardized Dataframe out" (README). It is the extracted Standardization stage of Enceladus: raw source data (often strings from CSV, fixed-width, mainframe) must be coerced into a target typed schema without losing rows, with every failed cast recorded per row instead of failing the job.

**Core abstraction and data model.**
- Entry point `Standardization.standardize(df, schema, config)` runs 3 steps: validate the target schema for self-consistency, standardize every field, clean the error column, then optionally add a record id (https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/Standardization.scala).
- The target schema is a Spark `StructType` whose field **metadata** drives parsing. Keys include `sourcecolumn` (rename/mapping from source name), `default`, `timezone`, `pattern`, `decimal_separator`, `grouping_separator`, `minus_sign`, `allow_infinity`, `radix`, `encoding` (base64), `strict_parsing`, plus/minus infinity symbols and values, and `is_non_standard` for mainframe century patterns (https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/schema/MetadataKeys.scala).
- A sealed `TypeParser` hierarchy has one parser per target type: Array, Struct, Integral, Fractional, Decimal, String, Binary, Boolean, Date, Timestamp (https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/stages/TypeParser.scala). Each returns a `ParseOutput(stdCol, errors)`: the typed column plus an array-of-errors column. Nested arrays and structs recurse, so errors inside arrays are collected and flattened.
- **Error column contract.** Every row gets `errCol: array<ErrorMessage>` where `ErrorMessage(errType, errCode, errMsg, errCol, rawValues, mappings)` (https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/ErrorMessage.scala). Four error kinds with stable codes: `stdCastError` E00000, `stdNullError` E00002 (null in non-nullable field), `stdTypeError` E00006 (for example struct to primitive), `stdSchemaError` E00007 (row did not match the reader schema, taken from Spark's corrupt-record column) (https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/StandardizationErrorMessage.scala, https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/config/DefaultErrorCodesConfig.scala). The cast error carries the raw value and, since #58/#59, the source type, pattern and target type (https://github.com/AbsaOSS/spark-data-standardization/issues/58). A failed value becomes the field default (metadata `default`, else a type default) and the error is recorded.
- Pre-existing `errCol` is appended to, not replaced, so errors accumulate across stages (Standardization.scala, `oldErrorColumn`).
- Record id strategy: `uuid`, `stableHashId` (hash of all columns, "all runs will yield the same IDs", intended for tests), or `none` (https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/RecordIdGeneration.scala).
- Schema self-validation happens before touching data: patterns, defaults and metadata are checked and produce `ValidationError` (fatal) or `ValidationWarning` (https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/stages/SchemaChecker.scala, https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/ValidationIssue.scala).

**Type coercion rules visible in code.**
- Integral targets list "overflowable" source types (Byte accepts Short/Int/Long sources with an extra range check); decimals and fractionals are compared back to the original to catch overflow (TypeParser.scala around the `IntegralParser` definition).
- Decimal: "NB! loss of precision is not addressed for any DecimalType e.g. 3.141592 will be Standardized to Decimal(10,2) as 3.14" (https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/stages/TypeParser.scala#L421). Silent rounding is a documented, accepted trade-off.
- Timestamp and date: a documented conversion table per source type (String, numeric epoch, Decimal, Timestamp, Date) with or without a default time zone, using `to_timestamp` then `to_utc_timestamp`. Epoch patterns (seconds, millis, micros, nanos) divide by an `epochFactor`. Numeric sources need a `pattern` or the schema validation fails (TypeParser.scala, DateTimeParser section). Default time zone is UTC (https://github.com/AbsaOSS/spark-data-standardization/blob/master/src/main/scala/za/co/absa/standardization/config/DefaultStandardizationConfig.scala).
- Infinity support: sentinel values such as `99999` or `0` map to configured plus/minus infinity dates. There is an ADR for ISO pattern fallbacks (https://github.com/AbsaOSS/spark-data-standardization/tree/master/src/main/scala/za/co/absa/standardization/adr/001-infinity-support-iso-pattern-defaults).

**Design decisions and trade-offs.**
- `failOnInputNotPerSchema` defaults to false: an impossible cast (struct to int) does not fail the job, it fills the default and logs a type error (DefaultStandardizationConfig.scala, TypeParser `checkSetupForFailure`). Row-level resilience is chosen over fail-fast.
- Error handling is hard-coded to the `ErrorMessage` array. Issue #60 proposes swapping to spark-commons' pluggable `ErrorHandler` (https://github.com/AbsaOSS/spark-data-standardization/issues/60), still open since 2024-09.
- Still imports `za.co.absa.spark.hofs.transform` (TypeParser.scala#L28) although spark-hofs itself says to migrate to native Spark (see section 5).

**Failure modes prevented.** Silent null-on-bad-cast (Spark's non-ANSI `cast` returns null); lost bad rows; invalid schemas discovered only mid-run.

**Failure modes suffered (open or recent issues).** Date/time parsing is the dominant bug class: minimum date off by one day (#94), ISO8601 with microseconds fails (#68, https://github.com/AbsaOSS/spark-data-standardization/issues/68), literal plus fraction patterns (#7, https://github.com/AbsaOSS/spark-data-standardization/issues/7), capitalised month names (#83, https://github.com/AbsaOSS/spark-data-standardization/issues/83), `Task not serializable` when casting to date (#57, https://github.com/AbsaOSS/spark-data-standardization/issues/57), no default for non-nullable arrays (#86).

**2026 coverage.** Spark ANSI mode plus `try_cast`/`try_to_timestamp` give null-or-error semantics but not a per-row error array with raw value and code (https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html). Spark Declarative Pipelines expectations and DLT expectations do row-level drop/warn/fail on predicates, not typed coercion with provenance (https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html). dbt model contracts enforce column types at build time, again without per-row diagnostics (https://docs.getdbt.com/docs/mesh/govern/model-contracts). The metadata-driven, per-field parsing spec with a stable error code catalogue is still fairly unique.

**Lessons for Ubunye.**
- Schema evolution/standardization: adopt a *declarative field spec* (name, source name, type, nullable, default, pattern, tz, decimal symbols, infinity sentinels) and validate the spec before reading data. Ubunye's quarantine then gets a reason code per field.
- Reconciliation and quarantine: the `errCol` contract (type, code, message, column, raw values) is a good quarantine record shape. Keep codes stable and documented.
- Semantic conformance: `sourcecolumn` metadata is a minimal source-to-target mapping; it is the seed of a mapping layer.
- Do NOT copy: silent decimal rounding; defaulting to "never fail" on structurally impossible casts; UDF-heavy implementation that ties you to Spark (Ubunye must also run pandas). Specify the rules, then implement per backend with a shared conformance test suite.

---

## 2. spark-commons

**Maturity.** Active. v1.0.0 released 2026-09-14 with Spark 4 leftovers (https://github.com/AbsaOSS/spark-commons/releases). Published per Spark minor: spark3.4, 3.5, 4.0, 4.1, plus `spark-commons-test` (https://github.com/AbsaOSS/spark-commons#readme).

**Purpose.** Shared routines extracted from Enceladus ("22 enceladus schema utils", https://github.com/AbsaOSS/spark-commons/pull/23). What recurs tells us where the pain is:
- **Nested schema navigation:** `getField(path)`, `getFieldType`, `fieldExists`, `isColumnArrayOfStruct`, `getAllArrayPaths`, `getDeepestCommonArrayPath`, `splitPath`, `col_of_path` for `a.b[0].c` style paths (README).
- **Schema comparison:** `isEquivalent`, `isSubset`, `diffSchema`, `alignSchema`, `getDataFrameSelector` (reorder nested columns so row hashing and `except` work) (https://github.com/AbsaOSS/spark-commons/blob/master/spark-commons/src/main/scala/za/co/absa/spark/commons/implicits/StructTypeImplicits.scala).
- **Spark version portability:** `TransformAdapter`, `CallUdfAdapter` with separate source trees `scala-spark3-jvm` and `scala-spark4-jvm`, and `SparkVersionGuard` to fail fast on an unsupported runtime (source tree https://github.com/AbsaOSS/spark-commons/tree/master/spark-commons/src/main).
- **Listener safety:** `NonFatalQueryExecutionListenerAdapter` stops a listener from receiving fatal exceptions (links commons#50).
- **Error handler abstraction** (see below).
- **Null typing:** `enforceTypeOnNullTypeFields` (https://github.com/AbsaOSS/spark-commons/pull/81) because empty inputs infer `NullType` and break unions.

**Core abstraction: `ErrorHandler`.** A trait that libraries accept and applications implement, so the library does not decide how errors are handled (https://github.com/AbsaOSS/spark-commons/blob/master/spark-commons/src/main/scala/za/co/absa/spark/commons/errorhandler/ErrorHandler.scala, spike rationale https://github.com/AbsaOSS/spark-commons/issues/83). Methods: `putError(df)(when)(submit)`, `putErrorsWithGrouping` (per column, first matching condition wins, compiled to one `CaseWhen`), `createErrorAsColumn`, `applyErrorColumnsToDataFrame`, and `dataFrameColumnType` so callers know the shape (https://github.com/AbsaOSS/spark-commons/issues/91). Four implementations: collect into array column, filter error rows, ignore, throw (README). The newer `ErrorMessage` has `errColsAndValues: Map` plus JSON `additionInfo` (https://github.com/AbsaOSS/spark-commons/blob/master/spark-commons/src/main/scala/za/co/absa/spark/commons/errorhandler/ErrorMessage.scala). Open: log-file and single-string implementations (#86, #87).

**Design decisions visible.** `diffSchema` is one-directional and case-insensitive: it walks fields of the left schema and reports "cannot be found in both schemas"; a field present only on the right is not reported unless the caller diffs both ways (StructTypeImplicits.scala#L226). `isEquivalent` ignores nullability and order. These are useful helpers, not a compatibility model.

**2026 coverage.** Spark 3.1+ `Column.withField`/`dropFields` covers much nested editing natively (https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.withField.html). Delta and Iceberg define schema evolution rules at the table layer (https://iceberg.apache.org/docs/latest/evolution/). Still unique: the pluggable error-channel contract between a library and its host app.

**Lessons for Ubunye.**
- Schema diff/compatibility: build a *symmetric*, typed diff (added, removed, type widened, type narrowed, nullability tightened, reordered) with a compatibility verdict, not a list of strings. spark-commons shows the need and the gap.
- Connectors/backends: the adapter-per-runtime-version pattern plus a version guard is exactly what a portable engine needs; Ubunye should publish a runtime capability check at start.
- Expectations/quarantine: copy the `ErrorHandler` idea as a port: tasks emit error columns; the policy (quarantine table, drop, fail, count only) is chosen by config.

---

## 3. commons (Scala, not Spark)

**Maturity.** Active; release 2.0.7 on 2026-09-22 via Maven release plugin (https://github.com/AbsaOSS/commons/commits/master).

**What it holds.** Language extensions, topological sort for DAGs, typed configuration (`ConfTyped`), `UpperSnakeCaseEnvironmentConfiguration` mapping `foo.bar.baz` to `FOO_BAR_BAZ` (https://github.com/AbsaOSS/commons#upper-snake-case-environment-configuration, issue https://github.com/AbsaOSS/commons/issues/54), reflection utils, build-info, semantic version parsing, temp files, S3 location helpers including trailing-slash normalisation (https://github.com/AbsaOSS/commons/pull/130), JSON SerDe, and **ErrorRef**: log the full exception server-side with a UUID and return only the UUID, time and a safe message to the client (https://github.com/AbsaOSS/commons#clientserver-error-cross-linking).

**Lessons for Ubunye.**
- Governance/security: ErrorRef is a cheap pattern for the MCP server and any HTTP surface: never return stack traces or config to a caller, return a correlation id that is also on the OTel span.
- Config: env-var key mapping is exactly what `secret://` plus env overrides need; document one canonical mapping.
- Schema diff lesson again: commons PR #9 "Add schema utils for comparison and diff" (https://github.com/AbsaOSS/commons/pull/9) shows the diff need appears even in non-Spark code.

---

## 4. spark-hats

**Maturity.** Dormant. Last release 0.3.0 on 2023-08-04, no commits since (https://github.com/AbsaOSS/spark-hats/releases). 38 stars, the most in this family.

**Purpose.** "Helpers for Array Transformations": add, drop, map fields inside arrays of structs at any nesting depth: `nestedWithColumn`, `nestedWithColumnExtended` (reference fields across nesting levels via a `getField` callback), `nestedDropColumn`, `nestedMapColumn`, `nestedUnstruct` (https://github.com/AbsaOSS/spark-hats#readme). Motivation example: adding `c = a + 1` to `my_array[*]` needs a hand-written `transform(... struct(...))` rebuild; with hats it is one call.

**Failure mode suffered.** Issue #41: several chained `nestedMapColumn` calls made the DataFrame "hang" on `.show()`. Maintainer answer: each mapping creates a big projection and Catalyst's traversal is super-linear; SPARK-28090, reportedly fixed in 3.4.0; workaround is a dummy cross join as an "optimization barrier", as used in Enceladus `OptimizerTimeTracker` (https://github.com/AbsaOSS/spark-hats/issues/41). Open backlog includes nested Map support and schema projection (#15, #19).

**2026 coverage.** Spark 3.1+ `withField`/`dropFields` and 3.x higher-order functions cover most cases natively. Nested maps and "reference parent field from inside array" remain awkward in native APIs (UNVERIFIED for Spark 4.1 specifics).

**Lessons for Ubunye.** Performance: plan size is a real cost. A config-first engine that generates many column expressions must detect plan blow-up (track analysis/optimisation time per task and warn). Do NOT copy the cross-join barrier hack; prefer `localCheckpoint` or staged writes, and measure.

---

## 5. spark-hofs

**Maturity.** Deprecated by its own README: "Starting from Spark 3.2.1 the high-order functions are available in the Scala API natively ... we recommend migrating" (https://github.com/AbsaOSS/spark-hofs#readme). Last release 0.5.0, 2023-08-04.

**Purpose.** Typed Scala API for `transform`, `filter`, `aggregate`, `zip_with` etc. when Spark 2.4 only had them in SQL text, plus control of lambda variable names in plans.

**Lesson.** A library can end its own life gracefully by pointing at the native API. The anti-pattern is downstream: spark-data-standardization still imports it (TypeParser.scala#L28). Ubunye should keep a dependency audit that flags "superseded by runtime" dependencies.

---

## 6. spark-functions-ex

**Maturity.** Empty. Only a README, license and templates; one commit "First basic files" on 2021-08-30 (https://github.com/AbsaOSS/spark-functions-ex/commits/master). README: "very prerelease version". Nothing to learn beyond: placeholder repos add catalogue noise. Ignore.

---

## 7. spark-partition-sizing

**Maturity.** Low activity. Releases 0.1.0 and 0.2.0 both on 2023-02-24; only dependabot commits since (https://github.com/AbsaOSS/spark-partition-sizing/releases). Artifacts for Spark 2.4, 3.2, 3.3 only (README). Five open issues from 2021 (skew suppression #13, use max not average #14, `ByteSize = Long` #15) (https://github.com/AbsaOSS/spark-partition-sizing/issues).

**Purpose.** Control output partition size when writing, because skewed or tiny partitions hurt later reads (README "Motivation").

**Algorithms (from code, https://github.com/AbsaOSS/spark-partition-sizing/blob/master/spark-partition-sizing/src/main/scala/za/co/absa/spark/partition/sizing/DataFramePartitioner.scala).**
- `repartitionByRecordCount(n)`: count rows per partition, then `repartition(ceil(total/n))`. Has a TODO "verify max of each partition, it might still break the limit" (line 45), because `repartition` is round robin and average based.
- `repartitionByPlanSize(min, max)`: caches the DataFrame, reads `optimizedPlan.stats.sizeInBytes`, divides by current partition count, then `coalesce` (too small) or `repartition` (too big).
- `repartitionByDesiredSize(sizer)`: pluggable `RecordSizer`: `FromDataframeSizer` (sum of every row's estimated size, accurate, slow), `FromSchemaSizer` (schema times typical type sizes, fast, inaccurate), `FromSchemaWithSummariesSizer` (weights by null ratio, flat schemas only), `FromDataframeSampleSizer` (random sample) (README).

**Does AQE supersede it?** Partly. AQE coalesces post-shuffle partitions toward `spark.sql.adaptive.advisoryPartitionSizeInBytes` and splits skewed joins (https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution). Spot's own analysis showed enabling Adaptive Execution fixed the "200 small files" problem for Enceladus (https://github.com/AbsaOSS/spot#example-small-files-issue). AQE does not target file size for writes without a shuffle; table formats now do that (Delta optimized writes and auto compaction, https://docs.databricks.com/aws/en/delta/tune-file-size; Iceberg write distribution and target file size, https://iceberg.apache.org/docs/latest/configuration/#write-properties). So the library's niche is mostly gone in 2026.

**Lessons for Ubunye.** Performance/cost: offer a declarative `target_file_size` hint and let the backend map it (AQE advisory size, Delta/Iceberg write props, pandas row groups). Record chosen partition count and bytes written in the run receipt. Do NOT copy: forcing `cache()` and extra `count()` passes just to size output; that doubles cost on large data.

---

## 8. spark-metadata-tool

**Maturity.** Dormant. v0.3.0 on 2023-04-04, last commit 2023-05-11, open issue "S3a support" (#61) (https://github.com/AbsaOSS/spark-metadata-tool/releases, https://github.com/AbsaOSS/spark-metadata-tool/issues/61).

**Purpose and production problem.** "Spark Structured Streaming references data files using absolute paths, which makes it impossible to move the data to a different location without breaking functionality" (https://github.com/AbsaOSS/spark-metadata-tool#motivation). This bit them during the HDFS to S3 migration: example `hdfs://old/path/...` to `s3://bucket/new_root/...`.

**Modes.** `fix-paths` (rewrite base path; "no checks are performed" that files exist), `merge` (merge two `_spark_metadata` logs respecting `.compact` files), `compare-metadata-with-data` (read-only; log inconsistencies between the log and files on disk, added in https://github.com/AbsaOSS/spark-metadata-tool/pull/50), `create-metadata` (rebuild a log from files, aligning to a max micro-batch number and compaction interval, https://github.com/AbsaOSS/spark-metadata-tool/pull/56). Universal: backup before change, deleted after success unless `--keep-backup`, `--dry-run`. File systems: S3, Unix, HDFS behind a `FileManager` trait. Typed error ADT (`IoError`, `ParsingError`, `NotFoundError`, `UnknownFileSystemError`...) (https://github.com/AbsaOSS/spark-metadata-tool/blob/develop/src/main/scala/za/co/absa/spark_metadata_tool/model/AppError.scala).

**2026 coverage.** Table formats (Delta, Iceberg) store paths relative to the table root, and cloud checkpoint relocation is still a known pain (UNVERIFIED for Spark 4.1 file sink behaviour). Spark 4.0 added a state data source for reading checkpoint state (https://spark.apache.org/docs/latest/streaming/structured-streaming-state-data-source.html).

**Lessons for Ubunye.**
- Recovery/resume: any state Ubunye persists (run receipts, replay store, checkpoints) must be **location independent**: store paths relative to a declared root plus the root separately.
- Reconciliation: `compare-metadata-with-data` is a model for a `ubunye verify` command: compare what the receipt says was written with what is actually on storage.
- Governance: backup-then-mutate with `--dry-run` is the right default for any repair command. Copy it.

---

## 9. spark-launcher-supervisor

**Maturity.** Proof of concept. No releases, last commit 2023-05-21, 0 stars (https://github.com/AbsaOSS/spark-launcher-supervisor).

**Purpose.** A JVM agent installed globally on edge nodes via `JAVA_TOOL_OPTIONS` that intercepts the `SparkContext` constructor (Byte Buddy advice) and checks settings. Current rules are hard-coded: `spark.master=yarn`, `spark.submit.deployMode=cluster`; it only prints warnings (https://github.com/AbsaOSS/spark-launcher-supervisor/blob/master/agent/src/main/java/za/co/absa/sparlus/SparkConfValidator.java#L14). README TODO lists: rules from config or central service, notifications, enforce mode, shading, RPM.

**Anti-pattern found.** The validator prints the **entire** Spark configuration to the console (`info("Spark Conf:" + ...)`, SparkConfValidator.java#L27). Spark confs routinely contain credentials (for example S3A keys); Spark itself redacts with `spark.redaction.regex` (https://spark.apache.org/docs/latest/configuration.html). A governance tool that leaks secrets is worse than none.

**Lessons for Ubunye.** Governance: policy-as-config over the resolved run config ("required", "forbidden", "range") checked at plan time, with warn vs enforce modes, is valuable and cheap for a config-first engine. Always log the policy verdict, never the raw config. Redact with the same rules as `secret://`.

---

## 10. spot

**Maturity.** Stalled. One release v0.0.1 (2024-02-12), last commit 2023-12-03, 20 open issues, open "Fix memory leakage" (#86) and "Add parallelism" (#85) from 2024 (https://github.com/AbsaOSS/spot/issues). Deployment asks for Python 3.7.16 (README).

**Purpose.** "Continuously apply statistical analysis on repeating (production) runs of the same applications" so you can compare time, cluster load and cost across code versions and configurations, without instrumenting the apps: it reads the Spark History Server REST API (https://github.com/AbsaOSS/spot#what-is-spot).

**Model.** Crawler merges attempts, executors and stages into one raw JSON doc per run, computes aggregations (min/max/mean/non-zero per array), derived values (total CPU allocation, efficiency, speedup based on Amdahl-style sequential vs parallel part), and stores raw, aggregate and error docs in Elasticsearch/OpenSearch indices with Kibana dashboards and alerts, including a "no new runs" alert (README; tree https://github.com/AbsaOSS/spot/tree/develop/spot/kibana/alerting). Regression and Setter modules (predict and set configs) are "Future" (README, issue #5).

**What they learned with it.** Dynamic allocation stabilised duration across varying input sizes; AQE fixed small files; idle executors showed over-allocation (README monitoring examples). This is real evidence that run-to-run comparison drives config decisions.

**Design decisions and weaknesses.**
- Run identity is parsed from the Spark **app name string**: `Enceladus <type> <app_version> <dataset> <dataset_version> <info_date> <info_version>`, with a separate parser for an old naming scheme (https://github.com/AbsaOSS/spot/blob/develop/spot/enceladus/classification.py). Issue #45 admits matching attempts to runs "relies on order of attempts and runs, which is not sufficient" and asks for an attempt id in Enceladus metadata (https://github.com/AbsaOSS/spot/issues/45).
- Sensitive Spark properties are dropped with a hand-maintained deny list (keytab, principal, extraJavaOptions...) (https://github.com/AbsaOSS/spot/blob/develop/spot/crawler/aggregator.py#L41). Deny lists miss new secret keys.
- Date without time is stored as noon UTC to survive time zone shifts in Elasticsearch/Kibana (classification.py `parse_info_date`).

**2026 coverage.** OpenTelemetry metrics, Databricks system tables and FOCUS cost exports cover cost/usage collection (https://focus.finops.org/). Spark event logs and listeners are still the only source of stage/task detail. Cross-version *regression detection per logical job* remains rare off the shelf (UNVERIFIED claim about market gap).

**Lessons for Ubunye (strong).**
- Performance/cost regression: Ubunye's run receipt already has content hashes and code version; add a stable `job_key` + `code_version` + `config_hash` + `input_bytes` + duration + core-seconds, and compare runs of the same job_key. That is Spot's whole idea, done without string parsing.
- Lineage/catalogue: put identity in structured fields (OpenLineage job namespace/name, run id, attempt id), never in a display name. Spot #45 is the cautionary tale.
- Security: allow-list what is exported from runtime config, do not deny-list.

---

## 11. rialto (deep read)

**Maturity.** Active. Version 2.2.2 in pyproject, CHANGELOG from 2.0.6 (2025-02) to 2.2.1 (2026-05-31); last merge 2026-08-25 "multi schema config fix"; PyPI package `rialto`, Python >= 3.11, PySpark 4.0, delta-spark 4 (https://github.com/AbsaOSS/rialto/blob/develop/pyproject.toml, https://github.com/AbsaOSS/rialto/blob/develop/CHANGELOG.md). No GitHub releases. Internal ticket prefixes RLG- and MLO- in PR titles suggest an internal MLOps team (https://github.com/AbsaOSS/rialto/pull/59).

**Purpose.** "A framework for building and deploying machine learning features in a scalable and reusable way", a feature marketplace (https://github.com/AbsaOSS/rialto#readme). It is Databricks/Unity Catalog centred (three-part table names, Delta).

**Core abstractions (code).**
- **Runner** = `RunnerEngine` over `RunnerServices` (config, date manager, registry, checker, executor, writer, tracker) (https://github.com/AbsaOSS/rialto/blob/develop/rialto/runner/runner.py, https://github.com/AbsaOSS/rialto/blob/develop/rialto/runner/engine.py). Flow: `register_tasks` (every scheduled date in a watch window per pipeline), `check_tasks` (completion + dependencies), `run_tasks`, `finalize` (mail + status log). Also `dry_run()` and `_debug()` (run first task, return the DataFrame, write nothing).
- **Schedule and dates.** `DateManager` builds the window `[run_date - watched_period, run_date]`, selects execution dates by frequency (daily, weekly `isoweekday`, monthly day or `last`), and derives a **partition date** by applying `info_date_shift` (https://github.com/AbsaOSS/rialto/blob/develop/rialto/runner/services/date_manager.py). This separation of *execution date* and *information date* is the key temporal idea.
- **Backfill.** Automatic: every missing scheduled date inside the window is a task. `rerun=True` forces all. `op` selects one pipeline. Overrides use a small path language (`pipelines[name=X].target.target_schema`, list index, append with -1) (README "Configuration").
- **Completion check** = the target partition has rows: `data_exists = df.count() > 0` (https://github.com/AbsaOSS/rialto/blob/develop/rialto/runner/services/data_checker.py#L57). With secondary partitions and no filters it forces rerun with a warning.
- **Dependency freshness** = for each dependency table, data exists in `[run_date - interval, run_date]` on its `date_col`, with optional column filters (README "Dependency Tracking", TaskStatusChecker https://github.com/AbsaOSS/rialto/blob/develop/rialto/runner/services/task_status_checker.py). Missing data fails that task/date only; others continue.
- **Write** = Delta `overwrite` with `replaceWhere` on the partition date (and secondary partition values), after aligning column order to the existing table and refusing if existing columns are missing (https://github.com/AbsaOSS/rialto/blob/develop/rialto/runner/services/writer.py). This makes a date rerun idempotent.
- **Enrichment**: `INFORMATION_DATE` and `VERSION` (the Python package version of the job) columns are added (README; `_add_job_version` in https://github.com/AbsaOSS/rialto/blob/develop/rialto/jobs/job_base.py).
- **Post-write reconciliation**: rows are counted back from storage for that partition (`check_written`); CHANGELOG 2.0.9 "runner no longer checks number of generated records before attempting to write them, instead it retrieves the information from storage".
- **Bookkeeping**: a `Record(job, target, date, time, records, status, reason, exception, run_timestamp)` appended to a Delta table (https://github.com/AbsaOSS/rialto/blob/develop/rialto/runner/reporting/record.py, https://github.com/AbsaOSS/rialto/blob/develop/rialto/runner/reporting/bookkeeper.py). CHANGELOG 2.0.7: "bookkeeping table is no longer being overwritten, new records are appended instead", meaning run history used to be destroyed.
- **Jobs registry**: `@datasource` and `@job` decorators; dependencies are injected by parameter name (`run_date`, `spark`, `config`, `table_reader`, `feature_loader`, `metadata_manager`, `job_metadata`, other datasources). `resolver_resolves()` test helper detects undefined and circular dependencies; `disable_job_decorators` makes jobs plain functions for unit tests (README "jobs", https://github.com/AbsaOSS/rialto/blob/develop/rialto/jobs/decorators.py). A job that returns nothing gets a one-row placeholder DataFrame "which will ensure that rialto notices that the job ran successfully" (README).
- **Maker**: `@feature(ValueType)`, `@desc`, `@param` (cartesian expansion into named features like `NUM_PRODUCT_A_STATUS_ACTIVE`), `@depends` (ordering). `FeatureMaker.make` (withColumn per feature) and `make_aggregated` (groupBy key agg). Returns features plus metadata (README "maker").
- **Metadata manager**: two Delta tables `group_metadata` and `feature_metadata`, upserted with MERGE on group/feature name (https://github.com/AbsaOSS/rialto/blob/develop/rialto/metadata/metadata_manager.py). `GroupMetadata(name, frequency, description, key, owner, fs_name, features)`, `FeatureMetadata(value_type, name, description, group)` (README).
- **Loader**: `PysparkFeatureLoader.get_feature/get_group/get_features_from_cfg(information_date)`; YAML config with `selection` (groups, prefix, features), `base` (key population) and `maps` (bridge tables between key spaces, for example ID number to account number) (README "loader").
- **TableReader.get_latest(table, date_column, date_until)**: max date `<= date_until`, then that partition (https://github.com/AbsaOSS/rialto/blob/develop/rialto/common/table_reader.py).

**Point-in-time guarantee: how strong is it?** The README promises "Run computations as they would have occurred on a specific date by setting run_date, ensuring data newer than that date is never used". In code, the runner passes `run_date` to the job, but `TableReader` is not bound to it; the guarantee holds only if the job author calls `get_latest(date_until=run_date)` or `get_table(date_to=run_date)` (table_reader.py; job_base.py `_get_resolver` registers a plain `TableReader`). So it is a convention, not an enforced property. Also, time travel is by *partition date*, not by table version, so late corrections written into an old partition change "history" (inference from writer.py `replaceWhere`; no Delta version pinning found in the code).

**Bugs and weaknesses found in code.**
1. `run_timestamp: datetime.timestamp = datetime.now()` is a dataclass default evaluated once at import, and `TaskResultMapper` never passes it (record.py#L44, https://github.com/AbsaOSS/rialto/blob/develop/rialto/runner/services/result_mapper.py). Every bookkeeping row written by one process gets the same timestamp, the module import time. Python docs say to use `default_factory` (https://docs.python.org/3/library/dataclasses.html#default-factory-functions).
2. Completion = "rows exist". A partial write that left rows is "complete"; a legitimately empty output is "incomplete" forever, which is why the one-row placeholder exists (data_checker.py#L57, README jobs notes).
3. `replaceWhere` is built by string formatting, `f"{key} = '{value}'"` (writer.py#L103), so values with quotes break it; there is no escaping.
4. Feature metadata is last-write-wins MERGE with no version or valid-from, so the loader cannot say what a feature *meant* on a past information date (metadata_manager.py). Only the `VERSION` data column links rows to package version.
5. Test-only package `pytest-mock` and the `pathlib` backport are runtime dependencies (pyproject.toml).

**2026 coverage.** Feature stores (Feast, Databricks Feature Engineering) provide point-in-time joins against event timestamps as an enforced operation (https://docs.feast.dev/getting-started/concepts/point-in-time-joins, https://docs.databricks.com/aws/en/machine-learning/feature-store/time-series). Orchestrators (Airflow, Dagster partitions) provide schedule backfills and partition status (https://docs.dagster.io/guides/build/partitions-and-backfills). Rialto's distinctive parts: the execution-date vs information-date split with shift rules, dependency freshness windows per input, "fill every missing scheduled date in the window" self-healing, and decorator DI with a resolvability test.

**Lessons for Ubunye (strongest in this family).**
- Backfill/partition: adopt `execution_date` and `data_date` (information date) as first-class run parameters, a `schedule` (frequency, day, shift) and a `watch_window`. `ubunye plan --window` lists missing partitions; `ubunye run --backfill` fills them. Idempotent per partition via replace-where or MERGE.
- Recovery/resume: completion must come from the **run receipt** (status=success, partition key, row count, content hash), not from "rows exist". Rialto shows why.
- Reconciliation: read back written row counts per partition and put them in the receipt; compare with input counts and quarantine counts (in = out + quarantined + filtered).
- Freshness: declare per-input `freshness: {date_col, max_lag}`; fail that partition only, keep going for others; emit it as an OpenLineage input facet.
- ML/feature provenance: stamp `data_date`, `code_version`, `config_hash` on outputs; version feature definitions (valid_from) instead of MERGE overwrite.
- Point-in-time: *enforce* it. Give tasks a reader that is already bound to `data_date` (and optionally a table version), so reading the future needs an explicit opt-out that is recorded in the receipt.
- Testing: a "resolves" test for the task graph (unknown input, cycle) belongs in Ubunye's conformance kit.
- Do NOT copy: import-time timestamps, string-built SQL predicates, placeholder rows to signal success, Databricks-only assumptions.

---

## 12. mag, fa-db, balta (the DB-as-API stack)

**mag.** "Common relational database utilities and abstractions": table, column, SQL expression, naming conventions; a small stable base so fa-db, balta and ultet do not import each other (https://github.com/AbsaOSS/mag#readme). No releases, dependabot-only commits (https://github.com/AbsaOSS/mag/commits/master). Evidence of the duplication it fixes: balta ships its own copy under package `db/mag/naming` and fa-db has `db/fadb/naming` with the same classes (trees https://github.com/AbsaOSS/balta/tree/master/balta/src/main/scala/za/co/absa/db, https://github.com/AbsaOSS/fa-db/tree/master/core/src/main/scala/za/co/absa/db/fadb/naming).

**fa-db (Functional Access to DB).** v0.7.0 on 2025-12-19 (https://github.com/AbsaOSS/fa-db/releases). Idea: the app reads and writes **only through database functions** (Postgres stored functions), giving "a stable contract between the DB and the application", "early locking of the data model", and security, because the app role can be granted only EXECUTE (https://github.com/AbsaOSS/fa-db#what-is-fa-db). Modules for Slick (Futures) and Doobie (any cats-effect `Async`, ZIO example). Class names are converted to function names via naming conventions (snake_case). Function kinds: single, multiple, optional result, each "WithStatus", and "WithAggStatus" with aggregators (first error, first row, majority errors) (README).
- **Status code contract** (https://github.com/AbsaOSS/fa-db/blob/master/core/src/main/scala/za/co/absa/db/fadb/status/README.md): every function returns `status` and `status_text`; 10-19 OK (11 created, 12 updated, 14 no-op, 15 deleted), 20-29 server misconfiguration, 30-39 data conflict, 40-49 not found, 50-89 data errors (60s missing value, 70s out of range, 80s bad JSON/XML content), 90-99 free. `StandardStatusHandling` maps the tens digit to typed exceptions (https://github.com/AbsaOSS/fa-db/blob/master/core/src/main/scala/za/co/absa/db/fadb/status/handling/implementations/StandardStatusHandling.scala).
- Open backlog: multiple function calls in one transaction (#159, https://github.com/AbsaOSS/fa-db/issues/159), streaming results (#107, PR #108 open), exposing Doobie `ConnectionIO` (#110), a WIP refactor proposal (#117). Most discussed PRs are core refactors and MonadError (https://github.com/AbsaOSS/fa-db/pull/36, https://github.com/AbsaOSS/fa-db/pull/113). Used by atum-service (balta's code was moved out of atum-service, https://github.com/AbsaOSS/balta/pull/10).

**balta.** v0.3.0 on 2024-10-24 (https://github.com/AbsaOSS/balta/releases). ScalaTest-based testing of Postgres functions: each test runs in a transaction that is rolled back, so tests are repeatable and isolated; `DBTable` insert/query helpers, `DBFunction` call and `verify` (https://github.com/AbsaOSS/balta#readme). Known gaps: time zone aware arrays, product conversion of nested types, case sensitivity (#1), parallel tests (#100), stub functions (#19) (https://github.com/AbsaOSS/balta/issues).

**2026 coverage.** pgTAP covers in-DB unit tests (https://pgtap.org/); Testcontainers covers disposable DBs (https://testcontainers.com/). The "status code contract for DB functions" is an Absa convention, close in spirit to HTTP status classes.

**Lessons for Ubunye.**
- Connectors: for JDBC sinks, support "write via a named procedure/function" as a connector capability, not only table insert. It is how some banks lock down writes.
- Status vocabulary: fa-db's two-digit classes are a ready model for Ubunye's task outcome codes (ok, no-op, conflict, not found, data error, config error), mapped onto receipt status and OTel span status.
- Testing: transaction-per-test with rollback is a good pattern for Ubunye's SQL connector conformance tests.
- Do NOT copy: three repos duplicating the same naming code before a shared base existed. Keep one small core package from day one.

---

## 13. ultet (state-based DB deployment)

**Maturity.** Pre-release. No releases, README is only build instructions, backlog of basics (multi-database #8, user creation #9, "allow certain data type changes" #37, warnings #31, check constraints #49) (https://github.com/AbsaOSS/ultet/issues).

**Idea.** "Instead of migrations, it uses a state-based approach" (repo description). Desired state is files: tables as YAML (columns, types, not null, description, default, primary key, indexes, owner), functions as `.sql` with header comments declaring owner and database/grants (https://github.com/AbsaOSS/ultet/blob/master/examples/database/src/main/my_schema/my_table.yaml, https://github.com/AbsaOSS/ultet/blob/master/examples/database/src/main/my_schema/public_function.sql). Actual state is read from Postgres catalogs (`pg_table_columns.sql` etc.). `TableDef - existing` yields `TableInsert` or `TableAlter`, which emits SQL entries grouped into ordered `TransactionGroup`s; `--dry-run` prints the SQL (https://github.com/AbsaOSS/ultet/blob/master/src/main/scala/za/co/absa/ultet/Ultet.scala).

**Safety rules in design.** Data type change throws (TODO #37), NULL to NOT NULL throws, NOT NULL to NULL is allowed (https://github.com/AbsaOSS/ultet/blob/master/src/main/scala/za/co/absa/ultet/model/table/TableDef.scala#L130).

**Bugs found (the diff does not work as written).** In `TableAlter`:
- column diff compares the original table with itself: `ColumnsDifferenceResolver(...)(origTable.columns, origTable.columns)` (https://github.com/AbsaOSS/ultet/blob/master/src/main/scala/za/co/absa/ultet/model/table/TableAlter.scala#L55);
- primary key compares `(origTable.primaryKey, origTable.primaryKey)` (#L43);
- indexes to add are `origTable.indexes.diff(origTable.indexes)`, always empty (#L34), and indexes to remove are computed the wrong way round (#L31);
- `generateAlterForDataTypeChange` throws for **every** common column, even when types are equal (TableDef.scala#L130-L132).
So any alteration of an existing table with columns would throw, and new columns, index or PK changes would not be emitted. This is an unfinished tool.

**2026 coverage.** Atlas declarative "schema apply" and sqldef do state-based diffing with lint and safety policies (https://atlasgo.io/declarative/apply). Nothing unique remains except the function-file header convention.

**Lessons for Ubunye.** Schema evolution: "desired schema in config, diff against actual, classify each change as safe/unsafe, dry-run first, apply in ordered groups" is the right model for Ubunye's output tables. The ultet bugs show you need property tests for the differ itself (diff(a,a) is empty; apply(diff(a,b), a) == b). Integrate with a table format's evolution rules rather than hand-rolling DDL diffing.

---

## 14. login-service and simba-athena-login-service-support

**login-service.** Active: v2.4.2 on 2026-07-13, commits 2026-09 (https://github.com/AbsaOSS/login-service/releases). A JWT issuer: `/token/generate` (access + refresh), `/token/refresh`, `/token/public-key`, `/token/public-keys`, `/token/public-key-jwks`; integrators verify signature, expiry and `type=access` themselves (https://github.com/AbsaOSS/login-service#readme). Auth providers are ordered and pluggable: config users, Active Directory LDAP (with SPNEGO/Kerberos and linear-backoff retries), MS Entra (validates Entra JWT against Microsoft JWKS then issues its own JWT). LDAP service account can come from AWS Secrets Manager or SSM Parameter Store (https://github.com/AbsaOSS/login-service/pull/77). **Key rotation** with `key-rotation-time`, `key-lay-over-time` (new key published before it signs) and `key-phase-out-time` (old key removed later), or keys polled from Secrets Manager (README "Key Provider"). A client library exists (https://github.com/AbsaOSS/login-service/pull/81, the most-discussed item in this family). Recent fix: NPE when service accounts lack `memberOf` (commit 2026-06-24). Heritage: "heavily inspired by Enceladus" (README).

**simba-athena-login-service-support.** A credentials provider for the Simba Athena JDBC driver: user gets an LS JWT, exchanges it at a "JWT2Token" endpoint for temporary AWS credentials, so DBeaver users query Athena without static keys (https://github.com/AbsaOSS/simba-athena-login-service-support#readme). v0.3.0 on 2026-01-08. It documents DBeaver version breakage (`${password}` token gone after 23.1) and a fallback property scheme.

**2026 coverage.** OIDC providers and workload identity federation cover most of this (AWS `AssumeRoleWithWebIdentity`, https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRoleWithWebIdentity.html). The lay-over/phase-out rotation timing is a nicely specified, reusable pattern.

**Lessons for Ubunye.** Governance/security: `secret://` refs should support short-lived credentials from an identity exchange (JWT to cloud creds), not only static secrets. MCP server auth: accept JWTs verified by JWKS with key rotation; never mint long-lived tokens. Record the identity (subject, not token) in the run receipt.

---

## 15. StatusBoard

**Maturity.** Active on security hygiene: Angular 21 upgrade for CVEs, AquaSec SBOM fixes, non-root containers (https://github.com/AbsaOSS/StatusBoard/commits/master, https://github.com/AbsaOSS/StatusBoard/pull/28). No releases.

**What it monitors.** "CPS Status Board ... real-time monitoring platform built specifically for ABSA" (README, https://github.com/AbsaOSS/StatusBoard#readme). Checkers: HTTP status-code-only, HTTP with JSON status, HTTP with message, AWS EMR, AWS RDS and RDS cluster, **EC2 AMI compliance**, composition (aggregate of other services), fixed and temporary fixed status (maintenance) (source tree https://github.com/AbsaOSS/StatusBoard/tree/master/src/main/scala/za/co/absa/statusboard/checker). Persistence in DynamoDB; notifications to email and MS Teams.

**Model.** `RawStatus` = RED/AMBER/GREEN/BLACK(not monitored) with message and an `intermittent` flag (https://github.com/AbsaOSS/StatusBoard/blob/master/src/main/scala/za/co/absa/statusboard/model/RawStatus.scala); `RefinedStatus` adds `firstSeen`, `lastSeen`, `notificationSent`, maintenance message (https://github.com/AbsaOSS/StatusBoard/blob/master/src/main/scala/za/co/absa/statusboard/model/RefinedStatus.scala). The notification decider suppresses intermittent failures until they persist for `secondsInState`, and does not re-notify if the last notified colour is the same (https://github.com/AbsaOSS/StatusBoard/blob/master/src/main/scala/za/co/absa/statusboard/notification/deciders/DurationBasedNotificationDeciderImpl.scala). Recovery ("green") notifications were added deliberately (https://github.com/AbsaOSS/StatusBoard/pull/5). Oddity: it requires AWS env vars even when no AWS checks are used, README says set them to "ignored" (README "Dependencies").

**Lessons for Ubunye.** Freshness and SLA alerts: a colour + intermittent + first/last seen model with "notify on change of colour, after N seconds" is a compact alerting contract for pipeline health (freshness late = amber, failed = red). The Spot "no new runs" alert and this decider together make a minimal reliability layer. Do NOT copy: making an optional cloud a hard startup dependency.

---

## 16. OTEL-example

**Maturity.** One commit "Add apps", 2025-02-25 (https://github.com/AbsaOSS/OTEL-example/commits/master).

**How Absa uses OpenTelemetry (evidence limited to this repo).** A Python FastAPI service with OTel SDK, `BatchSpanProcessor`, OTLP gRPC exporter, FastAPI and requests auto-instrumentation, manual spans, span attributes, error status (https://github.com/AbsaOSS/OTEL-example/blob/master/python_otel/app.py); a Scala Spring Boot app run with the OpenTelemetry **Java agent** (`-javaagent`, `otel.service.name`, OTLP http/protobuf) (https://github.com/AbsaOSS/OTEL-example/blob/master/spring-boot-scala-example/run.sh); a loop where Python calls Scala to show distributed traces. Backend: Elastic APM (Elasticsearch + Kibana 8.17) (README). So the pattern is OTLP to Elastic APM, zero-code agents where possible. Whether this is production practice is UNVERIFIED.

**Weaknesses.** The binary `opentelemetry-javaagent.jar` is committed to git (tree https://github.com/AbsaOSS/OTEL-example/tree/master/spring-boot-scala-example); built against a Scala 3.4.0 nightly (run.sh); README "How to stop" says `docker start` (typo). It is a spike.

**2026 coverage.** OTel Java agent and Python auto-instrumentation are standard (https://opentelemetry.io/docs/zero-code/java/agent/).

**Lessons for Ubunye.** Ubunye's OTel emission should work with a plain OTLP endpoint and be viewable in Elastic APM (Absa's apparent backend). Use W3C trace context propagation so an Ubunye span can be the child of an orchestrator or service span, like the Python-to-Scala demo.

---

## 17. Small tooling repos

- **scalatest-extras**: conditional test tags (`ignoreIf(ver"$SPARK_VERSION" < ver"2.4")`, `ignoreIf(!isDatabaseAvailable)`), stdout capture, `System.exit` interception, per-test env vars, whitespace normalisation (https://github.com/AbsaOSS/scalatest-extras#readme). Release 1.0.2 in 2025-06 (commits). Lesson for Ubunye's conformance kit: tests must *skip with a reason* when a backend or capability is unavailable, not fail or silently pass.
- **rest-api-doc-generator**: CLI to generate a Swagger definition from a Spring MVC context class in a jar (https://github.com/AbsaOSS/rest-api-doc-generator#readme). Release 1.1.4 in 2025-06. Lesson: generate API contracts from code in CI.
- **springdoc-openapi-scala**: makes springdoc understand case classes, `Option` as not-required, `Unit` as No Content, sealed-trait ADTs with discriminators, enums (https://github.com/AbsaOSS/springdoc-openapi-scala#readme). v0.3.6 on 2025-11-07. Lesson: optionality must map to "required" correctly in any schema export; relevant to Ubunye's config JSON Schema and MCP tool schemas.
- **absa-shaded / absa-shaded-jackson-module-scala**: a manually shaded clone of FasterXML jackson-module-scala 2.15 "to avoid conflicts", last touched 2023-06 (https://github.com/AbsaOSS/absa-shaded-jackson-module-scala#readme). Evidence of Jackson classpath hell next to Spark. Lesson: Ubunye's Spark backend must isolate its own dependencies (or avoid JVM deps), because users will hit Spark's bundled versions.
- **spark-hadoop2**: a **fork** of apache/spark (fork=true, parent apache/spark, created 2026-06-17). The real content is branch `3.5.8-hadoop2`, one commit ahead of `v3.5.8`: "Add support for Hadoop 2.7", touching poms, `SparkHadoopUtil.scala` and `Utils.scala` (branch https://github.com/AbsaOSS/spark-hadoop2/commits/3.5.8-hadoop2 ; verified with `gh api repos/AbsaOSS/spark-hadoop2/compare/apache:v3.5.8...AbsaOSS:3.5.8-hadoop2`: ahead 1, behind 0). Inference (UNVERIFIED beyond the branch itself): in mid 2026 someone at Absa still needs Spark 3.5 on Hadoop 2.7. Lesson: Ubunye cannot assume modern Hadoop/Spark on-prem; the portability matrix should include "old Hadoop, patched Spark" as a real target or explicitly exclude it.

---

## 18. Matrix

| Problem | Evidence links | AbsaOSS repo | Ubunye relevance | Leaning |
|---|---|---|---|---|
| Typed coercion of raw data with per-row error provenance | Standardization.scala; ErrorMessage.scala; MetadataKeys.scala | spark-data-standardization | Schema standardization, quarantine reasons | Build (declarative field spec + error record), backend-neutral |
| Stable error code catalogue | DefaultErrorCodesConfig.scala; fa-db status README | spark-data-standardization, fa-db | Quarantine and task outcome codes | Build |
| Pluggable error policy between library and app | spark-commons ErrorHandler.scala; issue #83 | spark-commons | Expectations/quarantine policy port | Build |
| Schema diff and compatibility | StructTypeImplicits.scala#L226; commons PR #9; ultet TableAlter | spark-commons, commons, ultet | Schema diff/compat (missing today) | Build typed symmetric diff; Integrate table-format evolution |
| Spark version portability | spark-commons scala-spark3/4 trees; SparkVersionGuard | spark-commons | Backend capability check | Build |
| Nested array transformation and plan blow-up | spark-hats #41 | spark-hats | Performance guard on generated plans | Ignore lib; Build plan-time metric |
| Output partition sizing | DataFramePartitioner.scala; Spark AQE docs | spark-partition-sizing | Perf/cost hint | Integrate (AQE, Delta/Iceberg props) |
| Absolute paths break portability of streaming state | spark-metadata-tool README | spark-metadata-tool | Recovery, receipt/replay store portability | Build (relative paths + verify command) |
| Governance of runtime settings | SparkConfValidator.java | spark-launcher-supervisor | Policy checks on resolved config | Build (warn/enforce, redacted) |
| Run-to-run perf/cost regression | spot README; classification.py; issue #45 | spot | Receipt comparison per job key | Build on receipt; Integrate OTel/FOCUS |
| Schedule-driven backfill with execution vs information date | date_manager.py; engine.py | rialto | Partition/backfill (missing today) | Build |
| Dependency freshness windows | task_status_checker.py; README | rialto | Input freshness, lineage facet | Build |
| Idempotent partition overwrite | writer.py (replaceWhere) | rialto | Recovery/rerun | Build via connector capability |
| Post-write read-back count | data_checker.py check_written; CHANGELOG 2.0.9 | rialto | Reconciliation (missing today) | Build |
| Completion inferred from data presence | data_checker.py#L57 | rialto | Recovery | Build from receipts instead (anti-pattern) |
| Feature metadata and loader with key bridging maps | metadata_manager.py; README loader | rialto | ML/feature provenance, semantic mapping | Plugin (not core) |
| Point-in-time reads | table_reader.py; job_base.py | rialto | ML provenance | Build (enforced bound reader) |
| Task graph resolvability test | rialto jobs `resolver_resolves` | rialto | Conformance/testing | Build |
| DB access only via stored functions | fa-db README | fa-db | Connector capability (procedure sink) | Plugin |
| Transaction-rollback DB tests | balta README | balta | Connector conformance kit | Integrate (Testcontainers/pgTAP) |
| State-based DB schema deploy | ultet Ultet.scala, TableAlter.scala | ultet | Output table evolution | Integrate (Atlas/table formats); Ignore ultet |
| JWT issuance with key rotation, cloud cred exchange | login-service README; simba-athena README | login-service, simba-athena | MCP auth, secret:// short-lived creds | Integrate (OIDC/JWKS) |
| Service health with intermittent suppression | RawStatus.scala; DurationBasedNotificationDeciderImpl.scala | StatusBoard | Freshness/SLA alerting | Build small; Integrate alerting tools |
| OTel to Elastic APM | OTEL-example app.py, run.sh | OTEL-example | OTel emission target | Integrate |
| Conditional tests by capability | scalatest-extras README | scalatest-extras | Conformance kit skip semantics | Build |
| Dependency conflicts with Spark | absa-shaded-jackson-module-scala | absa-shaded* | Backend packaging | Build isolation |
| Old Hadoop still in production | https://github.com/AbsaOSS/spark-hadoop2/commits/3.5.8-hadoop2 | spark-hadoop2 (fork) | Portability matrix | Decide scope explicitly |

---

## 19. Top 10 transferable ideas (ranked)

1. **Execution date vs data (information) date, with schedule + shift + watch window and automatic gap filling** (rialto DateManager and engine). This is the missing partition/backfill model in Ubunye, proven in a bank's ML pipelines, and it is small. It also gives resume for free when combined with idea 2.
2. **Completion and resume from the run receipt, plus read-back reconciliation.** Rialto counts written rows from storage after the write (good) but decides completion from "rows exist" (bad). Ubunye already has a content-hashed receipt: make "success receipt for (job_key, data_date, config_hash)" the completion signal and record in/out/quarantined counts so `in = out + quarantined + filtered` can be checked.
3. **Declarative field standardization spec with a per-row error record and stable codes** (spark-data-standardization). Gives Ubunye schema standardization, better quarantine reasons, and a portable spec that both Spark and pandas backends must pass in a shared conformance suite.
4. **Pluggable error policy port** (spark-commons ErrorHandler). Tasks produce error columns; config picks quarantine, drop, fail or count. Decouples task logic from policy and lets the MCP/LLM port reason over errors uniformly.
5. **Enforced point-in-time reader.** Improve on rialto: inject a reader already bound to `data_date` (and optionally a table version), record any opt-out in the receipt, and emit it in OpenLineage. This is the ML/feature provenance guarantee most tools only document.
6. **Per-input freshness windows** (rialto dependencies with interval and filters). Fail only the affected partition, continue others, and surface freshness as data in the receipt and as an OTel metric.
7. **Run comparison per logical job** (spot). Key receipts by structured job identity, code version and config hash; compare duration, core-seconds, bytes and cost across versions to flag regressions. Never parse identity from names.
8. **Typed, symmetric schema diff with safe/unsafe classification and dry-run** (gap shown by spark-commons diffSchema and ultet). Property-test the differ. Map "safe" to the table format's evolution rules.
9. **Policy checks on the resolved config, warn or enforce, with redaction** (spark-launcher-supervisor, spot deny list, commons ErrorRef). Governance for a config-first engine is cheap because the config is data.
10. **Location-independent state plus a verify command** (spark-metadata-tool). Store paths relative to a root in receipts and replay stores; `ubunye verify` compares receipts to storage, with backup-then-mutate and dry-run for any repair.

---

## 20. Anti-patterns observed

1. **Completion inferred from data presence** (rialto data_checker.py#L57) and the one-row placeholder DataFrame to fake success for jobs without output (rialto README).
2. **Import-time timestamp in a dataclass default** (rialto record.py#L44): all bookkeeping rows in a process share one timestamp. Earlier, the bookkeeping table was overwritten on each run (CHANGELOG 2.0.7).
3. **SQL predicates built by string formatting without escaping** (rialto writer.py#L103).
4. **Documented guarantee not enforced in code**: rialto's "data newer than run_date is never used" depends on the job author (table_reader.py).
5. **Metadata as last-write-wins MERGE** (rialto metadata_manager.py): no history of what a feature meant.
6. **Identity encoded in free-text app names** (spot classification.py; admitted fragile in spot #45).
7. **Deny-list redaction** (spot aggregator.py#L41) and **printing full Spark config** in a governance agent (spark-launcher-supervisor SparkConfValidator.java#L27).
8. **Silent precision loss** accepted in decimal standardization (TypeParser.scala#L421) and "never fail" default for impossible casts.
9. **Unfinished diff logic shipped in main** with self-comparisons (ultet TableAlter.scala#L31-L55) and an unconditional throw (TableDef.scala#L130); no property tests caught it.
10. **Keeping a dependency that its own maintainers deprecated** (spark-hofs used in spark-data-standardization TypeParser.scala#L28).
11. **Performance fixes by caching and extra counts** (spark-partition-sizing cacheIfNot, count then repartition), and optimizer-barrier hacks via dummy cross joins (spark-hats #41).
12. **Copy-pasted shared code across repos** before a common base existed (naming code in fa-db, balta, then mag).
13. **Placeholder and spike repos left public with no status** (spark-functions-ex, OTEL-example with a committed binary agent jar and nightly Scala build).
14. **Hard dependency on an optional cloud at startup** (StatusBoard needs AWS env vars even with no AWS checks).
15. **Runtime dependencies that belong to tests** (rialto pyproject: pytest-mock, pathlib backport).

---

## Appendix: activity snapshot (from `gh api repos/AbsaOSS/<r>`, 2026-09-25)

| Repo | Last push | Latest release | Status |
|---|---|---|---|
| spark-data-standardization | 2026-09-21 | v0.5.1 (2026-09-10) | active |
| spark-commons | 2026-09-21 | v1.0.0 (2026-09-14) | active |
| commons | 2026-09-22 | 2.0.7 (Maven, 2026-09-22) | active |
| spark-hats | 2023-08-04 | v0.3.0 (2023-08-04) | dormant |
| spark-hofs | 2023-08-04 | v0.5.0 (2023-08-04) | self-deprecated |
| spark-functions-ex | 2021-08-31 | none | empty |
| spark-partition-sizing | 2026-06-07 (dependabot) | v0.2.0 (2023-02-24) | maintenance only |
| spark-metadata-tool | 2023-11-06 | v0.3.0 (2023-04-04) | dormant |
| spark-launcher-supervisor | 2023-05-21 | none | PoC |
| spot | 2024-07-31 | v0.0.1 (2024-02-12) | stalled |
| rialto | 2026-08-25 | 2.2.2 (PyPI, no GH release) | active |
| mag | 2026-05-17 | none | new base lib |
| fa-db | 2026-09-07 | v0.7.0 (2025-12-19) | active |
| balta | 2026-09-07 | v0.3.0 (2024-10-24) | slow |
| ultet | 2026-07-27 (dependabot) | none | unfinished |
| login-service | 2026-09-03 | v2.4.2 (2026-07-13) | active |
| simba-athena-login-service-support | 2026-01-08 | 0.3.0 (2026-01-08) | active, small |
| StatusBoard | 2026-09-17 | none | active (security upkeep) |
| OTEL-example | 2025-02-25 | none | spike |
| scalatest-extras | 2025-06-21 | 1.0.2 (Maven) | maintenance |
| rest-api-doc-generator | 2025-06-21 | 1.1.4 (Maven) | maintenance |
| springdoc-openapi-scala | 2025-11-07 | v0.3.6 (2025-11-07) | active, small |
| absa-shaded | 2023-05-26 | 0.0.1 (Maven) | dormant |
| absa-shaded-jackson-module-scala | 2023-06-08 | none | dormant clone |
| spark-hadoop2 | 2026-06-18 | none | fork of apache/spark (Hadoop 2.7 patch on 3.5.8) |
