# AbsaOSS part E: per-repository NEED verdicts for Ubunye Engine

Date 2026-09-25. Read-only analysis. Inputs: parts A to D in this folder and `absaoss_repos.tsv` (160 repositories). Baseline is Ubunye v0.7.0 as described in the brief (what it has and what it does not have).

How to read this. Every repository gets exactly one verdict for its main capability. "Absa built it" is never a reason. Evidence is graded as:

- **Ubunye fact**: a stated property of Ubunye v0.7.0 that makes the failure happen today.
- **Same-user-class evidence**: a failure a bank data team hit in the AbsaOSS repo (issues, code, changelog). It shows the failure is real for teams like Ubunye's users, but not that a Ubunye user has hit it.
- **Hypothetical / Absa-specific**: no evidence for Ubunye's situation, or tied to Absa's own infrastructure (HDFS, Hadoop 2.7, Menas, internal Kafka contracts, internal platform).

Section references like A§9 point to the part file and section.

Correction to part D: the org has **45 forks, not 41**. A GitHub API check on 2026-09-25 (`fork == true`) also returns `spark`, `spark-hadoop2`, `spring-cloud-stream-binder-jms` and `spring-cloud-stream-binder-ibm-mq`. They sit in the forks table below.

## 1. Summary count per verdict

| Verdict | Count | Repositories |
|---|---|---|
| NEED-CORE | 3 | pramen (P1), atum (P2), spark-commons (P2) |
| NEED-PACKAGE | 1 | cobrix (P3) |
| ADOPT-PRACTICE | 3 | version-tag-check (P2), living-doc-utilities (P3), agentic-toolkit (P3) |
| HAVE-ALREADY | 5 | spline-python-agent, spot, OTEL-example, hermes, sbt-git-hooks |
| USE-STANDARD | 7 | spline, spline-producer-proxy, dataset-comparison, spark-data-standardization, spark-partition-sizing, rialto, generate-release-notes |
| NOT-NEEDED | 141 | 67 data or practice repositories (section 4), 45 forks (section 5), 29 out of scope (section 6) |
| **Total** | **160** | |

Also, two needs come from a *secondary* pattern inside a repository whose main capability is NOT-NEEDED. They are listed in section 2b and are not counted above: a connector conformance kit (NEED-PACKAGE, P2) and PyPI trusted publishing with attestations (USE-STANDARD, P2).

## 2. NEED-CORE, NEED-PACKAGE and ADOPT-PRACTICE

### 2a. By repository (main capability)

| Repository | Failure it prevents for Ubunye users | Evidence | Verdict | Priority | Why |
|---|---|---|---|---|---|
| pramen | A multi-task run fails at task 7 of 8. The rerun recomputes tasks 1 to 6 on paid Glue, Dataproc or Databricks compute, and rewrites outputs that were already correct. | **Ubunye fact**: "a failed multi-task run recomputes everything" and there is no step state. Same-user-class: Pramen's "bookkeeping written last" and resume after a crash (B§1). Rialto's bookkeeping was once overwritten on every run (C§11), which shows that getting completion state wrong is easy. | NEED-CORE | P1 | Skipping a task needs a small runner primitive that no plugin can add: skip a task when a *successful* receipt already exists for the same task, config hash, code hash, input row hashes and dt. Everything needed is already in the receipt. Airflow export covers resume only for users who orchestrate through Airflow, and `ubunye run` and `ubunye deploy` users get nothing. Take two things from daggerpool (D§2.1): mark a failed task's dependents as skipped while independent tasks keep running, and record interrupted tasks as `cancelled`. Do not copy Pramen's metastore, locks, date DSL or count probes. |
| atum | A join silently drops or multiplies rows. Output expectations still pass, because `row_count` and `not_null` only look at the output, so a bank publishes a table with lost records and nothing fails. | **Ubunye fact**: there is no input-versus-output reconciliation. Same-user-class: Atum exists for exactly this. Its README names inner-join loss, outer-join explosion and BCBS reporting (A§9). Pramen journals input, output and previous counts (B§1). Rialto moved to reading counts back from storage (C§11). | NEED-CORE | P2 | Make it an expectation that spans inputs and outputs. For example `output.count == input.count - quarantined` with a tolerance, and optional sum or abs_sum on a named column, using the existing fail/warn severities. Row counts are nearly free because rows-v1 already visits every row. The rule has to live in the engine because only the engine sees inputs and outputs in the same run. OpenLineage `dataQualityMetrics` can carry the numbers but not the verdict. It is P2, not P1, because no Ubunye user has reported silent loss yet. Do not copy `_INFO` sidecar files, session-global state or a central Postgres service. |
| spark-commons | An upstream column is renamed or retyped. The receipt shows only that the schema hash changed, so `ubunye gate` fails without saying which column changed or how, and the user diffs by hand. | **Ubunye fact**: it stores "only a schema hash". Same-user-class: schema-diff helpers were written again and again, in spark-commons `diffSchema`, commons PR #9 and Pramen `SchemaDifference` (C§2, C§3, B§1). | NEED-CORE | P2 | Store the schema itself (field, type, nullable) next to its hash in the receipt. Emit it as the OpenLineage SchemaDatasetFacet. Have `gate` print a typed, symmetric diff: added, removed, type changed, nullability changed. This is a few dozen lines on data Ubunye already reads. Compatibility policy (what counts as a breaking change) belongs to ODCS data contracts, not the core. Do not copy spark-commons' one-directional, case-insensitive diff (C§2). |
| cobrix | A bank or insurer has mainframe extracts (EBCDIC, COBOL copybooks, variable-length records). No Ubunye connector can read them, so the team decodes outside Ubunye and loses the receipt for that step. | **Ubunye fact**: there are no legacy-format connectors. Same-user-class: Cobrix is active and releases monthly (B§2). Its top issues are silent NULL on decode failure, a sign of real production use. Evidence that *Ubunye users* have mainframe feeds is **hypothetical**, since nobody has asked for it. | NEED-PACKAGE | P3 | If this is built, make it a thin `ubunye-connectors-mainframe` plugin that wraps Cobrix on the Spark backend and declares itself Spark-only. Surface Cobrix's `_corrupt_fields` as a decode-failure count in the receipt, so NULLs from failed decoding are visible. Never reimplement a copybook parser. Wait for a real user request. The same plugin could cover fixed-width files, which pandas already reads with `read_fwf`. |
| version-tag-check | A release goes to PyPI with a tag that does not match `pyproject.toml`, or with a skipped or duplicated version. PyPI never allows a version number to be uploaded again, so one bad release burns that number. | Hypothetical for Ubunye, since no botched release is on record. There is a structural risk: one maintainer plus agents cut the releases, and the 0.6.0 release was assembled on an integration branch. The chain is described in D§2.3. | ADOPT-PRACTICE | P2 | Add two small steps to the existing release workflow: "tag == pyproject version" and "tag is the next valid semver increment". Publish from a draft release, so a human publishing it is the approval gate. This is about 20 lines of workflow, not a dependency. PyPI's no-reupload rule makes a mistake permanent, which is why this is P2 rather than P3. |
| living-doc-utilities | A receipt is truncated by a crash mid-write, or its format changes without a version bump. `gate` and replay then misread old receipts. | Hypothetical, with no incident on record. Ubunye's rows-v1 naming already shows versioned hash schemes (D§2.2 rules R4, R5 and R12). | ADOPT-PRACTICE | P3 | Write down the receipt contract as numbered rules. The receipt schema id is a const, never an enum of aliases. Readers check the id before structure and say "align versions" on a mismatch. Receipts are written to a temp file and then renamed atomically. One full-sample test fills every optional field. This is a documentation and test practice, not new software. |
| agentic-toolkit | A change to an MCP tool description or output breaks how agents call `ubunye mcp`, and nobody notices until an agent misuses it. | Hypothetical. Ubunye ships an MCP server and is maintained with agents, but no regression is on record (D§2.7). | ADOPT-PRACTICE | P3 | Keep a small `evals/` file per MCP tool with realistic prompts and expected calls, run by hand before a release. Ship a SKILL.md that teaches agents how to use `ubunye mcp`. Keep the rule "AI accelerates authoring, never sits on the runtime path", which already matches Ubunye's replayed and budgeted LLM port. |

### 2b. Needs that come from a secondary pattern (not counted above)

| Source repository (main verdict) | Failure it prevents for Ubunye users | Evidence | Verdict | Priority | Why |
|---|---|---|---|---|---|
| spring-cloud-stream-binder-jms, a fork (NOT-NEEDED). Supported by the spline-spark-agent coverage table (A§2) and Cobrix golden test files (B§2) | A third-party or new connector implements `merge` or `overwrite_partitions` with different semantics. It might not be idempotent on rerun, or it might silently ignore `replace_where`. Users find out in production. A misconfigured write mode fails only after compute. | **Ubunye fact**: there are 7 built-in connectors with uneven write modes (replace_where is Delta only), connectors are plugins, and conformance kits exist only for backends. Needing a backend kit is itself evidence that implementations drift. Same-user-class: the binder TCK's abstract `EndToEndIntegrationTests` (B§11-12). | NEED-PACKAGE | P2 | Put a connector conformance pytest mixin next to the existing backend mixin. A connector subclasses it and inherits tests for schema round-trip, idempotent rerun per write mode, and partition overwrite. It also skips the tests for capabilities the connector does not declare, with the reason shown. This requires connectors to declare supported write modes, so `plan` can reject an unsupported mode before compute. |
| living-doc-utilities release.yml anti-pattern and cap-infra-dns cosign/SBOM (D§2.9, D§5.3) | A long-lived PyPI token leaks, or a published wheel cannot be traced to its CI build. | Hypothetical. I could not confirm whether Ubunye already uses PyPI trusted publishing (see section 7). | USE-STANDARD (PyPI trusted publishing via GitHub OIDC, plus PEP 740 attestations) | P2 if not already in place | This comes with GitHub Actions and PyPI for free, so there is nothing to build. |

## 3. HAVE-ALREADY and USE-STANDARD

| Repository | Failure it addresses | Where it is covered | Verdict | Priority | Why |
|---|---|---|---|---|---|
| spline-python-agent | Lineage and run identity are not known, or a code change does not change run identity. | Ubunye declares I/O in config and records a code hash of the task .py files in the receipt. | HAVE-ALREADY | - | This agent is the closest AbsaOSS analogue to Ubunye (A§7). Ubunye already hashes the code instead of embedding the source. |
| spot | A new version quietly doubles runtime or cost. | Ubunye has receipt timings plus the environment hash, OpenTelemetry metrics and FOCUS cost rows, keyed by structured identity rather than parsed app names. | HAVE-ALREADY | - | Spot's whole idea is comparing runs of the same job (C§10), and Ubunye already records the inputs to that comparison. Whether `gate` puts thresholds on timings is an open question (section 7). |
| OTEL-example | A pipeline cannot be traced in the bank's APM tool. | Ubunye already emits OpenTelemetry spans and metrics over OTLP, which works with Elastic APM, Absa's apparent backend (C§16). | HAVE-ALREADY | - | This is a one-commit spike repository, and Ubunye has more than it shows. |
| hermes | A migrated or re-platformed pipeline gives different output, and nobody notices. | Ubunye's rows-v1 order-independent hash of every output row, plus `ubunye gate`, already detects any difference between runs or backends. | HAVE-ALREADY | - | Detection is covered. Explaining *which* rows differ is dataset-comparison's row below. Hermes is archived (A§12). |
| sbt-git-hooks | Commit hooks differ between contributors. | The Ubunye repository already uses pre-commit (black). | HAVE-ALREADY | - | This is the sbt equivalent of a tool Ubunye already runs. |
| spline | A user asks "what breaks downstream if I change task A?" or "which runs fed this table?" and cannot get an answer. | **USE-STANDARD: Marquez or DataHub** consuming the OpenLineage 2-0-2 events Ubunye already emits. | USE-STANDARD | P3 (document the recipe) | The brief lists "lineage impact queries across tasks" as missing, but an OpenLineage backend answers them from Ubunye's events. Spline's own wire format is now OpenLineage (A§1). Do not build a graph store: Spline paid for one with ArangoDB BSL licensing and CPU blow-ups (A§1). |
| spline-producer-proxy | The OpenLineage endpoint is down, so events are lost or the job fails. | **USE-STANDARD: openlineage-python client transports** (file or composite, plus http) behind the existing emitter. Receipts on disk remain the durable record. | USE-STANDARD | P3 | An outbox for lineage is a transport concern the OpenLineage client already handles (A§6). Emission failures must never fail the job; see section 7 for whether that is already true. |
| dataset-comparison | `gate` reports that outputs differ between a legacy run and a Ubunye run, or between Spark and pandas, but not which rows or columns. | **USE-STANDARD: datacompy** (open source, pandas and Spark) as an optional diagnostic after a gate failure. | USE-STANDARD | P3 | Absa's tool has a real defect: a 32-bit `String.hashCode` with no separators (A§13). Ubunye's rows-v1 hash covers detection. For the diff, use a maintained library. Keep one idea from here: run the same task twice to learn which columns are nondeterministic. |
| spark-data-standardization | Casting raw strings to types turns bad values into NULLs silently. | **USE-STANDARD: Spark ANSI mode** (on by default in Spark 4) with `try_cast`, and **pandera** on pandas. Ubunye's `not_null`, `matches`, quarantine and ODCS contracts catch what remains. | USE-STANDARD | - | The per-row errCol with codes is well designed (C§1), but it is Spark and Scala only, and it accepts silent decimal rounding. Ubunye must stay backend-neutral. |
| spark-partition-sizing | A write produces too many small files, or skewed files. | **USE-STANDARD: Spark AQE** (advisory partition size), **Delta optimized writes and auto compaction**, and **Iceberg target file size**. | USE-STANDARD | - | Its niche has been superseded (C§7). At most, pass write options through to the connector. |
| rialto | Backfilling 90 days of a partitioned task, or keeping the execution date and the data date apart. | **USE-STANDARD: the orchestrator's logical date and backfill** (Airflow 2/3, which Ubunye already exports to; Databricks Workflows). For point-in-time features, **Feast**. **MLflow** covers the model side. | USE-STANDARD | P3 | The brief lists "partitions or backfill beyond a single dt" as missing, but every orchestrator Ubunye exports to already loops dt over a range. Rialto's own weaknesses (completion inferred from "rows exist", an import-time timestamp, C§11) argue against copying its runner. Once pramen's receipt-based skip exists, a `--dt-from/--dt-to` loop in `ubunye run` is trivial (section 7). |
| generate-release-notes | The changelog misses work, or direct commits bypass review. | **USE-STANDARD: GitHub's generated release notes** (label categories in `.github/release.yml`) plus the **rulesets** already applied to the org, which block direct pushes. | USE-STANDARD | - | The "service chapters" idea exists to audit direct commits and orphan PRs (D§2.3). The rulesets already prevent the main case. |

## 4. NOT-NEEDED (non-fork, in-scope repositories): 67

| Repository | Capability | Reason (failure, evidence, coverage) |
|---|---|---|
| spline-spark-agent | Spark logical-plan lineage listener | Ubunye declares its I/O, so it does not reverse-engineer Spark plans. The agent's issue list is dominated by Delta, Databricks and new Spark versions breaking plan parsing (A§2). If a user wants column-level lineage from Spark, the upstream openlineage-spark integration exists. There is no Ubunye failure. The redaction lesson is covered in section 7. |
| spline-ui | Lineage UI | Ubunye does not build UIs. Marquez or DataHub UIs show OpenLineage lineage. |
| spline-getting-started | Docker Compose for the Spline stack | Only useful if you run Spline. Its issues mostly show multi-service first-run failures (A§4). |
| spline-openlineage | OpenLineage-to-Spline bridge proof of concept | Dormant since 2022 and Absa-specific. Its lesson (consumers need write mode and FAIL events) is a verification item in section 7. |
| spline-misc | Spline 0.3 UI adapter for Menas | Legacy, Absa-specific. |
| spline-root-pom | Maven parent | JVM build scaffolding. |
| atum-service | Central Postgres store for control measures | Mandatory central service. It captures measures but never evaluates them (A§10). Ubunye keeps evidence in receipts and OpenLineage. The need for reconciliation is carried by `atum` above. |
| enceladus | Menas-driven standardisation and reference-data conformance engine (HDFS, MongoDB, Oozie) | Frozen, with 392 open issues (A§11). Code mapping (for example DE to Germany) is an ordinary join in `Task.transform`, or a dbt seed. Restatement versions are covered by Delta and Iceberg time travel. "Semantic mapping" appears on Ubunye's missing list, but no Ubunye user failure is on record (section 7). |
| datasets-similarity | Embedding-based table similarity | Archived by Absa for "no future practical use" (A§14). |
| fixed-width | Spark fixed-width data source | Dormant since 2023, and an invalid mode silently falls back to permissive (B§3). pandas `read_fwf` and Cobrix `record_format=D` cover it. Fold it into the cobrix plugin if that is ever built. |
| ABRiS | Avro plus Confluent Schema Registry for Spark | Ubunye has no streaming or Kafka connector, and no user has asked for one. If one is added, use Spark's built-in `from_avro` and the Confluent client (B§4). |
| py2k | pandas to Kafka producer | Dormant, and it infers schemas from sample data, an anti-pattern (B§5). |
| KafkaCase | Scala case classes for Absa's internal Kafka topics | Absa-specific contracts. The "dataset changed" event is covered by OpenLineage (B§6). |
| hyperdrive | Structured Streaming ingestion with exactly-once handling | Streaming is out of Ubunye's scope. Kafka EOS, Delta MERGE and Spark Declarative Pipelines AUTO CDC cover the patterns (B§7). |
| hyperdrive-trigger | Bespoke workflow scheduler with UI | Ubunye exports to Airflow. Airflow 3 asset scheduling covers sensors (B§8). |
| hyperdrive-hive-jobs | `MSCK REPAIR TABLE` job | This is a Hive-partition-discovery problem that Delta, Iceberg, Unity and Glue avoid, and it interpolates identifiers into SQL (B§9). |
| Jdbc2S | JDBC watermark streaming source | Dormant, and it misses late-committing rows by design (B§10). No Ubunye user has reported full reloads over JDBC as a problem. The `dt` variable in a JDBC query already allows windowed reads. Revisit only on request (P3 at most). |
| EventGate | Absa's schema-governed event Lambda (dlchange, runs, status_change) | Absa's internal telemetry intake. OpenLineage already standardises run and dataset events (B§13). An adapter would only matter if a customer runs EventGate. |
| commons | Scala utilities (config, S3 paths, ErrorRef) | JVM library. The ErrorRef idea (return an id, not a stack trace) only matters if `ubunye mcp` is exposed over the network (section 7). |
| spark-hats | Nested array transformations | Superseded by Spark `withField` and `dropFields`. Dormant since 2023 (C§4). |
| spark-hofs | Scala higher-order functions API | Deprecated by its own README in favour of native Spark (C§5). |
| spark-functions-ex | Placeholder | Empty repository (C§6). |
| spark-metadata-tool | Repairs `_spark_metadata` after moving streaming output | This is a streaming file-sink problem from the HDFS-to-S3 migration (C§8). Ubunye has no streaming, and Delta and Iceberg avoid it. |
| spark-launcher-supervisor | JVM agent that checks Spark settings on YARN edge nodes | PoC, YARN-specific, and it prints the full Spark config including secrets (C§9). No Ubunye failure. |
| mag | Relational DB naming utilities in Scala | JVM, internal base library. |
| fa-db | DB access only through Postgres functions | No Ubunye user has asked to write through stored procedures (C§12). A JDBC plugin could add it later. |
| balta | Scala tests for Postgres functions | pgTAP and Testcontainers cover this. |
| ultet | State-based Postgres schema deployment | Unfinished (its diff compares a table with itself, C§13). Atlas and table-format evolution cover the idea. |
| login-service | JWT issuer with LDAP and Entra | Identity infrastructure. Ubunye uses cloud secret stores and cloud identity, and OIDC providers cover issuance (C§14). |
| simba-athena-login-service-support | Athena JDBC credentials from Absa Login Service | Tied to Absa Login Service. |
| StatusBoard | Absa CPS service-health board | Internal status page. Alerting belongs in the user's OTel or Grafana stack (C§15). |
| scalatest-extras | Conditional ScalaTest tags | pytest `skipif` covers this for Ubunye's Python tests. |
| rest-api-doc-generator | Swagger from Spring MVC | Java web tooling. |
| springdoc-openapi-scala | OpenAPI for Scala case classes | Java and Scala web tooling. |
| absa-shaded | Shaded JVM artifacts | JVM classpath workaround. |
| absa-shaded-jackson-module-scala | Shaded Jackson clone | JVM classpath workaround (C§17). |
| living-doc | Documentation mined from issues and Gherkin | Ubunye's docs are hand-written for access. Nothing is failing that this would fix. |
| living-doc-collector-gh | Collects GitHub issues into JSON | Part of living-doc. |
| living-doc-collector-ad | Collects Azure DevOps work items | Part of living-doc. |
| living-doc-toolkit | Normalises living-doc JSON | Part of living-doc. |
| living-doc-generator-pdf | Renders PDF docs | Part of living-doc. |
| living-doc-generator-markdown | Renders Markdown docs | Part of living-doc. |
| living-doc-generator-mdoc | Renders mdoc docs | Part of living-doc. |
| release-notes-presence-check | Requires a Release Notes section in PR bodies | With one maintainer, a PR template does the job. |
| check-pr-requirements | PR title, body and branch checks | Contributor governance for large teams. The org rulesets are enough at one maintainer. |
| filename-inspector | Enforces test-file suffixes | pytest discovery config covers this. |
| organizational-workflows | AquaSec findings to GitHub issues | GitHub code scanning, secret scanning and Dependabot alerts already have their own lifecycle, and Ubunye does not use AquaSec. |
| aquasec-scan-results | AquaSec to SARIF | Archived, and AquaSec is commercial. |
| GH-automation | Dependent-issues workflow | A README plus a third-party action call (D§2.5). |
| reusable-workflows | Placeholder | Empty since 2021. |
| validate-certificates | Certificate expiry check action | Ubunye manages no certificates. |
| daggerpool | Go DAG worker pool | Go library. Its useful ideas (skip dependents, record cancelled tasks) are folded into the pramen NEED-CORE row. |
| knowledge-base | Build-time docs aggregator | For many doc sites in a restricted network. Ubunye has one docs site. |
| knowledge-base-docs-example | Example for knowledge-base | Same as above. |
| k3d-action | Ephemeral k3s in GitHub Actions | Ubunye's k8s tests use kind, and helm/kind-action is the maintained equivalent. Dormant since 2023. |
| karpenter-provider-vsphere | Karpenter provider for vSphere | Kubernetes platform. Its "hash plus hash-version" drift idea is already present in Ubunye's versioned `rows-v1` hash scheme. |
| golic | License-header injector (Go) | REUSE or ruff handle this for Python. Dormant since 2021. |
| env-binder | Env vars to Go structs | Go, deprecated. |
| go-k8s-operator-binder | Env vars and annotations to Go structs | Go operator tooling. |
| pgdump-lambda | Scheduled `pg_dump` to S3 | Operational utility, and it commits binaries to git (D§2.9). |
| gh-pages-skeleton | Jekyll docs starter | Superseded. |
| root-pom | Maven parent POM | JVM build. |
| py-composite-action-lib | Composite action helper | 2024 stub. |
| gh-action-upload-release-asset-composite | Release asset upload | 2021 stub. `gh release upload` covers it. |
| ghpages-to-tf-provider-registry | Terraform registry on GitHub Pages | Ubunye publishes to PyPI. |
| tf-provider-registry-generator | Terraform registry metadata | Same as above. |
| absaoss.github.io | Org landing site | Not software. |

## 5. Forks (45)

Every fork is NOT-NEEDED as a repository, because it is someone else's project. The last column says whether Ubunye should use the upstream project.

| Fork | Upstream | Verdict | Upstream relevant to Ubunye? |
|---|---|---|---|
| spark | apache/spark (2019 mirror) | NOT-NEEDED | Already used: it is Ubunye's Spark backend. |
| spark-hadoop2 | apache/spark (3.5.8 plus a Hadoop 2.7 patch) | NOT-NEEDED | Already used upstream. It shows old Hadoop still exists at Absa, which is a scope question, not a need (section 7). |
| spring-cloud-stream-binder-jms | spring-attic | NOT-NEEDED | No (JMS messaging). Its test-kit pattern drives the section 2b conformance kit. |
| spring-cloud-stream-binder-ibm-mq | spring-attic | NOT-NEEDED | No. |
| setup-terraform | hashicorp/setup-terraform | NOT-NEEDED | Only if Ubunye's cloud CI uses Terraform. Use upstream directly. |
| terraform-controller | rancher/terraform-controller | NOT-NEEDED | No. |
| indy-sdk | hyperledger-indy/indy-sdk | NOT-NEEDED | No (identity). |
| aries-framework-rs | openwallet-foundation/vcx | NOT-NEEDED | No. |
| aries-vcx | openwallet-foundation/vcx | NOT-NEEDED | No. |
| terraform-provider-artifactory | jfrog/terraform-provider-artifactory | NOT-NEEDED | No. |
| react-native-keychain | oblador/react-native-keychain | NOT-NEEDED | No (mobile). |
| react-native-facetec-zoom | petr-hlavnicka560/react-native-facetec-zoom | NOT-NEEDED | No. |
| ghaction-import-gpg | hashicorp/ghaction-import-gpg | NOT-NEEDED | No. PyPI does not need GPG signing. |
| olm-bundle | upbound/olm-bundle | NOT-NEEDED | No. |
| log4j-jndi-be-gone | nccgroup/log4j-jndi-be-gone | NOT-NEEDED | No. The 2021 CVE is patched upstream. |
| jacoco (archived) | jacoco/jacoco | NOT-NEEDED | No (JVM coverage). |
| sbt-jacoco (archived) | sbt/sbt-jacoco | NOT-NEEDED | No. |
| k8gb | k8gb-io/k8gb | NOT-NEEDED | No. The governance lesson (donate to a neutral home) is not a current need. |
| ca-injector | microcumulus/ca-injector | NOT-NEEDED | No. |
| DuckHunt-JS | MattSurabian/DuckHunt-JS | NOT-NEEDED | No (a Confluent demo game). |
| external-dns | kubernetes-sigs/external-dns | NOT-NEEDED | No. |
| cert-manager | cert-manager/cert-manager | NOT-NEEDED | No. |
| certmanager_website | cert-manager/website | NOT-NEEDED | No. |
| rancher-charts | rancher/charts | NOT-NEEDED | No. |
| fleet | rancher/fleet | NOT-NEEDED | No. |
| rancher | rancher/rancher | NOT-NEEDED | No. |
| cluster-api-operator | kubernetes-sigs/cluster-api-operator | NOT-NEEDED | No. |
| cluster-api-provider-aws | adammw/cluster-api-provider-aws | NOT-NEEDED | No. |
| cluster-api-addon-provider-fleet | rancher/cluster-api-addon-provider-fleet | NOT-NEEDED | No. |
| cluster-api-provider-rke2 | rancher/cluster-api-provider-rke2 | NOT-NEEDED | No. |
| openpubkey | openpubkey/openpubkey | NOT-NEEDED | No. |
| opkssh | openpubkey/opkssh | NOT-NEEDED | No. |
| coredns-crd-plugin | k8gb-io/coredns-crd-plugin | NOT-NEEDED | No. |
| krr | robusta-dev/krr | NOT-NEEDED | No. |
| modules | kcl-lang/modules | NOT-NEEDED | No. |
| traefik | traefik/traefik | NOT-NEEDED | No. |
| chaos-mesh | chaos-mesh/chaos-mesh | NOT-NEEDED | No. |
| terraform-aws-eks | terraform-aws-modules/terraform-aws-eks | NOT-NEEDED | No. EMR Serverless and Glue deployment does not need EKS. |
| rolesanywhere-credential-helper | aws/rolesanywhere-credential-helper | NOT-NEEDED | Possibly, for on-prem users who need AWS credentials. Not a Ubunye need. |
| elemental-toolkit | rancher/elemental-toolkit | NOT-NEEDED | No. |
| elemental | SUSE/elemental | NOT-NEEDED | No. |
| karpenter | kubernetes-sigs/karpenter | NOT-NEEDED | No. |
| renovate | renovatebot/renovate | NOT-NEEDED | No. Dependabot already covers dependency bumps. |
| tau-pi | earendil-works/pi (coding agent) | NOT-NEEDED | No. |
| cloud-provider-vsphere | kubernetes/cloud-provider-vsphere | NOT-NEEDED | No. |

## 6. Out of scope (29): all NOT-NEEDED

| Repository | Area | One line |
|---|---|---|
| rn-indy-sdk (archived) | Identity / Hyperledger | React Native Indy wrapper, now upstream. |
| vcxagencynode | Identity / Hyperledger | Aries mediator agency, decommissioned. |
| aries-oob-shortener | Identity / Hyperledger | URL shortener for Aries messages. |
| sovrin-networks | Identity / Hyperledger | Sovrin genesis files. |
| driver-did-sov | Identity / Hyperledger | `did:sov` resolver driver. |
| dlt-cocoapods-specs | Mobile | CocoaPods spec repository. |
| manabu-ui | UI / internal app | Learning portal front end. |
| manabu-backend | Internal app | Learning portal back end. |
| ang-tools | UI library | Angular tools. |
| microfrontends-poc (archived) | UI | Module Federation PoC. |
| cps-shared-ui | UI library | Angular component library. |
| cps-mdoc-viewer | UI library | Angular Markdown doc viewer. |
| inception | App scaffold | Java plus Angular framework. |
| ScAPI (archived) | Test framework | API test framework. |
| pit-junit-cucumber-upstream | Test framework | Mutation testing for Cucumber. |
| hackathon-turbo | Notebooks | 2023 hackathon. |
| gitbook | Empty | Placeholder. |
| coredns-delegate | DNS | CoreDNS plugin. |
| cert-manager-webhook-externaldns | DNS / k8s | DNS01 solver. |
| external-dns-infoblox-webhook | DNS / k8s | Infoblox provider. |
| cap-infra-dns | DNS / k8s operator | Cluster API DNS registrar. Its cosign/SBOM release pipeline is noted in section 2b. |
| samlet | k8s operator | saml2aws credentials operator. |
| aws-get-token | k8s / AWS | EKS token generator. |
| CertMe | PKI | Vault-signed certificates to ACM. |
| provider-jet-rancher | k8s / Crossplane | Moved upstream. |
| docker-distribution-artifactory | Registry | Docker registry on Artifactory. |
| artifactory-registry-meta-generator | Registry | Metadata scraper. |
| homebrew-tap | Packaging | Brew tap. |
| gopkg | Go library | Controller helpers. |

## 7. Where I was unsure, and why

1. **Is resume really missing for orchestrated users?** The verdict on pramen assumes many users run multi-task pipelines through `ubunye run` or `ubunye deploy` rather than an exported Airflow DAG. If nearly everyone orchestrates through Airflow, the failure mostly disappears (Airflow retries only failed tasks) and pramen drops to P2. I kept P1 because the brief states the recompute-everything behaviour as a fact and paid cloud compute makes it expensive.
2. **Backfill: USE-STANDARD or NEED-CORE?** I sent rialto's backfill to orchestrators. If Ubunye users run without an orchestrator (for example on Container Apps or a plain k8s CronJob), a `--dt-from/--dt-to` loop in `ubunye run` that uses the pramen receipt-skip becomes a small P2 core item. I found no evidence either way about how users orchestrate.
3. **Secrets in emitted events: not checked.** Spline had to add password masking after plaintext JDBC passwords appeared in lineage (A§2). I did not verify whether Ubunye's OpenLineage dataset namespaces, receipts or OTel attributes can contain *resolved* `secret://` values, for example inside a JDBC URL. If they can, that is a P1 core fix, not a NOT-NEEDED. It needs a five-minute check in the code.
4. **OpenLineage completeness: not checked.** spline-openlineage shows that consumers need FAIL events and the write mode (append versus overwrite) to rebuild lineage over time (A§5). I do not know whether Ubunye emits FAIL events or records the write mode in a facet. Verify before calling the spline row fully USE-STANDARD.
5. **Does emission failure fail the job?** The spline-producer-proxy verdict assumes an unreachable OpenLineage or OTel endpoint does not fail a Ubunye run. That is unverified.
6. **Does `gate` threshold timings and cost?** The spot HAVE-ALREADY assumes the receipt data is enough. If `gate` compares only hashes, performance regressions are recorded but not gated. That is a small follow-up, not a new need.
7. **Reconciliation priority.** Atum is P2 only because no Ubunye user has reported silent row loss. For bank and insurer users under BCBS 239-style audit, an auditor may ask for "in = out + rejected" evidence, which would make it P1. I had no user evidence either way.
8. **Semantic and reference-data mapping (enceladus).** It is on Ubunye's missing list, yet I judged it ordinary transform code. If users repeat the same point-in-time code-mapping join across many tasks, a small plugin could earn its place. There is no evidence yet.
9. **Mainframe demand (cobrix).** African banks do run mainframes (Absa itself does), but I found no Ubunye user asking for EBCDIC. I rated it P3 NEED-PACKAGE rather than NOT-NEEDED because the brief lists legacy-format connectors as missing and the fix is a thin wrapper around a maintained upstream library.
10. **PyPI trusted publishing.** I could not confirm whether Ubunye's release workflow already uses it (memory mentions a pending PyPI approval). If it does, the section 2b row becomes HAVE-ALREADY.
11. **Fork count.** Part D says 41 forks. The GitHub API returns 45 (it adds spark, spark-hadoop2 and the two spring binders). I used 45, so the in-scope count is 86 rather than 90.
12. **Old Hadoop (spark-hadoop2).** This is evidence that some bank estates still run Spark 3.5 on Hadoop 2.7. Whether Ubunye supports that is a scope decision for the maintainer, not a need I can derive from evidence.
