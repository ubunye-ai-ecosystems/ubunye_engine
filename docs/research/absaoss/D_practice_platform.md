# AbsaOSS research, part D: engineering practice and platform tooling

Scope: every AbsaOSS public repo not covered by parts A, B and C (106 repos). Research date 2026-09-25. Read-only: GitHub REST API reads of repo metadata, trees, READMEs, source, workflows and issue search. Base URL for every repo below is `https://github.com/AbsaOSS/<repo>`. Anything I could not confirm from a primary source is marked UNVERIFIED. Statements marked "inferred from code" are my reading of the source, not behaviour I ran.

## 0. The short version

Absa's public engineering practice has two eras. The 2020 to 2023 era is platform work by a Kubernetes team (k8gb, k3d-action, golic, samlet, env-binder, Terraform registry tooling). Most of it is now dormant or has been handed to upstream foundations. The 2024 to 2026 era is a coherent **Python GitHub Actions governance stack**: release notes, tag checks, PR checks, security backlogs, and the living-doc pipeline. All of it is built by one group and applied the same way across new repos (same issue templates, same `release_draft.yml`, same Copilot rules files). For Ubunye the most useful material is:

1. **living-doc-utilities `docs/contracts.md`**. This is a written, numbered rulebook for artifacts passed between pipeline stages: const schema ids, a metadata envelope, per-artifact stats, field lineage, "whole source or fail", and one retry policy. It is the closest thing in the org to Ubunye's run receipt, and it is stricter.
2. **The release chain**: version-tag-check, release-notes-presence-check, generate-release-notes, a two-stage draft-then-publish release, and a tag-equals-pyproject check.
3. **daggerpool**. A small, readable DAG executor. Its useful idea is the 5-state result model with frontier and blocked-by views. It is not a retry engine, despite what its description says.
4. **agentic-toolkit**. Absa is shipping *skills with evals*, not MCP servers. It says MCP servers come later.

## 1. Classification

### 1a. Forks of upstream projects (41, no deep dive)

The `gh api repos/AbsaOSS/<r> --jq '.fork,.parent.full_name'` check returned `fork=true` for all of these:

| Repo | Upstream parent |
|---|---|
| setup-terraform | hashicorp/setup-terraform |
| terraform-controller | rancher/terraform-controller |
| indy-sdk | hyperledger-indy/indy-sdk |
| aries-framework-rs | openwallet-foundation/vcx |
| aries-vcx | openwallet-foundation/vcx |
| terraform-provider-artifactory | jfrog/terraform-provider-artifactory |
| react-native-keychain | oblador/react-native-keychain |
| react-native-facetec-zoom | petr-hlavnicka560/react-native-facetec-zoom |
| ghaction-import-gpg | hashicorp/ghaction-import-gpg |
| olm-bundle | upbound/olm-bundle |
| log4j-jndi-be-gone | nccgroup/log4j-jndi-be-gone |
| jacoco (archived) | jacoco/jacoco |
| sbt-jacoco (archived) | sbt/sbt-jacoco |
| k8gb | k8gb-io/k8gb |
| ca-injector | microcumulus/ca-injector |
| DuckHunt-JS | MattSurabian/DuckHunt-JS |
| external-dns | kubernetes-sigs/external-dns |
| cert-manager | cert-manager/cert-manager |
| certmanager_website | cert-manager/website |
| rancher-charts | rancher/charts |
| fleet | rancher/fleet |
| rancher | rancher/rancher |
| cluster-api-operator | kubernetes-sigs/cluster-api-operator |
| cluster-api-provider-aws | adammw/cluster-api-provider-aws |
| cluster-api-addon-provider-fleet | rancher/cluster-api-addon-provider-fleet |
| cluster-api-provider-rke2 | rancher/cluster-api-provider-rke2 |
| openpubkey | openpubkey/openpubkey |
| opkssh | openpubkey/opkssh |
| coredns-crd-plugin | k8gb-io/coredns-crd-plugin |
| krr | robusta-dev/krr |
| modules | kcl-lang/modules |
| traefik | traefik/traefik |
| chaos-mesh | chaos-mesh/chaos-mesh |
| terraform-aws-eks | terraform-aws-modules/terraform-aws-eks |
| rolesanywhere-credential-helper | aws/rolesanywhere-credential-helper |
| elemental-toolkit | rancher/elemental-toolkit |
| elemental | SUSE/elemental |
| karpenter | kubernetes-sigs/karpenter |
| renovate | renovatebot/renovate |
| tau-pi | earendil-works/pi (the "Pi" minimal coding agent) |
| cloud-provider-vsphere | kubernetes/cloud-provider-vsphere |

Three of these forks were on the brief's in-scope list: **k8gb, terraform-controller, ca-injector, log4j-jndi-be-gone and tau-pi are forks.** A few carry a story worth one line each:

- **k8gb** began at Absa. The upstream k8gb-io/k8gb (created 2019-11-27, about 1.3k stars) still shows a historical `absaoss/k8gb` Docker Hub badge and says legacy `k8gb.absa.oss/v1beta1` resources are "automatically migrated to `k8gb.io/v1beta1`". It calls itself a "CNCF Incubating Project" ([k8gb-io/k8gb README](https://github.com/k8gb-io/k8gb)). The lesson is **donate to a neutral home and keep a migration path for the old API group**. The exact CNCF maturity level is as the README states it; I did not cross-check it on cncf.io (UNVERIFIED).
- **tau-pi** is a fork of Pi, which agentic-toolkit documents as a supported lean coding agent ([docs/tools/pi.md](https://github.com/AbsaOSS/agentic-toolkit/blob/master/docs/tools/pi.md)).
- The 2024 to 2026 Rancher, Cluster API and Karpenter forks sit next to Absa's own karpenter-provider-vsphere and cap-infra-dns. Together they point to an on-prem vSphere + RKE2 + Cluster API platform (inferred from the set; the internal platform itself is UNVERIFIED).

### 1b. Out of scope for data infrastructure (one line each)

| Repo | Problem it solves |
|---|---|
| rn-indy-sdk (archived) | React Native wrapper for Hyperledger Indy, now continued upstream ([README](https://github.com/AbsaOSS/rn-indy-sdk)) |
| vcxagencynode | NodeJS Aries/LibVCX mediator agency for self-sovereign identity; README carries a "Decommission notice" |
| aries-oob-shortener | URL shortener for Aries RFC0434 out-of-band messages |
| sovrin-networks | Registry of genesis files for Sovrin Indy networks |
| driver-did-sov | Universal Resolver driver for `did:sov` identifiers |
| dlt-cocoapods-specs | CocoaPods spec repo (mobile, DLT); empty README |
| manabu-ui / manabu-backend | MEAN-stack internal learning portal (video courses, admin roles, Keycloak) |
| ang-tools, microfrontends-poc (archived), cps-shared-ui, cps-mdoc-viewer | Angular component libraries, a Module Federation PoC, and an Angular markdown doc viewer. cps-shared-ui is actively maintained (dark mode #516, a11y fixes #563/#577) |
| inception | "Absa Inception Framework" Java + Angular app scaffold (2021 to 2022) |
| ScAPI (archived), pit-junit-cucumber-upstream | API and Cucumber test frameworks |
| hackathon-turbo | 2023 hackathon notebooks ("stock market trading signals using AI") |
| gitbook | Empty ("Initial page") |
| coredns-delegate | CoreDNS plugin that resolves non-authoritative requests |
| cert-manager-webhook-externaldns | cert-manager DNS01 solver that writes `DNSEndpoint` CRs for external-dns / k8gb CoreDNS |
| external-dns-infoblox-webhook | Infoblox provider for ExternalDNS, split out of the in-tree provider (28 stars, active 2026) |
| cap-infra-dns | Registers Cluster API control-plane endpoints in DNS. Out of scope in function, but its release pipeline is cited below |
| samlet | K8s operator that turns saml2aws SAML federation into rotating AWS credentials held in Secrets ("rotated 10 minutes before expiration") |
| aws-get-token | Go reimplementation of `aws eks get-token` so it can run in slim containers |
| CertMe | Generates a CSR, signs it via Vault, and imports it into ACM/SSM |
| provider-jet-rancher | Crossplane Rancher provider; README says "Moved" to crossplane-contrib |
| docker-distribution-artifactory, artifactory-registry-meta-generator | Docker registry distribution backed by Artifactory checksum storage, plus its metadata scraper. docker-distribution-artifactory is not a GitHub fork, but its README is the upstream distribution README (a detached copy; lineage UNVERIFIED) |
| homebrew-tap | Brew tap for older tool versions |
| gopkg | Small Go controller helper library (2020 to 2022) |

### 1c. In scope (deep or short dive below)

living-doc, living-doc-collector-gh, living-doc-collector-ad, living-doc-toolkit, living-doc-utilities, living-doc-generator-pdf, living-doc-generator-markdown, living-doc-generator-mdoc, generate-release-notes, version-tag-check, release-notes-presence-check, check-pr-requirements, filename-inspector, organizational-workflows, aquasec-scan-results (archived), GH-automation, reusable-workflows, validate-certificates, agentic-toolkit, daggerpool, knowledge-base, knowledge-base-docs-example, k3d-action, karpenter-provider-vsphere, golic, env-binder, go-k8s-operator-binder, pgdump-lambda, gh-pages-skeleton, root-pom, sbt-git-hooks, py-composite-action-lib, gh-action-upload-release-asset-composite, ghpages-to-tf-provider-registry, tf-provider-registry-generator, absaoss.github.io.

Totals: 41 forks + 30 out-of-scope rows (some rows group several repos) + 36 in scope = 106 repos.

## 2. Deep dives

### 2.1 daggerpool: a DAG worker pool for reconcile loops

Evidence: [README](https://github.com/AbsaOSS/daggerpool), [workerpool.go](https://github.com/AbsaOSS/daggerpool/blob/main/pkg/workerpool/workerpool.go), [job_result.go](https://github.com/AbsaOSS/daggerpool/blob/main/pkg/workerpool/job_result.go), [readiness/](https://github.com/AbsaOSS/daggerpool/tree/main/pkg/readiness), [workerpool_test.go](https://github.com/AbsaOSS/daggerpool/blob/main/pkg/workerpool/workerpool_test.go).

**Purpose.** It is a Go library, "not a framework", for running a job graph inside a controller. The fixtures name jobs `managementVanilla`, `managementTerraform` and `capacityTerraform0` (test file), so it was built to provision Kubernetes management and capacity clusters. The author is Michal Kuritka (commit log), who also leads the k8gb-era tooling. There are 7 commits between 2026-02-05 and 2026-02-20, tags v1.0.0 and v1.1.0, and 3 stars. The code is young and has a single author.

**Core design.**
- The job contract is `Do(ctx) (isReady bool, err error)`. The graph is a plain `map[string][]string` from a job to its dependencies.
- It schedules with Kahn-style dependency counters over a buffered channel and runs a fixed number of workers (`maxConcurrentJobs`), all under **one global timeout** (`context.WithTimeout`).
- There are **three outcomes per job, each with different blast radius**:
  - `err != nil` means `Failed`, and the **whole run is cancelled at once** ("On first failure we cancel the whole pipeline").
  - `isReady == false` means `InProgress` ("NotReady"). Its **successor subtree is marked `Skipped`**, but independent branches keep running.
  - A job that was in flight when cancellation came stays `Unknown`. The worker deliberately drops the result: "If Job was cancelled during execution I dont want to send anything into results Channel".
- The result views are the best part. `DAGResult.Frontiers(dag)` returns every non-successful job that is failed, in progress, or unblocked, with its `BlockedBy` list and its `Subtree`. `BoundarySubtrees` returns successful jobs that have unfinished work downstream. `FirstInProgress` answers "what is blocking readiness".
- The `readiness` package splits **Prober** (bounds a single check with its own timeout, and drops late results) from **Poller** (the period loop). The caller's context bounds the total wait. So there are three distinct timeouts: per check, poll period, and overall.

**What it does not do (verified in code).**
- **There is no retry anywhere in the source**, even though the repo description says "retries". Retry is left to the outer reconcile loop: NotReady is a normal outcome, and the controller simply calls `Start()` again later. This is level-triggered, idempotent-job design.
- **Cycle detection covers only self-loops.** `validate()` checks `dep == job`, and the test named "identify cycle" only uses a self-dependency (`capacityTerraform0: {…, capacityTerraform0}`). A multi-node cycle (A to B to A) would never reach zero remaining dependencies. Inferred from code: the run would idle until the global timeout and then report `timeout_error`, not a validation error.
- Jobs listed in `jobs` but missing from `dag` are not rejected. `totalJobs` counts the dag only.
- There is no persisted state. `pkg/store` is in-memory, and the README warns that it must not be "the source of truth for orchestration decisions".

**Maturity.** It has CI (`go test`, golangci-lint, yamllint), mocks via mockgen, a race test in the taskfile, and Renovate. It is a small, sound library that has not been hardened.

**Lesson for Ubunye task recovery.**
- Adopt the **5-state result vocabulary** (Success / NotReady / Skipped / Failed / Unknown), the **frontier-plus-blocked-by report**, and "skip the subtree, not the run" for soft failures.
- Put the frontier into the run receipt, so "resume from here" becomes a query on the receipt.
- Reject daggerpool's gaps: add real cycle detection at plan time (Ubunye already builds a plan), and record in-flight jobs as `cancelled` rather than `unknown`. A job that finished its side effect but whose result was dropped is exactly the case a resume must reconcile.
- Retry should be a declared per-task policy, as living-doc's R13 does (2.2), not implied by a description.

### 2.2 living-doc family: documentation as a mined, contract-checked pipeline

Evidence: [living-doc README](https://github.com/AbsaOSS/living-doc), [living-doc-utilities README](https://github.com/AbsaOSS/living-doc-utilities), [contracts.md](https://github.com/AbsaOSS/living-doc-utilities/blob/master/docs/contracts.md), [living-doc-toolkit](https://github.com/AbsaOSS/living-doc-toolkit), [collector-gh](https://github.com/AbsaOSS/living-doc-collector-gh), [collector-ad](https://github.com/AbsaOSS/living-doc-collector-ad), [generator-pdf](https://github.com/AbsaOSS/living-doc-generator-pdf), [generator-markdown](https://github.com/AbsaOSS/living-doc-generator-markdown), [generator-mdoc](https://github.com/AbsaOSS/living-doc-generator-mdoc).

**Purpose.** Documentation drifts from the system. Living-doc generates it "directly from the systems teams already use" (GitHub issues and Projects, Azure DevOps work items, source-code header blocks, Gherkin `.feature` files) instead of maintaining it by hand.

**Architecture (four stages).** Authoring (by hand or AI-assisted), then a collector that mines to JSON, then the toolkit that normalizes to a canonical dataset, then a generator that renders Markdown or PDF. The **entity model** is User Story, Feature and Functionality, each with versioned acceptance criteria written `AC:<id> (v<x.y.z> - <state>)`. The **document types** are technical project, test catalog, and a **coverage matrix** that cross-references acceptance criteria against Gherkin scenarios tagged `@AC:`. There are two views: *inner* shows everything, and *release* filters out `planned` and `in_review` ([living-doc README](https://github.com/AbsaOSS/living-doc)).

The project is honest about status. The README table says source-code mining is "Specced, not yet built" and Azure DevOps boards/pipelines are "Planned". The collector-gh doc-issues mode badge says "in development" ([collector-gh README](https://github.com/AbsaOSS/living-doc-collector-gh)). This was reorganised in 2025 to 2026: generator-mdoc dates from 2024 and the utilities, collector-gh and generator-pdf repos were created on 2025-04-22 (repo metadata).

**How truthfulness is enforced.** This is the core of it: [contracts.md](https://github.com/AbsaOSS/living-doc-utilities/blob/master/docs/contracts.md) is a numbered rulebook.

- **"The rationale is part of the contract. Several rules exist because a specific failure was observed in a real pipeline run."** Rule numbers are stable identifiers that code and error messages quote.
- **R4:** `schema_version` is a JSON Schema `const`, never an `enum`, because "an enum is how alias values accumulate". There are no deprecated aliases.
- **R5:** consumers check in a fixed order: id parses, then id is in the expected set, then structural validation. The error names both utilities versions and says "align the pins".
- **R6/R7:** every artifact carries `metadata.producer` (the tool that *wrote this file*) and `source_inputs[]` (the provenance of each input, with `stats` and `selected_stats`). "A difference between `stats` and `selected_stats` is filtering, while a difference between `selected_stats` and the output is loss."
- **R8:** legacy provenance shapes are removed with "no alias window". Readers are deleted in the same change, "removing the reader is what makes the retirement real".
- **R11:** every artifact carries `cardinality` and `field_occupancy` per record path. A transform declares an input-to-output **lineage table**. A mapped field with input occupancy above 0 and output 0 is a hard `FIELD_LOSS` error. Partial drops are reported downstream, not failed.
- **R12:** no repo may vendor a schema copy; CI enforces this with `check_no_vendored_schemas`. There is exactly one read path and one write path. `write_artifact` validates in memory, then writes to a temp file and does an atomic rename. A **full-sample test** populates every optional field so that "nothing was lost" means something.
- **R13:** "a collector collects everything it was configured for, or fails". The unit of failure is a configured source. The default is hard failure with **no output file at all**, because "writing a short file and exiting zero is what turns a collection outage into a silent documentation regression". There is an opt-in partial mode, and empty is not failed (`EMPTY_SOURCE` is a warning). The retry policy is shared: 5 attempts, exponential backoff from 2s with jitter, a 60s cap, a 15-minute cap for rate-limit resets, and 10s/60s connect/read timeouts, with an explicit table of retryable signals (429/5xx retry, 401 on first call fails at start, 404 no retry). The shared decorator "logs and re-raises every exception; it never turns a failure into `None`".
- **AI-free runtime.** "Every step … is deterministic tooling … with no LLM call anywhere in that path." AI (agentic-toolkit) only speeds up authoring, and "a human writing the same input by hand is a fully supported, identical path" ([utilities README](https://github.com/AbsaOSS/living-doc-utilities)).
- **Docs are tested like code.** living-doc runs `examples-check.yml`, which validates the examples corpus and self-tests the validator. `real-collector-snapshot.yml` runs the *real* pinned collector and toolkit over the doc examples weekly and on PRs, and fails on diff against `docs/examples/_expected/` ([workflows](https://github.com/AbsaOSS/living-doc/tree/master/.github/workflows)).
- **Versioning.** Utilities are pinned exactly (`==0.5.0`). The version stays `0.x` "on purpose" until the whole ecosystem reaches 1.0 together, with "one minor version across the ecosystem". The contract id version is independent of the package version.

**Issues.** Engagement is low. The most-commented generator-mdoc issue has 5 comments (#30, a refactoring issue). The work is maintainer-driven.

**Lesson for Ubunye.** This is the strongest transferable asset in the whole org:
- Ubunye's run receipt should adopt R4 to R7, R11 and R13 almost verbatim: a const contract id, producer versus source_inputs, per-output cardinality and field occupancy, field-loss detection across a transform, and "no partial output on source failure, unless opted in".
- R11's `stats` versus `selected_stats` distinction is a ready-made answer to "did the pipeline drop rows or filter them?"

### 2.3 Release discipline chain

Evidence: [generate-release-notes](https://github.com/AbsaOSS/generate-release-notes), [service_chapters.md](https://github.com/AbsaOSS/generate-release-notes/blob/master/docs/features/service_chapters.md), [motivation.md](https://github.com/AbsaOSS/generate-release-notes/blob/master/docs/motivation.md), [version-tag-check](https://github.com/AbsaOSS/version-tag-check), [version_validator.py](https://github.com/AbsaOSS/version-tag-check/blob/master/version_tag_check/version_validator.py), [qualifier-spec.md](https://github.com/AbsaOSS/version-tag-check/blob/master/docs/qualifier-spec.md), [release-notes-presence-check](https://github.com/AbsaOSS/release-notes-presence-check), [living-doc release_draft.yml](https://github.com/AbsaOSS/living-doc/blob/master/.github/workflows/release_draft.yml), [living-doc-utilities release.yml](https://github.com/AbsaOSS/living-doc-utilities/blob/master/.github/workflows/release.yml).

**How the pieces fit.**
1. **At PR time**, release-notes-presence-check requires a `Release Notes:` section in the PR body. `skip-labels` gives an escape hatch. `skip-placeholders` (for example `TBD`) makes a PR template's empty heading fail instead of passing.
2. **At release time** (a manual `workflow_dispatch` taking `tag-name` and an optional `from-tag-name`):
   - version-tag-check validates the new tag is `vX.Y.Z` and is **exactly the next valid increment**. That means a patch step of +1 within the same major.minor, a minor step of +1 with patch 0, or a major step of +1 with 0.0. It also allows backport patches on older series, and qualifier progression such as RC to RELEASE per the qualifier spec. With `should-exist=true` it instead checks that the from-tag exists.
   - The workflow then creates the tag via the API and runs generate-release-notes to build a **draft** release.
3. **Publishing the draft is the human approval gate.** A second workflow fires on `release: published`, **verifies the tag equals `pyproject.toml`'s version**, builds, and uploads to PyPI ([utilities release.yml](https://github.com/AbsaOSS/living-doc-utilities/blob/master/.github/workflows/release.yml)).

**generate-release-notes design.**
- It is label-to-chapter mapping plus extraction of the `Release Notes:` bullets from the issue body, then from the linked PR bodies. The CodeRabbit AI summary is only a fallback. The design principles are "Determinism", "Fail Safe … never silently fabricate content" and "Transparency".
- The key feature is **Service Chapters**, which report hygiene gaps *inside* the release notes:
  - closed issues without labels or without a PR
  - merged PRs without an issue
  - PRs merged against a still-open issue
  - **direct commits** to the default branch
  - an "Others - No Topic" catch-all
- Compare mode verifies both tags exist before calling the compare API. This came from bug #322/#324, a 404 when the target tag had not been created yet.
- Maturity: 16 releases, latest v1.3.3 on 2026-08-26 (releases API), 13 stars, on the GitHub Marketplace. The floating `v1` tag is moved by `update_v1_tag.yml` with `git tag -f v1 … && git push -f`.

**Failure modes this chain prevents:**
- a skipped or duplicated version
- a tag that doesn't match the built artifact
- releases whose notes are empty or say "TBD"
- orphan work (direct commits, unlinked PRs) disappearing from the changelog
- an accidental irreversible publish (only a draft is created until a human publishes)

**Lesson for Ubunye.** Adopt the whole shape:
- a draft release gate
- a tag-equals-pyproject check
- the monotonic tag check
- a release-notes section required on PRs
- service chapters, which suit a sole maintainer especially well because they audit your own shortcuts

Ubunye already has an OIDC setup, so it can do better than Absa's twine + `PYPI_API_TOKEN` secret by using PyPI trusted publishing.

### 2.4 check-pr-requirements and filename-inspector

- [check-pr-requirements](https://github.com/AbsaOSS/check-pr-requirements) is a pure-bash composite action with 8 toggleable checks: title format (conventional, `#123:` or custom regex), description length and required sections, issue reference (optionally only closing keywords, and Azure Boards `AB#`), release notes, branch name with ticket, PR size, labels, and target branch. It has skip-actors (Dependabot) and skip-labels. **All PR text reaches the scripts through `env:` (`INPUT_PR_TITLE: ${{ inputs.pr-title }}`), not by being interpolated into `run:`** ([action.yml](https://github.com/AbsaOSS/check-pr-requirements/blob/main/action.yml)). That is the correct defence against script injection from PR titles and bodies. There is a bash test suite per check, and an [`absa_aligned.yml`](https://github.com/AbsaOSS/check-pr-requirements/blob/main/examples/absa_aligned.yml) that encodes "ABSA git and change-integration guidelines": the Overview / Release Notes / Related PR template, and hotfixes to `support/*` and `release/*`.
- [filename-inspector](https://github.com/AbsaOSS/filename-inspector) enforces naming patterns such as `*UnitTest.*` and `*IntegrationTest.*` under `src/test`. The purpose is that test runners which select by suffix don't silently skip misnamed tests. It outputs a report in console, CSV or JSON.

**Lesson.** Adopt: a PR contract check plus a check that "tests are named so the runner can see them". Both catch problems that silently go missing (untested files, unlinked work).

### 2.5 organizational-workflows (policy as code), aquasec-scan-results, GH-automation, reusable-workflows

- The repo description of [organizational-workflows](https://github.com/AbsaOSS/organizational-workflows) says "templates, labels, policy examples", but the **only shipped solution is Security**. It fetches AquaSec findings through the API (HMAC-signed auth), normalises them with a **stable fingerprint**, and turns them into GitHub Issues. New findings create issues, recurring findings update them, reappearing findings **reopen** closed issues, and resolved findings are **auto-closed**. Parent epics per rule auto-close when their children do. Severity maps to priority on a ProjectV2 board. One Teams card is sent per run, and only on change ([security.md](https://github.com/AbsaOSS/organizational-workflows/blob/master/docs/security/security.md)).
  - Hidden metadata sits in an HTML comment (`<!--secmeta type=child fingerprint=… rule_id=… -->`), so the issue body is machine-reconcilable.
  - **Waivers are human-only.** `sec:suppression` and `sec:false-positive` labels are applied by people, and "Pipeline never adds, removes, or reads these labels".
  - `--dry-run` was added in #48.
  - It is distributed as a **versioned reusable workflow**. Issue search shows Dependabot PRs bumping `organizational-workflows/.github/workflows/aquasec-scan.yml` from 1.0.0 to 1.1.0 to 1.2.0 to 1.3.0 across about 11 repos: EventGate, StatusBoard, atum-service, knowledge-base and the living-doc repos ([search](https://github.com/search?q=org%3AAbsaOSS+%22organizational-workflows%2F.github%2Fworkflows%22&type=pullrequests)).
- [aquasec-scan-results](https://github.com/AbsaOSS/aquasec-scan-results) is **archived** (2026). It converted findings to SARIF for the Security tab and had a branch-compare mode that fails a PR on *new* findings. Its role moved into organizational-workflows (inferred from the archive date and the consumers above).
- [GH-automation](https://github.com/AbsaOSS/GH-automation) is essentially an idea: a README plus a workflow that calls the third-party `z0al/dependent-issues` action with a PAT (`PAT_REPO_PROJECT_DISCUSS`). There is no action code.
- [reusable-workflows](https://github.com/AbsaOSS/reusable-workflows) holds only a README and LICENSE (2021). It is an abandoned start.

**Lesson.**
- Adapt the fingerprint, reopen and auto-close lifecycle for Ubunye's data-quality or run-failure findings. A recurring pipeline failure should reopen the same issue, not open a new one each time.
- Keep waivers human-only.
- Ship org policy as versioned reusable workflows that consumers pin, with Dependabot bumping them.

### 2.6 validate-certificates

A [composite action](https://github.com/AbsaOSS/validate-certificates) that checks a JSON array of certificate files with `openssl` and writes a Step Summary table. It works on GNU and BSD `date`. The clever part: certificates are **grouped by subject**, so an expired certificate that already has a valid replacement counts as informational, and the job fails only on "truly unrecoverable expirations". It has a `warning_days` setting and `fail_on_warn`.

**Lesson.** Minor. The general pattern "fail only on unrecoverable states, and summarise the rest in the Step Summary" is a good model for Ubunye's CI output.

### 2.7 agentic-toolkit: what Absa builds for agents

Evidence: [README](https://github.com/AbsaOSS/agentic-toolkit), [skill-testing.md](https://github.com/AbsaOSS/agentic-toolkit/blob/master/docs/testing/skill-testing.md), [responsible-agent-use.md](https://github.com/AbsaOSS/agentic-toolkit/blob/master/docs/responsible-agent-use.md), [test-scripts.yml](https://github.com/AbsaOSS/agentic-toolkit/blob/master/.github/workflows/test-scripts.yml).

**What it is.**
- About 25 **Agent Skills** (the agentskills.io format), installed with `npx skills add`. Examples: `pr-review`, `tdd-workflow`, `test-unit-*`, `create-repository` (an interview, then an annotated `gh` plan executed on confirmation), `token-saving`, and a large `living-doc-*` / Gherkin / PageObject family.
- One agent persona, `@living-doc-bdd-copilot`.
- A layering model: shared base, then personal, then project skills.
- The scope statement is explicit: skills now, and "MCP Servers … Plugins" only "as our use of agentic tooling matures".

**Engineering practice.**
- Every skill carries `evals/evals.json` and often `trigger-eval.json` plus fixture files. The testing guide describes a regression-first loop ("fix the smallest part of the skill that explains the largest failure cluster") and with-skill versus without-skill comparisons.
- Deterministic helper scripts inside skills (for example `find_unused_steps.py`) have `test_*.py` files, which CI discovers and runs.
- The LLM evals themselves are run manually in a Copilot CLI session. They are not in CI (inferred from the guide and workflow).
- A cost-awareness doc explains usage-based Copilot billing "as of June 1, 2026" and the input, output and cached token economics.
- There is activity: 10 open issues, and #38 and #34 have 6 to 7 comments each.

**Repo-level agent scaffolding across the org.** Many new repos ship `.github/agents/*.agent.md` role boards. version-tag-check has architect, ba, pm, senior-dev, tester and others; living-doc has specification-master, senior-developer, sdet, reviewer and devops-engineer. They also carry `copilot-instructions.md` and `copilot-review-rules.md` written in a constrained "Must / Must not / Prefer / Avoid" style. living-doc even has a `.claude/` folder with `/implement-task` and `/verify-pr-ready` commands and states "These helpers are acceleration, not dependency … Nothing in CI or the release path requires them" ([.claude/README.md](https://github.com/AbsaOSS/living-doc/blob/master/.claude/README.md)). The copilot instructions also tell agents to treat the PR body as an append-only changelog ([copilot-instructions.md](https://github.com/AbsaOSS/generate-release-notes/blob/master/.github/copilot-instructions.md)).

**Comparison with Ubunye's MCP server.**
- Absa is at the "books" layer, in its own words: "skills as books, agents as the people who read them, and MCP servers as the phone lines". Ubunye already ships the "phone line".
- Two things to borrow:
  1. **Eval files next to each agent-facing capability.** Each MCP tool should get realistic prompts, expected outcomes and fixtures.
  2. **The "AI accelerates authoring, never on the runtime path" rule** with a hand-written equivalent path. That matches Ubunye's cost-bounded, replayable LLM-step direction.
- Also worth shipping: a small Ubunye skill (SKILL.md) that teaches agents how to call the MCP server. That is cheaper for adopters than a plugin.

### 2.8 knowledge-base and knowledge-base-docs-example

[knowledge-base](https://github.com/AbsaOSS/knowledge-base) (created 2026-06) is a **build-time aggregator**. It pulls each registered doc app's `kb-docs.tar.gz` from GitHub Releases, rewrites URLs under `/knowledge-base/{slug}/`, wraps each app in a common masthead, generates a catalog, and serves the result with nginx or as a web fragment. It makes "no third-party requests at runtime" because fonts and mermaid are vendored, so it works behind restricted egress. It has a "Contract v1" (#74, #75: `kb-docs.json` is the source of truth) and a reusable `publish-docs` action (#76). The [docs example](https://github.com/AbsaOSS/knowledge-base-docs-example) repo runs "the same contract checks on every pull request" and publishes on release.

**Lesson.** Adapt: publish docs as a release artifact with a manifest, and check the manifest on PRs. The self-hosted, no-CDN rule matters for bank-style restricted networks, which is also Ubunye's enterprise target.

### 2.9 Kubernetes and platform tooling

- **[k3d-action](https://github.com/AbsaOSS/k3d-action)** is the org's most-starred repo in this slice (203). It runs ephemeral single or multi-cluster k3s in GitHub Actions and was built for k8gb operator E2E tests. It **pins the k3d version per action release** in a mapping table ("external dependencies get broken"). The most-discussed issue, #14 with 11 comments, asked for a wait-until-ready option. The last release was v2.4.0 on 2023-01-12, so it is dormant. **Lesson:** Ubunye uses kind; adopt the version-mapping table and a built-in readiness wait.
- **[karpenter-provider-vsphere](https://github.com/AbsaOSS/karpenter-provider-vsphere)** is active (2026-09-24) and labelled "Early alpha - NOT for Production use". It is a Karpenter cloud provider for vSphere: `VsphereNodeClass` CRD selectors, RKE2 as the first-class distro, ignition or cloud-config. **Drift detection** compares a nodeclass hash plus a *hash-version* annotation. If the versions differ it reports no drift, so changing the hash algorithm does not trigger a fleet-wide replacement ([drift.go](https://github.com/AbsaOSS/karpenter-provider-vsphere/blob/main/pkg/cloudprovider/drift.go)). This mirrors upstream Karpenter providers (UNVERIFIED which one it was copied from). **Lesson:** adopt "config hash + hash-scheme version" for Ubunye's plan or config fingerprint in receipts.
- **[golic](https://github.com/AbsaOSS/golic)** (106 stars, last push 2021) injects license headers declaratively. `.licignore` uses gitignore syntax with a deny-all default, an embedded master `.golic.yaml` can be overridden, and CI runs `--dry`. See anti-pattern 5 below for its duplicate-header problem.
- **[env-binder](https://github.com/AbsaOSS/env-binder)**, now deprecated in favour of **[go-k8s-operator-binder](https://github.com/AbsaOSS/go-k8s-operator-binder)**, binds env vars *and* K8s annotations and labels to Go struct tags with `default=`, `require=true` and `protected=true`. The deprecation notice says the successor is "fully backwards compatible". **Lesson:** a clean deprecation handoff.
- **[pgdump-lambda](https://github.com/AbsaOSS/pgdump-lambda)** is a Terraform-deployed Lambda that runs `pg_dump` to S3 on an EventBridge schedule, with credentials from Secrets Manager and tags from SSM common values. It **commits the `pg_dump` binary and `libcrypto.so.3` / `libssl.so.3` into git** ([tree](https://github.com/AbsaOSS/pgdump-lambda/tree/master/lambda/bin)).
- **[cap-infra-dns](https://github.com/AbsaOSS/cap-infra-dns)** has the best-engineered Go release pipeline in the org: release-please, then GoReleaser with **cosign** and **Syft SBOM**, pushed to ghcr, with `id-token: write`, plus gitleaks and golic checks ([release.yaml](https://github.com/AbsaOSS/cap-infra-dns/blob/main/.github/workflows/release.yaml)).

### 2.10 Build and publishing scaffolding

- **[root-pom](https://github.com/AbsaOSS/root-pom)** (still maintained, 2026-09) is a parent POM whose profiles are independent concerns: `-Drelease`, `-Dlicense-check`, `-Dcode-coverage`, `-Dossrh`, `-Ddocker`. They are activated with `-D`, not `-P`, so they combine freely. Only `release:prepare` is used, never `release:perform`, and CI publishes. **Lesson:** adapt as one shared config package for Ubunye's repos, where each concern is a separate opt-in toggle.
- **[sbt-git-hooks](https://github.com/AbsaOSS/sbt-git-hooks)** versions git hooks in `project/git_hooks` and syncs them into `.git/hooks`. It is the sbt equivalent of pre-commit.
- **[gh-pages-skeleton](https://github.com/AbsaOSS/gh-pages-skeleton)** (Jekyll, 2019 to 2023) is a docs starter with versioned doc folders and a Ruby release-notes script (ZenHub). It is the ancestor of generate-release-notes and knowledge-base.
- **[ghpages-to-tf-provider-registry](https://github.com/AbsaOSS/ghpages-to-tf-provider-registry)** + **[tf-provider-registry-generator](https://github.com/AbsaOSS/tf-provider-registry-generator)** (2021) turn GitHub Pages into a private Terraform provider registry: GoReleaser, then GPG signing, then generated registry metadata committed to `gh-pages`. **Lesson:** using a static host as a package registry is a cheap pattern if Ubunye ever needs a private plugin index.
- **[py-composite-action-lib](https://github.com/AbsaOSS/py-composite-action-lib)** and **[gh-action-upload-release-asset-composite](https://github.com/AbsaOSS/gh-action-upload-release-asset-composite)** are 2021 and 2024 stubs. living-doc-utilities has since made `get_action_input` / `set_action_output` shared library code.
- **[absaoss.github.io](https://github.com/AbsaOSS/absaoss.github.io)** is the org landing site (CSS, 2026-01).

## 3. Matrix

| Practice / problem | Evidence | AbsaOSS repo | Ubunye relevance | Verdict |
|---|---|---|---|---|
| Const contract id, no aliases (R4) | [contracts.md](https://github.com/AbsaOSS/living-doc-utilities/blob/master/docs/contracts.md) | living-doc-utilities | Receipt / config schema id | Adopt |
| Fixed-order input check with named version skew (R5) | same | living-doc-utilities | Config and receipt loading errors | Adopt |
| Producer versus source_inputs provenance (R6/R7) | same | living-doc-utilities | Run receipt lineage (pairs with OpenLineage) | Adopt |
| Per-artifact cardinality + field occupancy, FIELD_LOSS (R11) | same | living-doc-utilities, living-doc-toolkit | Row and column loss detection per task | Adapt |
| Whole-source-or-fail, no partial output, opt-in partial mode (R13) | same | collectors | Task output semantics on source failure | Adopt |
| One declared retry policy with a signal table | same | living-doc-utilities | Per-connector retry config | Adopt |
| Atomic temp-file-then-rename writes | [utilities README](https://github.com/AbsaOSS/living-doc-utilities) | living-doc-utilities | Receipt and state writes | Adopt |
| Full-sample test populating every optional field | contracts.md R12 | living-doc-utilities | Schema coverage tests | Adopt |
| Snapshot docs against the real pinned tool, weekly | [real-collector-snapshot.yml](https://github.com/AbsaOSS/living-doc/blob/master/.github/workflows/real-collector-snapshot.yml) | living-doc | Docs examples run by CI | Adopt |
| AI on authoring only, deterministic runtime | [utilities README](https://github.com/AbsaOSS/living-doc-utilities) | living-doc, agentic-toolkit | LLM-step boundary | Adopt |
| 5-state job result, skip subtree on NotReady, frontiers | [job_result.go](https://github.com/AbsaOSS/daggerpool/blob/main/pkg/workerpool/job_result.go) | daggerpool | Task recovery report | Adapt |
| Prober / Poller split (per-check versus total timeout) | [readiness](https://github.com/AbsaOSS/daggerpool/tree/main/pkg/readiness) | daggerpool | Wait-for-cluster / wait-for-table | Adopt |
| Fail-fast cancel dropping in-flight results | [workerpool.go](https://github.com/AbsaOSS/daggerpool/blob/main/pkg/workerpool/workerpool.go) | daggerpool | Anti-lesson: record cancelled tasks | Ignore (do the opposite) |
| Monotonic semver tag check | [version_validator.py](https://github.com/AbsaOSS/version-tag-check/blob/master/version_tag_check/version_validator.py) | version-tag-check | Release workflow | Adopt |
| Release notes required on PR, placeholders fail | [README](https://github.com/AbsaOSS/release-notes-presence-check) | release-notes-presence-check | PR gate | Adopt |
| Label chapters + service chapters (direct commits, orphans) | [service_chapters.md](https://github.com/AbsaOSS/generate-release-notes/blob/master/docs/features/service_chapters.md) | generate-release-notes | Changelog that audits a sole maintainer | Adopt (use the action directly) |
| Draft release as approval gate, tag equals pyproject check | [release.yml](https://github.com/AbsaOSS/living-doc-utilities/blob/master/.github/workflows/release.yml) | living-doc-utilities | PyPI release | Adopt (with OIDC trusted publishing) |
| PR contract checks, inputs via env | [action.yml](https://github.com/AbsaOSS/check-pr-requirements/blob/main/action.yml) | check-pr-requirements | Contributor PRs | Adapt |
| Test-name suffix enforcement | [README](https://github.com/AbsaOSS/filename-inspector) | filename-inspector | pytest discovery | Ignore (pytest config covers it) |
| Findings to issues: fingerprint, reopen, auto-close, human waivers | [security.md](https://github.com/AbsaOSS/organizational-workflows/blob/master/docs/security/security.md) | organizational-workflows | DQ / failure backlog | Adapt |
| Org policy as versioned reusable workflows, bumped by Dependabot | [search](https://github.com/search?q=org%3AAbsaOSS+%22organizational-workflows%2F.github%2Fworkflows%22&type=pullrequests) | organizational-workflows | ubunye-ai-ecosystems org | Adopt |
| Skills with evals + trigger evals | [skill-testing.md](https://github.com/AbsaOSS/agentic-toolkit/blob/master/docs/testing/skill-testing.md) | agentic-toolkit | MCP tool evals, Ubunye skill | Adopt |
| Constrained-keyword agent rules files | [copilot-review-rules.md](https://github.com/AbsaOSS/living-doc/blob/master/.github/copilot-review-rules.md) | living-doc et al. | CLAUDE.md / AGENTS.md style | Adapt |
| Release-artifact docs + manifest contract, no CDN | [knowledge-base](https://github.com/AbsaOSS/knowledge-base) | knowledge-base | Docs publishing | Adapt |
| Ephemeral k8s in CI with pinned tool version map | [k3d-action](https://github.com/AbsaOSS/k3d-action) | k3d-action | kind tests | Adapt |
| Config hash + hash-version drift | [drift.go](https://github.com/AbsaOSS/karpenter-provider-vsphere/blob/main/pkg/cloudprovider/drift.go) | karpenter-provider-vsphere | Plan fingerprint in receipt | Adopt |
| cosign + SBOM + release-please | [release.yaml](https://github.com/AbsaOSS/cap-infra-dns/blob/main/.github/workflows/release.yaml) | cap-infra-dns | Supply chain for wheels and images | Adapt |
| Independent opt-in build profiles | [root-pom](https://github.com/AbsaOSS/root-pom) | root-pom | Shared tooling config | Adapt |
| Donate the project, keep a migration path for the old API group | [k8gb-io/k8gb](https://github.com/k8gb-io/k8gb) | k8gb (fork) | Long-term governance | Adapt (later) |
| Env / annotation struct binding | [go-k8s-operator-binder](https://github.com/AbsaOSS/go-k8s-operator-binder) | env-binder successor | None (Python) | Ignore |
| Private TF registry on Pages | [ghpages-to-tf-provider-registry](https://github.com/AbsaOSS/ghpages-to-tf-provider-registry) | tf registry tools | Maybe a plugin index | Ignore for now |

## 4. Top 10 transferable practices (ranked)

1. **"Whole source or fail, and never write a short file" (R13)**, with an opt-in partial mode and `sources_configured` / `sources_failed` counts carried to the final output. This prevents the worst data-pipeline failure: a silent partial success. [contracts.md](https://github.com/AbsaOSS/living-doc-utilities/blob/master/docs/contracts.md)
2. **Self-describing artifacts.** A const contract id, a `producer` (the writer) separate from `source_inputs[]` (upstream provenance), and a `warnings[]` array forwarded through every stage. This maps directly onto Ubunye's run receipt. [contracts.md](https://github.com/AbsaOSS/living-doc-utilities/blob/master/docs/contracts.md)
3. **Stats plus lineage-table field-loss checks**, with `stats` versus `selected_stats` to tell filtering apart from loss. It is cheap to compute and answers "where did my column go". [contracts.md](https://github.com/AbsaOSS/living-doc-utilities/blob/master/docs/contracts.md)
4. **Draft-then-publish release, with tag, version and notes gates.** Use version-tag-check, the tag equals pyproject check, and release-notes-presence-check with placeholder rejection, then publish only when a human publishes the draft. [utilities release.yml](https://github.com/AbsaOSS/living-doc-utilities/blob/master/.github/workflows/release.yml)
5. **Service chapters in generated release notes.** Direct commits, orphan PRs and unlabeled issues show up in the changelog itself. For a sole maintainer this is the missing reviewer. [service_chapters.md](https://github.com/AbsaOSS/generate-release-notes/blob/master/docs/features/service_chapters.md)
6. **A structured DAG result with frontiers and blocked-by**, plus the skip-subtree / cancel-all distinction. Adopt the vocabulary, add real cycle detection, and record cancelled tasks. [daggerpool](https://github.com/AbsaOSS/daggerpool)
7. **One declared retry and timeout policy with a signal table.** Retry 429/5xx and honour Retry-After, never retry 401/404, fail at start on configuration errors, and use separate connect and read timeouts. Never turn exceptions into `None`. [contracts.md](https://github.com/AbsaOSS/living-doc-utilities/blob/master/docs/contracts.md)
8. **Docs examples executed by CI against the real pinned tool**, on a weekly schedule to catch drift between PRs. [real-collector-snapshot.yml](https://github.com/AbsaOSS/living-doc/blob/master/.github/workflows/real-collector-snapshot.yml)
9. **Eval files for every agent-facing capability, and AI kept off the runtime path.** Ubunye's MCP tools should ship `evals.json` with realistic prompts and fixtures. [agentic-toolkit](https://github.com/AbsaOSS/agentic-toolkit)
10. **Org policy as versioned reusable workflows** that consumers pin and Dependabot bumps. Apply it to the ubunye-ai-ecosystems org's security and PR gates. [organizational-workflows](https://github.com/AbsaOSS/organizational-workflows)

## 5. Anti-patterns observed

1. **Descriptions that promise more than the code does.**
   - daggerpool's description says "retries", but there is no retry code ([workerpool.go](https://github.com/AbsaOSS/daggerpool/blob/main/pkg/workerpool/workerpool.go)).
   - organizational-workflows says "templates, labels, policy examples", but ships only security automation ([README](https://github.com/AbsaOSS/organizational-workflows)).
   - reusable-workflows is empty ([repo](https://github.com/AbsaOSS/reusable-workflows)). GH-automation is a README plus a third-party action call ([repo](https://github.com/AbsaOSS/GH-automation)).

   To be fair, living-doc does this right with its "Specced, not yet built" status column.
2. **A test named for a case it does not cover.** daggerpool's "identify cycle" test only checks a self-loop. Inferred from code, multi-node cycles surface as timeouts ([test](https://github.com/AbsaOSS/daggerpool/blob/main/pkg/workerpool/workerpool_test.go)).
3. **A long-lived PyPI token instead of trusted publishing** (`TWINE_PASSWORD: ${{ secrets.PYPI_API_TOKEN }}`). The same release job also checks an installer against a checksum downloaded from a `latest` URL, which limits what the check can prove ([release.yml](https://github.com/AbsaOSS/living-doc-utilities/blob/master/.github/workflows/release.yml)).
4. **Inconsistent action pinning.** Some steps are pinned to a SHA (`actions/checkout@3d3c42e…`) while others in the same file use tags (`actions/setup-python@v7`). Callers carry `@v1.3.0 # TODO: pin to the newest release SHA` and `project-number: 0 # TODO` ([living-doc aquasec-night-scan.yml](https://github.com/AbsaOSS/living-doc/blob/master/.github/workflows/aquasec-night-scan.yml)). The floating `v1` tag is force-pushed ([update_v1_tag.yml](https://github.com/AbsaOSS/generate-release-notes/blob/master/.github/workflows/update_v1_tag.yml)). That is common practice, but the tag is mutable.
5. **License headers stacking up.** cap-infra-dns `release.yaml` carries three stacked golic headers, one of them naming *external-dns-infoblox-webhook*, a sign of copy-paste plus re-injection ([release.yaml](https://github.com/AbsaOSS/cap-infra-dns/blob/main/.github/workflows/release.yaml)). The `golic inject --dry` CI check did not catch it.
6. **Binaries committed to git.** pgdump-lambda commits `pg_dump`, `libssl.so.3` and `libcrypto.so.3` with no provenance or checksum ([tree](https://github.com/AbsaOSS/pgdump-lambda/tree/master/lambda/bin)).
7. **Personal access tokens where the built-in token or an app would do.** GH-automation uses `PAT_REPO_PROJECT_DISCUSS` for both write and read ([dependent_items.yml](https://github.com/AbsaOSS/GH-automation/blob/master/.github/workflows/dependent_items.yml)).
8. **Abandonment without archiving.** k3d-action (203 stars, last release 2023-01), golic (last push 2021), and the gopkg / py-composite-action-lib / gh-action-upload-release-asset-composite stubs are neither archived nor marked as unmaintained. By contrast, env-binder's deprecation notice and aquasec-scan-results' archival are the right way to do it.
9. **Very low outside engagement.** The most-commented issues in the new governance repos have 0 to 7 comments (issue search, for example [version-tag-check #43](https://github.com/AbsaOSS/version-tag-check/pull/43) with 6). These are internal tools published in the open, not community projects. So their design quality has not been tested by outside users, and adoption claims are UNVERIFIED beyond the Dependabot-bump evidence.
10. **Heavy per-repo process scaffolding.** Nine issue templates, 5 to 8 agent persona files and Copilot rules are copied into every new repo, even single-purpose actions (see the version-tag-check tree: [.github/agents](https://github.com/AbsaOSS/version-tag-check/tree/master/.github/agents)). For a sole maintainer, copy one reviewer rules file, not the whole board. For Absa this is reasonable at team scale, but copies drift. living-doc's `.claude/README.md` already records "not carried over" deltas between copies.
