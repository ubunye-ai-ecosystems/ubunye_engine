# Gate pull requests on the run receipt

`ubunye gate` compares a run's receipt with a baseline and fails when it
regresses. In CI the baseline is the task run on the pull request's base, the
candidate the same task on its head.

```bash
ubunye gate -d pipelines -u shop -p orders -t clean                     # previous run vs latest
ubunye gate --baseline base.json --candidate head.json                  # two exported records
ubunye gate ... --max-slowdown 0.5 --max-row-change 0.1 --summary "$GITHUB_STEP_SUMMARY"
```

`--baseline` and `--candidate` take a run record file (`ubunye lineage show
--json` writes one), a run id (or its first characters), `previous` or `latest`.

## What fails the gate

| Rule | Fails when |
|---|---|
| run | the candidate run did not succeed |
| expectation | a `fail` [expectation](../config/expectations.md) was broken |
| data | an output's data changed and the task's `VERSION` did not |
| schema | an output's columns or types changed and `VERSION` did not |
| output | an output is missing from the candidate |
| time | slower than the baseline by more than `--max-slowdown` (runs under 1 s are noise), or longer than `--max-seconds` |
| rows | an output's row count moved by more than `--max-row-change` |

A deliberate change bumps `VERSION` in the task's `config.yaml`, or passes
`--allow-data-change` (then it warns). Every changed output also says what else
changed between the runs (config, code, environment, which inputs); a change
with none of them is reported as a transform that is not deterministic.

Warnings never fail the gate: a `warn` expectation broken, rows quarantined, a
new output, hashes that cannot be compared (a record from before 0.6.0, or an
unreadable output).

Exit code 1 when the gate fails; `--json` prints the findings; `--summary FILE`
appends a Markdown table.

## The GitHub Action

```yaml
on: pull_request

jobs:
  gate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: ubunye-ai-ecosystems/ubunye_engine/.github/actions/gate@v0.7.0
        with:
          usecase-dir: pipelines
          usecase: shop
          package: orders
          task: clean
          gate-args: --max-slowdown 0.5
```

It installs the engine, runs the task on the pull request's base and on its head
(pandas by default, no Java needed), gates head against base, and writes the
table to the job summary. Inputs: `backend`, `engine` (a pip requirement),
`extra-packages`, `baseline-ref` (default: the pull request's base), `gate-args`,
`python-version`. Output: `passed`.
