# Contributing to Ubunye Engine

Thank you for helping. Ubunye is built in the open, from Africa, for anyone who
runs data and ML pipelines. This page is everything you need to make a change
that gets merged.

## Ways to help

- **Report a bug**: what you ran, what you expected, what happened (the full
  error), and your Python, OS, and Spark or pandas versions.
- **Ask for a feature**: the problem first, then the config or API you would
  like, and what it would cost.
- **Fix something**: bugs, docs, tests, examples.
- **Add a plugin**: a reader, writer, transform, hook or a whole backend, from
  your own package, with no change to the engine. See
  [Execution Backends](https://ubunye-ai-ecosystems.github.io/ubunye_engine/backends/)
  and the [plugin guide](https://ubunye-ai-ecosystems.github.io/ubunye_engine/connectors/plugin_guide/).

Issues and pull requests: <https://github.com/ubunye-ai-ecosystems/ubunye_engine>.

## Set up

```bash
git clone https://github.com/ubunye-ai-ecosystems/ubunye_engine.git
cd ubunye_engine
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -U pip
pip install -e ".[dev]"
pre-commit install
```

`[dev]` includes pandas and pyarrow, so the whole unit tier runs with no Java.

## Run the tests

| Tier | Command | Needs |
|---|---|---|
| Unit | `pytest tests/unit -m "not integration"` | nothing else |
| Spark | `pytest tests/integration` | Java 17 or 21 and `pip install -e ".[spark,delta]"` |
| Types | `python -m mypy ubunye` | nothing else |
| Lint | `pre-commit run --all-files` | nothing else |
| Docs | `pip install -r docs/requirements.txt && mkdocs build --strict` | nothing else |
| Performance | `python benchmarks/guard.py --base <checkout of the base branch>` | a second checkout (`git worktree add ../base origin/main`) |

The Spark tier includes the parity suite, which runs the same data through
Spark and pandas and checks they agree on types, values, files and run
records. Any change to how data is read, written or hashed must keep it green.
It also runs the backend conformance suite (`ubunye.testing.backend_conformance`,
which a new backend runs too) and a distributed check: one task on a single
core and on a two-executor Spark must leave the same run record, with the
driver's result size capped far below the data, so nothing may pull rows back
to the driver.

CI also times every pull request against the branch it goes into
(`benchmarks/guard.py`) and fails it if an operation gets more than 30% slower.

## How a change is made

1. **Write the failing test first.** It should fail for the reason you are
   fixing, then pass with the fix. A change without a test is not finished.
2. **One commit per fix or feature**, with a message that says what was wrong
   and why this is the right fix, in the
   [Conventional Commits](https://www.conventionalcommits.org/) style
   (`fix(pandas): ...`, `feat(cli): ...`, `docs: ...`).
3. **Docs and changelog in the same commit.** If users would notice the change,
   update the page they would read and add a line under `## [Unreleased]` in
   `docs/changelog.md`.
4. **Never skip the hooks** (`--no-verify`). If a hook fails, fix the cause.
5. **Nothing that works today breaks.** Configs, CLI flags, Python calls and
   notebooks keep working; anything renamed keeps its old name with a
   deprecation warning for at least one release.
6. **The core stays engine free.** Code in `ubunye/core` never imports Spark,
   pandas or pyarrow (a test checks every import). Engines are adapters,
   found through entry points.

Then open a pull request against `main` and say what changed, why, and how you
tested it. CI runs the unit tier on Python 3.10 to 3.13 on Linux, Windows and
macOS, and again on the oldest versions of every dependency the package accepts;
the Spark tier runs on Spark 3.5 and Spark 4.

## Writing style

Plain language, short sentences. Explain a term the first time. Say what the
code does and why, not how clever it is. Error messages say what went wrong,
where, and what to try.

## Releases

Maintainers release by bumping the version on `main`; the publish to PyPI waits
for a person to approve it.
