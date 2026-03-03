# Contributing to Ubunye Engine

Thanks for your interest in contributing! This guide explains how to propose changes, file issues, and submit pull requests (PRs).

---

## Code of Conduct
By participating, you agree to uphold our Code of Conduct (see `CODE_OF_CONDUCT.md`).

---

## Ways to contribute
- 🐛 **Report bugs:** include repro steps, stack traces, versions, and OS/env details.
- 💡 **Feature requests:** explain the problem, proposed API/config, and trade-offs.
- 🧪 **Tests:** add unit/integration tests for new or changed behavior.
- 📚 **Docs:** improve docs and docstrings; examples are welcome.
- 🔌 **Plugins:** new Readers/Writers/Transforms/ML backends via entry points.

---

## Development workflow (TL;DR)
1. **Fork** the repo and create a branch: `git checkout -b feat/my-change`.
2. **Set up dev env:**
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -U pip
   pip install -e .[dev,spark,ml]
   pre-commit install
    ```
3. Run tests & linters:
   ```python
    pytest -q
    ruff check .
    black --check .
   ```
4. Make changes with docstrings + type hints.
5. Add/Update tests and docs.
6. Commit using conventional commits:
   - ``feat(core): add transform pipeline``
   - ``fix(reader/jdbc): handle missing driver``
   - ``docs(cli): document export command``
7. Push and open a PR against ``main``.
_____________________

### Project structure (high level)
```bash
ubunye/
  core/            # engine, interfaces, config schema/loader
  backends/        # Spark backend(s)
  plugins/
    readers/       # hive, jdbc, delta, unity, ...
    writers/       # s3, jdbc, delta, unity, ...
    transforms/    # noop + custom
    ml/            # base.py, adapters.py, sklearn.py, torch.py, pysparkml.py
  orchestration/   # base + exporters (airflow, databricks)
  telemetry/       # events, prometheus, otel
docs/
tests/
```
_____________________

### Pull requests
- Keep PRs small and focused (≤ 500 LOC preferred).

- Include motivation, design summary, and breaking changes (if any).

- Add tests and doc updates.

- Ensure CI is green (formatting, lint, tests).

### PR checklist
- Feature/bug motivation described
- Unit tests added/updated
- Docs updated (``docs``/ or docstrings)
- ``CHANGELOG.md`` entry (if user-visible)
- ``pyproject.toml`` entry points updated (if new plugin)
- Backwards compatibility considered
_____________________________________

### Testing

- Unit tests live in ```tests/`` mirroring package paths.

- Prefer fast tests; mark slow/integration with ```@pytest.mark.slow.

- Spark tests: use ``local[*]`` and small toy datasets.

- Databricks-related logic should be exporter-only and unit-testable without a cluster.

### Style & quality

- Type hints everywhere; from ```__future__ import annotations```.

- Docstrings (Google or NumPy style).

- Black for formatting, Ruff for linting.

- Avoid hard dependencies on cloud providers or ML frameworks; keep them in extras.

### Backwards compatibility

- Don’t break public APIs lightly. If needed:

- Deprecate with warnings first.

- Document migration steps.

- Bump minor/major version accordingly.

### Security

- Never commit secrets; use env vars and secret managers.

- Report vulnerabilities privately via security contact in ```SECURITY.md``.

### Release process (maintainers)

- Update ```CHANGELOG.md``.

- Bump version in ```pyproject.toml/setup.py``.

- Tag: ```git tag vX.Y.Z && git push --tags``.

- Build & publish: ```python -m build && twine upload dist/*``.

Thanks again for contributing!
