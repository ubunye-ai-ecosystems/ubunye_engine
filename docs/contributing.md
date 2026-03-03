# Contributing

Thanks for your interest in contributing to Ubunye Engine!

---

## Ways to contribute

- **Report bugs** — include repro steps, stack traces, Python/Spark/OS versions.
- **Feature requests** — describe the problem, proposed API or config, and trade-offs.
- **Tests** — add unit or integration tests for new or changed behaviour.
- **Docs** — improve pages, fix typos, add examples.
- **Plugins** — new Readers, Writers, Transforms, or Monitors via entry points.

Open issues and PRs at [github.com/ubunye-ai-ecosystems/ubunye_engine](https://github.com/ubunye-ai-ecosystems/ubunye_engine).

---

## Development setup

```bash
git clone https://github.com/ubunye-ai-ecosystems/ubunye_engine.git
cd ubunye_engine

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -U pip
pip install -e ".[dev,spark,ml]"
pre-commit install
```

Run checks:

```bash
pytest -q                          # all tests
ruff check .                       # lint
black --check .                    # formatting
```

---

## Workflow

1. **Fork** the repository and create a branch:
   ```bash
   git checkout -b feat/my-change
   ```

2. Make changes with docstrings and type hints.

3. Add or update tests in `tests/`.

4. Update documentation in `docs/` where relevant.

5. Use [Conventional Commits](https://www.conventionalcommits.org/):
   ```
   feat(core): add transform pipeline
   fix(reader/jdbc): handle missing driver jar
   docs(cli): document models sub-commands
   ```

6. Push and open a PR against `main`.

---

## PR checklist

- [ ] Motivation and design described in the PR body
- [ ] Unit tests added or updated
- [ ] Documentation updated (`docs/` or docstrings)
- [ ] `CHANGELOG.md` entry added (for user-visible changes)
- [ ] `pyproject.toml` entry points updated (if adding a new plugin)
- [ ] Backwards compatibility considered

---

## Testing

Tests live in `tests/` mirroring the package structure:

```
tests/
├── unit/
│   ├── cli/         ← CLI command tests
│   ├── config/      ← schema, loader, resolver tests
│   ├── lineage/     ← lineage recorder, storage, hasher
│   └── models/      ← UbunyeModel, registry, gates, loader
└── integration/     ← Spark tests (marked @pytest.mark.integration)
```

**Unit tests must not import PySpark.** Use `MockDF` and duck-typed stubs.

Run only unit tests (fast, no Spark):

```bash
pytest tests/unit/ -v
```

Run integration tests (requires a local Spark install):

```bash
pytest tests/ -m integration
```

---

## Style guide

- Type hints everywhere; `from __future__ import annotations` at module top.
- Docstrings in Google style.
- Black for formatting, Ruff for linting.
- No hard dependencies on cloud providers or ML frameworks — keep them in optional extras.

---

## Security

Never commit secrets. Use environment variables and secret managers.
Report security vulnerabilities privately via the contact in `SECURITY.md`.

---

## Release process (maintainers)

1. Update `docs/changelog.md` with the new version section.
2. Bump `version` in `pyproject.toml`.
3. Tag and push:
   ```bash
   git tag v0.2.0 && git push --tags
   ```
4. Build and publish:
   ```bash
   python -m build
   twine upload dist/*
   ```
