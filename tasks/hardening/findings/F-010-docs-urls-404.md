# F-010: docs urls 404

**Status:** fixed on fix/f010-f042-docs
**Severity:** minor
**Source:** stranger (both), round 1 (2026-09-28)
**Promise:** none

## What happens
Guessed docs URLs returned 404; the README and PyPI page give no map to the docs.

## Repro
From PyPI, try to find the expectations page.

## Expected
The README links the main docs pages directly.

## Evidence
The PyPI page is the README (`pyproject.toml`: `readme = "README.md"`). Before the fix
the README linked the docs home, the quickstart, and two links ("Patterns", "plugin
guide") that both pointed at the docs home, not at those pages. Nothing named the
expectations, config, lineage or backends pages.

The site is `https://ubunye-ai-ecosystems.github.io/ubunye_engine` (`mkdocs.yml`
`site_url`, built by `.github/workflows/docs.yml` on a push to `main`).
`use_directory_urls` is not set, so `docs/x/y.md` is served at `/x/y/`. The guesses a
newcomer makes miss (curl, 2026-09-30):

| guessed | status | real page |
|---|---|---|
| `/expectations/` | 404 | `/config/expectations/` |
| `/getting-started/` | 404 | `/getting_started/install/` |
| `/docs/` | 404 | `/` |

## Fix
The README has a "Docs" section (and a "Docs map" link at the top): a table of what
you want to do and the page that says how, with the published address of each. The
"Patterns" and "plugin guide" links now point at real pages.

Every address in the README, checked with curl on the live site on 2026-09-30:

| status | page |
|---|---|
| 200 | `/` |
| 200 | `/getting_started/install/` |
| 200 | `/getting_started/quickstart/` |
| 200 | `/config/overview/` |
| 200 | `/config/io/` |
| 200 | `/config/expectations/` |
| 200 | `/cli/#ubunye-lineage` (the `id="ubunye-lineage"` heading is in the live HTML) |
| 200 | `/architecture/adr-006-run-record/` |
| 200 | `/backends/` |
| 200 | `/cli/` |
| 200 | `/api/` |
| 200 | `/patterns/llm/` |
| 200 | `/patterns/rag/` |
| 200 | `/examples/` |
| 200 | `/connectors/overview/` |
| 200 | `/connectors/plugin_guide/` |
| 200 | `/deployment/` |
| 200 | `/errors/` |
| 200 | `/changelog/` |

Not linked, because they are not published yet: the proving ground pages
(`/proving-ground/` and `/guides/portable-transforms/` return 404). They are in the
nav on this branch, but the docs site builds from `main` only, so they go live with the
release PR. Link them from the README then.

`tests/unit/test_readme_docs_links.py` keeps the map from rotting. With no network, it
maps every docs link in the README to its source file (`/x/y/` to `docs/x/y.md` or
`docs/x/y/index.md`) and checks the file exists, is in the `mkdocs.yml` nav, and, for
a `#anchor`, has a heading that makes that anchor. It also checks the README has the
map and that `site_url` matches the `Documentation` address in `pyproject.toml`.

- Before (README at 29bc907): `test_the_readme_has_a_docs_map` fails (1 failed, 3 passed).
- After: 21 passed.
- A broken link fails it: changing `/config/expectations/` to `/expectations/` in the
  README gives `AssertionError: /expectations/: no docs/expectations.md`.

What the test cannot see: a page on this branch that `main` has not published. That is
why the curl check above is part of the fix, and why the proving ground pages wait.
