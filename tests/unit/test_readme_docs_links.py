"""Every docs link in README.md points at a page the docs site builds (F-010).

README.md is also the PyPI page, so its links are how a newcomer finds the docs.
MkDocs builds ``docs/x/y.md`` to ``<site_url>/x/y/`` (``use_directory_urls`` is on
by default). This maps each README link back to its source file and checks the
file exists and is in the ``mkdocs.yml`` nav, so a renamed or dropped page fails
here instead of turning into a 404. No network: it reads the repo only.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
MKDOCS = yaml.safe_load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"))
SITE = MKDOCS["site_url"].rstrip("/")
README = (ROOT / "README.md").read_text(encoding="utf-8")
LINK = re.compile(re.escape(SITE) + r"(/[^)\"'\s>]*)?")


def _nav_files(node) -> set[str]:
    if isinstance(node, str):
        return {node}
    if isinstance(node, list):
        return set().union(*(_nav_files(n) for n in node)) if node else set()
    if isinstance(node, dict):
        return set().union(*(_nav_files(v) for v in node.values())) if node else set()
    return set()


NAV = _nav_files(MKDOCS["nav"])
LINKS = sorted({m.group(1) or "/" for m in LINK.finditer(README)})


def _source(path: str) -> str:
    """``/x/y/`` -> ``x/y.md`` (or ``x/y/index.md``); ``/`` -> ``index.md``."""
    page = path.strip("/")
    if not page:
        return "index.md"
    if (ROOT / "docs" / page / "index.md").exists():
        return f"{page}/index.md"
    return f"{page}.md"


def _slug(heading: str) -> str:
    # What the toc extension does for these headings: drop markup, lower case,
    # spaces to hyphens, keep word characters and hyphens.
    text = re.sub(r"[`*_]", "", heading).strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"[\s]+", "-", text)


def test_the_readme_has_a_docs_map():
    assert "## Docs" in README
    assert len(LINKS) >= 10, LINKS


def test_site_url_matches_pyproject():
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert f'Documentation = "{SITE}/"' in pyproject


@pytest.mark.parametrize("link", LINKS)
def test_readme_link_is_a_built_page(link):
    path, _, anchor = link.partition("#")
    assert not path.endswith(".md"), f"{link}: the site serves /x/y/, not /x/y.md"
    assert path == "/" or path.endswith("/"), f"{link}: end a page link with /"
    source = _source(path)
    assert (ROOT / "docs" / source).exists(), f"{link}: no docs/{source}"
    assert source in NAV, f"{link}: docs/{source} is not in the mkdocs.yml nav"
    if anchor:
        text = (ROOT / "docs" / source).read_text(encoding="utf-8")
        slugs = {_slug(h) for h in re.findall(r"^#+\s+(.+)$", text, re.M)}
        assert anchor in slugs, f"{link}: no heading gives #{anchor}"
