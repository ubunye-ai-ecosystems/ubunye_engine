"""What PyPI shows about the package, pinned so it cannot drift.

The build itself (wheel, ``twine check``, a clean install that runs the CLI) is
checked by the Package job in CI; these tests pin the rules the metadata keeps.
"""

from __future__ import annotations

from pathlib import Path

import pytest

tomllib = pytest.importorskip("tomllib")  # Python 3.11+

ROOT = Path(__file__).resolve().parents[2]
PROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
META = PROJECT["project"]


def test_the_licence_is_an_spdx_expression_with_its_file():
    assert META["license"] == "MIT"
    assert META["license-files"] == ["LICENSE"]


def test_setuptools_is_new_enough_for_spdx_licences():
    (req,) = [r for r in PROJECT["build-system"]["requires"] if r.startswith("setuptools")]
    assert req == "setuptools>=77"


def test_no_licence_classifier_next_to_the_expression():
    # setuptools 77 refuses both; the SPDX expression is the one that counts.
    assert not [c for c in META["classifiers"] if c.startswith("License ::")]


def test_the_licence_file_is_the_full_mit_text():
    text = (ROOT / "LICENSE").read_text(encoding="utf-8")
    assert text.startswith("MIT License")
    assert "Copyright (c) 2025-2026 Ubunye AI Ecosystems" in text
    assert "WITHOUT WARRANTY OF ANY KIND" in text


def test_classifiers_list_only_the_pythons_ci_tests():
    workflow = (ROOT / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")
    listed = {
        c.rsplit(" ", 1)[1]
        for c in META["classifiers"]
        if c.startswith("Programming Language :: Python :: 3.")
    }
    assert listed, "no Python versions listed"
    for version in listed:
        assert f'"{version}"' in workflow, f"Python {version} is claimed but not tested"


def test_one_urls_table_with_the_project_links():
    urls = META["urls"]
    assert {"Homepage", "Documentation", "Repository", "Issues"} <= set(urls)


def test_the_person_and_the_organisation_are_both_named():
    assert META["authors"][0]["name"] == "Thabang Mashinini-Sekgoto"
    assert META["maintainers"][0]["name"] == "Ubunye AI Ecosystems"
