"""The quickstart commands work exactly as written in the docs.

The block between ``<!-- quickstart:begin -->`` and ``<!-- quickstart:end -->``
in README.md and in docs/getting_started/quickstart.md is read from the file
and each ``ubunye`` line is run as it stands, in an empty folder, with no Java.
Change a command in the docs and this runs the new one; break a command in the
code and this fails. The two copies must also stay the same.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

import pytest
from typer.testing import CliRunner

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from ubunye.cli.main import app  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PAGES = [ROOT / "README.md", ROOT / "docs" / "getting_started" / "quickstart.md"]
BLOCK = re.compile(r"<!-- quickstart:begin -->\s*```bash\n(.*?)```\s*<!-- quickstart:end -->", re.S)


def _commands(page: Path):
    (block,) = BLOCK.findall(page.read_text(encoding="utf-8"))
    return [line.strip() for line in block.splitlines() if line.strip()]


def test_both_pages_show_the_same_commands():
    readme, docs = (_commands(p) for p in PAGES)
    assert readme == docs and len(readme) >= 3


@pytest.mark.parametrize("page", PAGES, ids=lambda p: p.name)
def test_the_quickstart_runs_word_for_word(page, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    for command in _commands(page):
        words = shlex.split(command)
        assert words[0] == "ubunye", command
        result = runner.invoke(app, words[1:])
        assert result.exit_code == 0, f"{command}\n{result.output}"
    # And it did what the page says it does.
    out = tmp_path / "pipelines" / "demo" / "starter" / "filter_adults" / "output" / "adults"
    assert (out / "_SUCCESS").exists()
    assert "success" in result.output  # the last command lists the recorded run
