"""A CSV that doubles its quotes, read with Spark's backslash escape, is flagged.

Found on real data: Kaggle's Olist reviews and Amazon Fine Food Reviews are written
the way pandas writes CSV (a quote inside text is ""). Read with Spark's default
escape, rows split in the wrong place and a number column silently became text, on
Spark and, since it reads as Spark does, on the pandas backend. What is read is not
changed; the run and `ubunye plan` say how to read the file right.
"""

from __future__ import annotations

import logging

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from typer.testing import CliRunner  # noqa: E402

from ubunye.adapters.pandas_io import escape_hint  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.cli.main import app  # noqa: E402

DOUBLED = 'id,text,score\n1,"she said ""great"" twice",5\n2,"plain",4\n'
CLEAN = 'id,text,score\n1,"a, comma",5\n2,"",4\n3,plain,1\n'


def _csv(tmp_path, text, name="in.csv"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return str(path)


class TestTheHint:
    def test_a_doubled_quote_inside_text_is_flagged(self, tmp_path):
        hint = escape_hint(_csv(tmp_path, DOUBLED), {"header": "true"})
        assert hint is not None and "escape: '\"'" in hint

    def test_a_file_without_one_is_not(self, tmp_path):
        assert escape_hint(_csv(tmp_path, CLEAN), {"header": "true"}) is None

    def test_an_escape_the_user_chose_is_left_alone(self, tmp_path):
        path = _csv(tmp_path, DOUBLED)
        assert escape_hint(path, {"escape": '"'}) is None
        assert escape_hint(path, {"ESCAPE": "\\"}) is None  # any explicit choice

    def test_another_delimiter(self, tmp_path):
        text = 'id;text\n1;"she said ""hi"" once"\n'
        assert escape_hint(_csv(tmp_path, text), {"sep": ";"}) is not None

    def test_a_folder_is_checked_by_its_first_file(self, tmp_path):
        folder = tmp_path / "parts"
        folder.mkdir()
        (folder / "part-0.csv").write_text(DOUBLED, encoding="utf-8")
        (folder / "_SUCCESS").write_text("", encoding="utf-8")
        assert escape_hint(str(folder), {}) is not None


class TestWhereItShows:
    def test_the_pandas_reader_logs_it_and_reads_as_spark_does(self, tmp_path, caplog):
        path = _csv(tmp_path, DOUBLED)
        with caplog.at_level(logging.WARNING, logger="ubunye.adapters.pandas_io"):
            frame = PandasBackend().read_frame("csv", path, options={"header": "true"})
        assert "doubles quotes" in caplog.text
        assert frame.count() == 2  # the read itself is unchanged

    def test_plan_warns_before_the_run(self, tmp_path):
        data = _csv(tmp_path, DOUBLED, "reviews.csv")
        task = tmp_path / "uc" / "pkg" / "t"
        task.mkdir(parents=True)
        (task / "transformations.py").write_text(
            "from ubunye.core.interfaces import Task\n\n\n"
            "class T(Task):\n"
            "    def transform(self, sources):\n"
            "        return {'out': sources['src']}\n",
            encoding="utf-8",
        )
        (task / "config.yaml").write_text(
            "MODEL: etl\n"
            'VERSION: "1.0.0"\n'
            "CONFIG:\n"
            "  inputs:\n"
            "    src:\n"
            "      format: s3\n"
            f'      path: "{data}"\n'.replace("\\", "/") + "      file_format: csv\n"
            "      options:\n"
            '        header: "true"\n'
            "  transform: {}\n"
            "  outputs:\n"
            "    out:\n"
            "      format: s3\n"
            f'      path: "{(tmp_path / "out").as_posix()}"\n'
            "      file_format: parquet\n"
            "      mode: overwrite\n",
            encoding="utf-8",
        )
        args = ["plan", "-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t"]
        result = CliRunner().invoke(app, args + ["--backend", "pandas"])
        assert result.exit_code == 0, result.output
        assert "inputs.src" in result.output and "doubles quotes" in result.output
