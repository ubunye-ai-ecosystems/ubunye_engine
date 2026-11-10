"""The built-in ``ident`` Jinja filter: a SQL name quoted once, part by part.

Databricks catalog names often carry a hyphen (``aws-x-catalog``). Unquoted in SQL,
``aws-x-catalog.schema.t`` is read as a subtraction and fails. Teams pass the name
through a job parameter or an environment variable, sometimes with backticks and
sometimes without, so the config cannot simply add backticks: a name that arrives
quoted would be quoted twice. ``| ident`` takes either and gives the quoted form.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ubunye.config.resolver import ident, required_env_references, resolve_config


class TestIdent:
    @pytest.mark.parametrize(
        "raw, quoted",
        [
            ("aws-x-catalog", "`aws-x-catalog`"),
            ("`aws-x-catalog`", "`aws-x-catalog`"),
            ("  aws-x-catalog \n", "`aws-x-catalog`"),
            (" `aws-x-catalog` ", "`aws-x-catalog`"),
            ("aws-x-catalog.raw.trips", "`aws-x-catalog`.`raw`.`trips`"),
            ("`aws-x-catalog`.`raw`.`trips`", "`aws-x-catalog`.`raw`.`trips`"),
            ("`aws-x-catalog`.raw.trips", "`aws-x-catalog`.`raw`.`trips`"),
            ("aws-x-catalog . raw . trips", "`aws-x-catalog`.`raw`.`trips`"),
            ("plain", "`plain`"),
            # A dot inside backticks is part of the name, not a separator.
            ("`odd.name`.t", "`odd.name`.`t`"),
            # A backtick inside a quoted part is written doubled, as Spark SQL reads it.
            ("`we``ird`", "`we``ird`"),
        ],
    )
    def test_quotes_each_part_once(self, raw, quoted):
        assert ident(raw) == quoted

    def test_quoting_twice_changes_nothing(self):
        once = ident("aws-x-catalog.raw.trips")
        assert ident(once) == once

    @pytest.mark.parametrize("bad", ["", "   ", "a..b", ".a", "a.", "``", "`open", "a`b"])
    def test_refuses_what_is_not_a_name(self, bad):
        with pytest.raises(ValueError, match="ident"):
            ident(bad)


class TestIdentInConfig:
    def test_env_var_with_and_without_backticks(self):
        raw = {"sql": "SELECT * FROM {{ env.CAT | ident }}.raw.trips"}
        for value in ("aws-x-catalog", "`aws-x-catalog`", " aws-x-catalog "):
            out = resolve_config(raw, _env={"CAT": value})
            assert out["sql"] == "SELECT * FROM `aws-x-catalog`.raw.trips"

    def test_whole_dotted_name(self):
        raw = {"table": "{{ env.TARGET | ident }}"}
        out = resolve_config(raw, _env={"TARGET": "aws-x-catalog.gold.trips"})
        assert out["table"] == "`aws-x-catalog`.`gold`.`trips`"

    def test_after_default(self):
        raw = {"sql": "SELECT 1 FROM {{ env.CAT | default('dev-cat') | ident }}.s.t"}
        assert resolve_config(raw, _env={})["sql"] == "SELECT 1 FROM `dev-cat`.s.t"

    def test_cli_variable(self):
        raw = {"sql": "FROM {{ catalog | ident }}.s.t"}
        out = resolve_config(raw, cli_vars={"catalog": "`aws-x-catalog`"})
        assert out["sql"] == "FROM `aws-x-catalog`.s.t"

    def test_unset_variable_still_names_it(self):
        with pytest.raises(ValueError, match="CAT"):
            resolve_config({"sql": "{{ env.CAT | ident }}"}, _env={})

    def test_empty_value_is_refused_with_the_template(self):
        with pytest.raises(ValueError, match="ident"):
            resolve_config({"sql": "{{ env.CAT | ident }}"}, _env={"CAT": ""})

    def test_a_default_after_ident_still_makes_the_variable_optional(self):
        text = "sql: \"{{ env.CAT | ident | default('x') }}\"\nb: '{{ env.NEED | ident }}'"
        assert required_env_references(text) == {"NEED"}


CONFIG = """\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    trips:
      format: unity
      sql: "SELECT * FROM {{ env.TELM_READ_CATALOG | ident }}.raw.trips"
  transform: {}
  outputs:
    out:
      format: s3
      path: "{{ task_dir }}/out"
      file_format: csv
      mode: overwrite
"""


def _task(root: Path) -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(CONFIG, encoding="utf-8")
    return task


@pytest.mark.parametrize("value", ["aws-x-catalog", "`aws-x-catalog`"])
def test_validate_and_plan_show_the_quoted_sql(tmp_path, monkeypatch, value):
    pytest.importorskip("pandas")
    import re

    from typer.testing import CliRunner

    from ubunye.cli.main import app

    _task(tmp_path)
    monkeypatch.setenv("TELM_READ_CATALOG", value)
    args = ["-d", str(tmp_path), "-u", "uc", "-p", "pkg", "-t", "t"]
    runner = CliRunner()

    ok = runner.invoke(app, ["validate", *args])
    clean = re.sub(r"\x1b\[[0-9;]*m", "", ok.output)
    assert ok.exit_code == 0, clean
    assert "SELECT * FROM `aws-x-catalog`.raw.trips" in clean, clean

    as_json = runner.invoke(app, ["validate", *args, "--json"])
    assert '"trips": "SELECT * FROM `aws-x-catalog`.raw.trips"' in as_json.output

    plan = runner.invoke(app, ["plan", *args])
    clean = re.sub(r"\x1b\[[0-9;]*m", "", plan.output)
    assert "SELECT * FROM `aws-x-catalog`.raw.trips" in clean, clean
    assert "``" not in clean
