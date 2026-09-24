"""`ubunye export spark-pipeline`: a task as a Spark Declarative Pipeline (Spark 4.1+).

The spec is checked with Spark's own parser when pyspark 4.1+ is installed; the
pipeline itself is run with `spark-pipelines` in ubunye-infra (`sdp-live`), where
it wrote the same data as every other platform.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.core.errors import ConfigError
from ubunye.orchestration.spark_pipeline_exporter import SparkPipelineExporter, _input_spec

runner = CliRunner()

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Copy(Task):
    def transform(self, sources):
        return {"clean": sources["raw"], "copy": sources["raw"]}
"""


def _task(root: Path, config: str) -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (task / "transformations.py").write_text(TRANSFORM, encoding="utf-8")
    (task / "helper.py").write_text("X = 1\n", encoding="utf-8")
    (task / "config.yaml").write_text(config, encoding="utf-8")
    return task / "config.yaml"


CONFIG = """\
ENGINE:
  spark_conf: {spark.sql.shuffle.partitions: "2"}
CONFIG:
  inputs:
    raw: {format: s3, path: "data/in-{{ dt }}.csv", file_format: csv, options: {header: "true"}}
  outputs:
    clean: {format: s3, path: "out/clean", file_format: delta, mode: merge, merge_keys: [id]}
    copy: {format: s3, path: "out/copy", file_format: parquet, mode: overwrite}
"""


def _export(tmp_path, config=CONFIG, *extra):
    cfg = _task(tmp_path, config)
    out = tmp_path / "pipeline"
    result = runner.invoke(
        app,
        ["export", "spark-pipeline", "-c", str(cfg), "-o", str(out), "-dt", "2026-07-13", *extra],
    )
    return result, out


def test_the_export_writes_a_spec_the_code_and_the_task(tmp_path):
    result, out = _export(tmp_path)
    assert result.exit_code == 0, result.output
    spec = yaml.safe_load((out / "spark-pipeline.yml").read_text(encoding="utf-8"))
    assert spec["name"] == "ubunye_uc_pkg_t"
    assert spec["storage"].startswith("file:")
    assert spec["libraries"] == [{"glob": {"include": "transformations/**"}}]
    assert spec["configuration"] == {"spark.sql.shuffle.partitions": "2"}
    # The task's code sits outside the folder Spark imports definitions from.
    assert sorted(p.name for p in (out / "task").iterdir()) == [
        "config.yaml",
        "helper.py",
        "transformations.py",
    ]
    [module] = list((out / "transformations").iterdir())
    source = module.read_text(encoding="utf-8")
    compile(source, module.name, "exec")
    assert "data/in-2026-07-13.csv" in source  # rendered with the run's dt
    assert "_OUTPUTS = ['clean', 'copy']" in source


def test_what_does_not_carry_over_is_said(tmp_path):
    result, _ = _export(tmp_path)
    assert "mode 'merge' becomes a full recompute" in result.output
    assert "written to the pipeline catalog, not out/clean" in result.output


def test_the_spec_passes_sparks_own_parser(tmp_path):
    cli = pytest.importorskip("pyspark.pipelines.cli")
    _, out = _export(tmp_path)
    spec = cli.load_pipeline_spec(out / "spark-pipeline.yml")
    assert spec.name == "ubunye_uc_pkg_t" and len(spec.libraries) == 1


@pytest.mark.parametrize(
    "io, expected",
    [
        ({"format": "hive", "db_name": "raw", "tbl_name": "t"}, {"kind": "table", "table": "raw.t"}),
        ({"format": "unity", "table": "c.s.t"}, {"kind": "table", "table": "c.s.t"}),
        (
            {"format": "binary", "path": "p", "path_glob_filter": "*.txt", "recursive": True},
            {"kind": "path", "format": "binaryFile", "path": "p",
             "options": {"pathGlobFilter": "*.txt", "recursiveFileLookup": "true"}},
        ),
        ({"format": "delta", "path": "p"}, {"kind": "path", "format": "delta", "path": "p", "options": {}}),
    ],
)  # fmt: skip
def test_inputs_map_to_spark_readers(io, expected):
    assert _input_spec("x", io) == expected


def test_a_reader_with_no_equivalent_is_refused():
    with pytest.raises(ConfigError, match="no Spark Declarative Pipelines equivalent"):
        _input_spec("x", {"format": "rest_api", "url": "https://x"})


def test_secrets_are_not_written_into_a_pipeline(tmp_path):
    cfg = _task(tmp_path, CONFIG)
    with pytest.raises(ConfigError, match="secret://"):
        SparkPipelineExporter().export(
            cfg,
            output_path=tmp_path / "out",
            options={
                "config": {
                    "CONFIG": {"inputs": {}, "outputs": {"o": {"password": "secret://env/X"}}}
                }
            },
        )
