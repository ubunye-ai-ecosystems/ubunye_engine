"""On live Spark, a task that overwrites the folder it reads is refused first (F-047).

Spark does not refuse the plan itself: it deletes the folder's files, then the
write reads files that are gone. The run failed and the source was lost. Now the
engine refuses before anything is read or deleted, and the input is untouched.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

pytest.importorskip("pyspark", reason="pyspark not installed")

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.core.errors import BackendCapabilityError  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()


def _task(root: Path, out: str, file_format: str = "parquet") -> Path:
    task = root / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    r = root.as_posix()
    (task / "config.yaml").write_text(textwrap.dedent(f"""\
            MODEL: etl
            VERSION: "1.0.0"
            CONFIG:
              inputs:
                src: {{format: s3, path: "{r}/in", file_format: {file_format}}}
              transform: {{}}
              outputs:
                out: {{format: s3, path: "{r}/{out}", file_format: {file_format}, mode: overwrite}}
            """))
    (task / "transformations.py").write_text(textwrap.dedent("""\
            from pyspark.sql import functions as F
            from ubunye.core.interfaces import Task


            class T(Task):
                def transform(self, sources):
                    return {"out": sources["src"].withColumn("t", F.current_timestamp())}
            """))
    return task


@pytest.mark.parametrize("via", ["session", "cli-backend"])
def test_an_overwrite_of_its_own_input_is_refused_and_the_input_survives(spark, tmp_path, via):
    spark.range(1000).write.parquet(str(tmp_path / "in"))
    task = _task(tmp_path, out="in")
    how = {"spark": spark} if via == "session" else {"backend": "spark"}
    with pytest.raises(BackendCapabilityError, match="source is lost"):
        ubunye.run_task(str(task), dt="1", **how)
    assert spark.read.parquet(str(tmp_path / "in")).count() == 1000


def test_writing_next_to_the_input_still_works(spark, tmp_path):
    spark.range(1000).write.parquet(str(tmp_path / "in"))
    ubunye.run_task(str(_task(tmp_path, out="out")), spark=spark, dt="1")
    assert spark.read.parquet(str(tmp_path / "out")).count() == 1000
