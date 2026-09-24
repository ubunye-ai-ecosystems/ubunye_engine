"""One transform, written once with Narwhals, gives the same data on Spark and pandas (ADR 005).

The spike behind ADR 005 took the Titanic example's Spark transforms (clean,
then aggregate survival by class and age group) and wrote them once with
Narwhals. Run on both backends, the run records must carry the same data hash:
same rows, same values, same types. This is that check, end to end through
``run_task``, on Titanic-shaped data (quoted names with doubled quotes, missing
ages).
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")
pytest.importorskip("narwhals")

from pyspark.sql import SparkSession  # noqa: E402

import ubunye  # noqa: E402
from ubunye.backends.databricks_backend import DatabricksBackend  # noqa: E402
from ubunye.lineage.storage import FileSystemLineageStore  # noqa: E402

pytestmark = pytest.mark.integration

DATA = '''\
PassengerId,Survived,Pclass,Name,Sex,Age,Fare
1,0,3,"Braund, Mr. Owen Harris",male,22,7.25
2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)",female,38,71.2833
3,1,3,"Heikkinen, Miss. Laina",female,26,7.925
23,1,3,"McGowan, Miss. Anna ""Annie""",female,15,8.0292
102,0,3,"Petroff, Mr. Pastcho (""Pentcho"")",male,,7.8958
8,0,3,"Palsson, Master. Gosta Leonard",male,2,21.075
10,1,2,"Nasser, Mrs. Nicholas (Adele Achem)",female,14,30.0708
12,1,1,"Bonnell, Miss. Elizabeth",female,58,26.55
'''

TRANSFORM = """\
import narwhals as nw

from ubunye.core.interfaces import Task


class SurvivalByGroup(Task):
    def transform(self, sources):
        people = nw.from_native(sources["titanic"])
        clean = people.filter(
            ~nw.col("Survived").is_null() & ~nw.col("Pclass").is_null()
        ).with_columns(
            age_group=nw.when(nw.col("Age") < 18).then(nw.lit("child")).otherwise(nw.lit("adult"))
        )
        summary = (
            clean.group_by("Pclass", "age_group")
            .agg(
                nw.len().alias("passengers"),
                nw.col("Survived").cast(nw.Int64).sum().alias("survivors"),
            )
            .with_columns(rate=(nw.col("survivors") / nw.col("passengers")).round(4))
            .sort("Pclass", "age_group")
        )
        return {"clean": clean, "summary": summary}
"""


def _docs_example() -> str:
    """The transform shown in docs/backends.md, exactly as a reader copies it."""
    page = (Path(__file__).resolve().parents[2] / "docs" / "backends.md").read_text("utf-8")
    section = page.split("## One transform for every engine", 1)[1]
    return section.split("```python\n", 1)[1].split("```", 1)[0]


def _task(root, transform=TRANSFORM, outputs=("clean", "summary")):
    task = root / "uc" / "titanic" / "survival"
    task.mkdir(parents=True)
    (task / "titanic.csv").write_text(DATA, encoding="utf-8")
    (task / "transformations.py").write_text(transform, encoding="utf-8")
    written = "".join(
        f"""\
    {name}:
      format: s3
      path: "{{{{ task_dir }}}}/output/{{{{ backend }}}}/{name}"
      file_format: parquet
      mode: overwrite
"""
        for name in outputs
    )
    (task / "config.yaml").write_text(
        """\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    titanic:
      format: s3
      path: "{{ task_dir }}/titanic.csv"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  transform: {}
  outputs:
"""
        + written,
        encoding="utf-8",
    )
    return task


def _run_on_both(task):
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    on_spark = ubunye.run_task(
        str(task),
        backend=DatabricksBackend(spark=spark),
        lineage=True,
        variables={"backend": "spark"},
    )
    on_pandas = ubunye.run_task(
        str(task), backend="pandas", lineage=True, variables={"backend": "pandas"}
    )
    records = FileSystemLineageStore(str(task.parents[2] / ".ubunye" / "lineage")).list_runs(
        "uc/titanic/survival"
    )
    by_backend = {r.backend: {o.name: o for o in r.outputs} for r in records}
    assert set(by_backend) == {"databricks", "pandas"}
    return on_spark, on_pandas, by_backend


def _same(by_backend, name, rows):
    spark_out, pandas_out = by_backend["databricks"][name], by_backend["pandas"][name]
    assert spark_out.row_count == pandas_out.row_count == rows, name
    assert spark_out.data_hash.startswith("sha256:"), name
    assert spark_out.data_hash == pandas_out.data_hash, name


def test_the_same_narwhals_transform_gives_the_same_receipt(tmp_path):
    on_spark, on_pandas, by_backend = _run_on_both(_task(tmp_path))
    # The caller gets each engine's own frames, not the Narwhals wrapper.
    assert type(on_pandas["summary"]).__name__ == "DataFrame"
    assert hasattr(on_spark["summary"], "rdd")
    _same(by_backend, "clean", 8)
    _same(by_backend, "summary", 4)


def test_the_example_in_the_docs_does_what_the_docs_say(tmp_path):
    # docs/backends.md promises the same data hash on both engines.
    task = _task(tmp_path, transform=_docs_example(), outputs=("summary",))
    _, _, by_backend = _run_on_both(task)
    _same(by_backend, "summary", 4)
