"""What ``ubunye init`` writes into a new task folder.

Two templates:

``local`` (the default)
    A sample CSV next to the task, a config that reads it and writes Parquet
    under ``output/``, and a transform written so that it means the same thing
    in pandas and in Spark. It runs on a laptop with no Java
    (``--backend pandas``) and on Spark from the same files.
``databricks``
    The scaffold ``ubunye init`` wrote before 0.7.0: read a Unity Catalog table,
    write to an ``s3a://`` bucket.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Dict, List

TEMPLATES = ("local", "databricks")
_DOC = '"""'  # a docstring quote for the code we write

#: The sample the local scaffold reads: people of all ages, so the example has
#: something to keep and something to drop.
PEOPLE_CSV = """name,age,city
Thandi,34,Soshanguve
Sipho,17,Mamelodi
Lerato,22,Soweto
Kagiso,15,Tembisa
Naledi,41,Pretoria
Bongani,19,Durban
Zanele,12,Cape Town
Mpho,28,Polokwane
"""


def class_name(task: str) -> str:
    return "".join(s.capitalize() for s in task.replace("-", "_").split("_"))


def local_config(input_path: str, output_path: str) -> str:
    return f"""MODEL: "etl"
VERSION: "0.1.0"

# This folder is the whole task. It reads the sample CSV next to it and writes
# Parquet into output/ next to it, and runs on pandas (no Java) or on Spark from
# the same files:
#   ubunye run ... --backend pandas
#   ubunye run ...
# {{ task_dir }} is this folder, so the paths work from wherever you run it.
CONFIG:
  inputs:
    people:
      format: s3
      path: "{input_path}"
      file_format: csv
      options:
        header: "true"
        inferSchema: "true"
  transform: {{}}
  outputs:
    adults:
      format: s3
      path: "{output_path}"
      file_format: parquet
      mode: overwrite
"""


def local_transform(task: str) -> str:
    return f"""from typing import Any, Dict

from ubunye.core.interfaces import Task


class {class_name(task)}(Task):
    {_DOC}Keep the people aged 18 and over.

    `people[people["age"] >= 18]` means the same thing in pandas and in Spark,
    so this task runs on either backend unchanged.
    {_DOC}

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        people = sources["people"]
        return {{"adults": people[people["age"] >= 18]}}
"""


def databricks_config(usecase: str, package: str, task: str) -> str:
    return f"""MODEL: "etl"
VERSION: "0.1.0"
ENGINE:
  spark_conf:
    spark.sql.shuffle.partitions: "50"

CONFIG:
  inputs:
    tx_data:
      format: unity
      db_name: raw_db
      tbl_name: {task}_input
  transform: {{}}
  outputs:
    output_features:
      format: s3
      path: "s3a://your-bucket/{usecase}/{package}/{task}/{{{{ dt | default('1970-01-01') }}}}"
      mode: overwrite
"""


def databricks_transform(task: str) -> str:
    return f"""from typing import Dict, Any
from ubunye.core.interfaces import Task

class {class_name(task)}(Task):
    {_DOC}User-defined Spark transformation task.{_DOC}
    def setup(self) -> None:
        pass

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        # Replace with your pure DataFrame transformations.
        df = sources.get("tx_data")
        return {{"output_features": df}}
"""


def files_for(
    template: str, usecase: str, package: str, task: str, target: Path
) -> Dict[Path, str]:
    """The files a template writes for one task: ``{path: text}``."""
    builders: Dict[str, Callable[[], Dict[Path, str]]] = {
        "local": lambda: {
            target / "data" / "people.csv": PEOPLE_CSV,
            target
            / "config.yaml": local_config(
                "{{ task_dir }}/data/people.csv", "{{ task_dir }}/output/adults"
            ),
            target / "transformations.py": local_transform(task),
        },
        "databricks": lambda: {
            target / "config.yaml": databricks_config(usecase, package, task),
            target / "transformations.py": databricks_transform(task),
        },
    }
    return builders[template]()


def next_steps(usecase_dir: Path, usecase: str, package: str, tasks: List[str]) -> List[str]:
    """What to type after a local scaffold."""
    where = f"-d {usecase_dir.as_posix()} -u {usecase} -p {package}"
    names = " ".join(f"-t {t}" for t in tasks)
    return [
        "Next, from this folder:",
        f"  ubunye plan {where} {names} --backend pandas   # check it; it moves nothing",
        f"  ubunye run {where} {names} --backend pandas    # run it, no Java needed",
        f"  ubunye run {where} {names}                     # or on Spark",
    ]
