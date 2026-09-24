# ubunye/orchestration/airflow_exporter.py
"""An Airflow DAG that runs a task with the CLI, on Airflow 2.4+ and Airflow 3.

What the DAG must get right, each of which the generated file used to get wrong:

- ``schedule=`` (Airflow 3 removed ``schedule_interval``);
- ``BashOperator`` from the standard provider on Airflow 3, from ``airflow.operators``
  on Airflow 2 (the file tries one, then the other);
- ``append_env=True``: ``env=`` alone *replaces* the environment, so ``ubunye`` was
  not on ``PATH`` and every run failed;
- the pipelines folder where the DAG runs, not where it was exported
  (``usecase_dir`` option, default the exporting machine's path).
"""

from __future__ import annotations

import shlex
from pathlib import Path
from typing import Any, Mapping

from .base import OrchestratorExporter

_AIRFLOW_TEMPLATE = """\
# Written by `ubunye export airflow`. Runs on Airflow 2.4+ and Airflow 3.
from datetime import datetime

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG
except ImportError:  # Airflow 2
    from airflow import DAG
    from airflow.operators.bash import BashOperator

default_args = {{
    "owner": {owner!r},
    "retries": {retries},
}}

with DAG(
    dag_id={dag_id!r},
    start_date=datetime.strptime({start_date!r}, "%Y-%m-%d"),
    schedule={schedule!r},
    catchup={catchup},
    default_args=default_args,
    tags={tags!r},
) as dag:
    task = BashOperator(
        task_id={task_id!r},
        bash_command={bash_cmd!r},
        env={env!r},
        # env= alone replaces the whole environment: ubunye would not be on PATH.
        append_env=True,
    )
"""


class AirflowExporter(OrchestratorExporter):
    def export(
        self, config_path: Path, *, output_path: Path, options: Mapping[str, Any] | None = None
    ) -> Path:
        opts = options or {}
        dag_id = opts.get("dag_id", self._dag_id_from(config_path))
        task_id = opts.get("task_id", "run_task")
        start_date = opts.get("start_date", "2025-01-01")
        schedule = opts.get("schedule", "@daily")
        owner = opts.get("owner", "ubunye")
        retries = int(opts.get("retries", 1))
        catchup = bool(opts.get("catchup", False))
        tags = list(opts.get("tags", ["ubunye"]))

        # The generated command must be one the CLI can actually run: -d/-u/-p/-t/-m.
        profile = opts.get("profile", "prod")
        task_dir = Path(config_path).resolve().parent
        usecase_dir = opts.get("usecase_dir") or task_dir.parents[2].as_posix()
        parts = [
            "ubunye", "run", "-d", str(usecase_dir), "-u", task_dir.parents[1].name,
            "-p", task_dir.parent.name, "-t", task_dir.name, "-m", str(profile),
        ]  # fmt: skip
        if opts.get("backend"):
            parts += ["--backend", str(opts["backend"])]
        if opts.get("lineage"):
            parts.append("--lineage")
        # {{ ds }} is Airflow's logical date: each scheduled run gets its own dt.
        bash_cmd = " ".join(shlex.quote(p) for p in parts) + " -dt {{ ds }}"

        env = dict(opts.get("env", {}))  # e.g. values from Airflow connections

        dag_py = _AIRFLOW_TEMPLATE.format(
            owner=owner,
            retries=retries,
            dag_id=dag_id,
            task_id=task_id,
            start_date=start_date,
            schedule=schedule,
            catchup=str(catchup),
            tags=tags,
            bash_cmd=bash_cmd,
            env=env,
        )
        compile(dag_py, str(output_path), "exec")  # never write a DAG that cannot parse
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(dag_py, encoding="utf-8")
        return output_path

    def _dag_id_from(self, config_path: Path) -> str:
        # e.g. fraud_detection_claims_claim_etl
        parts = Path(config_path).resolve().parent.parts[-3:]  # usecase/package/task
        return "ubunye_" + "_".join(parts)
