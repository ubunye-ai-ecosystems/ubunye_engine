# ubunye/orchestration/airflow_exporter.py
from __future__ import annotations
from pathlib import Path
from typing import Mapping, Any
from .base import OrchestratorExporter

_AIRFLOW_TEMPLATE = """\
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {{
    "owner": "{owner}",
    "retries": {retries},
}}

with DAG(
    dag_id="{dag_id}",
    start_date=datetime.strptime("{start_date}", "%Y-%m-%d"),
    schedule_interval="{schedule}",
    catchup={catchup},
    default_args=default_args,
    tags={tags},
) as dag:
    task = BashOperator(
        task_id="{task_id}",
        bash_command="{bash_cmd}",
        env={env},
    )
"""

class AirflowExporter(OrchestratorExporter):
    def export(self, config_path: Path, *, output_path: Path,
               options: Mapping[str, Any] | None = None) -> Path:
        opts = options or {}
        dag_id  = opts.get("dag_id", self._dag_id_from(config_path))
        task_id = opts.get("task_id", "run_task")
        start_date = opts.get("start_date", "2025-01-01")
        schedule = opts.get("schedule", "@daily")
        owner = opts.get("owner", "ubunye")
        retries = int(opts.get("retries", 1))
        catchup = bool(opts.get("catchup", False))
        tags = opts.get("tags", ["ubunye"])

        # Command uses the same CLI the developer runs locally
        profile  = opts.get("profile", "prod")
        bash_cmd = f"ubunye run -c {config_path} --profile {profile}"

        env = opts.get("env", {})  # e.g. JDBC creds via Airflow connections or env

        dag_py = _AIRFLOW_TEMPLATE.format(
            owner=owner, retries=retries, dag_id=dag_id, task_id=task_id,
            start_date=start_date, schedule=schedule, catchup=str(catchup),
            tags=tags, bash_cmd=bash_cmd, env=env
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(dag_py, encoding="utf-8")
        return output_path

    def _dag_id_from(self, config_path: Path) -> str:
        # e.g. fraud_detection_claims_claim_etl
        parts = config_path.parent.parts[-3:]  # usecase/package/task
        return "ubunye_" + "_".join(parts)
