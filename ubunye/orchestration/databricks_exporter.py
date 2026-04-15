from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .base import OrchestratorExporter


class DatabricksExporter(OrchestratorExporter):
    """
    Generate a Databricks Jobs API spec (job.json) that runs `ubunye run ...`
    on a job cluster. Assumes your wheel is available on DBFS or installed from a repo.
    """

    def export(
        self, config_path: Path, *, output_path: Path, options: Mapping[str, Any] | None = None
    ) -> Path:
        opts = dict(options or {})
        job_name = opts.get("job_name") or self._job_name_from(config_path)
        profile = opts.get("profile", "prod")

        # Where your wheel lives (uploaded in CI): adjust to your path
        wheel_dbfs_path = opts.get(
            "wheel_dbfs_path", "dbfs:/libs/ubunye_engine-0.1.0-py3-none-any.whl"
        )

        # Minimal single-task job cluster (Databricks runtime version can be parameterized)
        job = {
            "name": job_name,
            "tasks": [
                {
                    "task_key": "run_ubunye_task",
                    "libraries": [{"whl": wheel_dbfs_path}],
                    # Easiest: run a Bash command that invokes the Ubunye CLI
                    "new_cluster": {
                        "spark_version": opts.get("spark_version", "13.3.x-scala2.12"),
                        "node_type_id": opts.get("node_type_id", "i3.xlarge"),
                        "num_workers": int(opts.get("num_workers", 2)),
                        "spark_conf": opts.get("spark_conf", {}),
                        "aws_attributes": opts.get(
                            "aws_attributes", {}
                        ),  # optional per-cloud attrs
                    },
                    "spark_python_task": {
                        # Wrap CLI call inside Python so it's portable; alternatively use a notebook_task
                        "python_file": "dbfs:/libs/run_ubunye.py",
                        "parameters": ["-c", str(config_path), "--profile", profile],
                    },
                }
            ],
        }

        # Optional schedule (turn this into a scheduled workflow if provided)
        if "schedule_quartz" in opts:
            job["schedule"] = {
                "quartz_cron_expression": opts["schedule_quartz"],
                "timezone_id": opts.get("timezone_id", "UTC"),
                "pause_status": "UNPAUSED",
            }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(job, indent=2), encoding="utf-8")
        return output_path

    def _job_name_from(self, config_path: Path) -> str:
        # e.g. ubunye_fraud_detection_claims_claim_etl
        parts = config_path.parent.parts[-3:]
        return "ubunye_" + "_".join(parts)
