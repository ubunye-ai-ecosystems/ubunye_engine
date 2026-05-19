"""Tests for Databricks job creation and update."""

from unittest.mock import MagicMock

import pytest

from ubunye.core.errors import BundleDeployError
from ubunye.deploy.databricks.bundle import build_job_spec, deploy_job
from ubunye.deploy.databricks.targets import DatabricksTargetConfig


@pytest.fixture
def serverless_target():
    return DatabricksTargetConfig(host="https://adb-123.azuredatabricks.net")


@pytest.fixture
def cluster_target():
    return DatabricksTargetConfig(
        host="https://adb-123.azuredatabricks.net",
        serverless=False,
        spark_version="13.3.x-scala2.12",
        node_type_id="i3.xlarge",
        num_workers=0,
    )


@pytest.fixture
def tags():
    return {
        "ubunye_managed": "true",
        "ubunye_usecase": "fraud",
        "ubunye_pipeline": "ingestion",
        "ubunye_task": "claim_etl",
        "ubunye_target": "dev",
    }


class TestBuildJobSpec:
    def test_serverless_spec_no_cluster(self, serverless_target, tags):
        spec = build_job_spec(
            job_name="ubunye-fraud-ingestion-claim_etl-dev",
            notebook_path="/Workspace/ubunye/fraud/ingestion/claim_etl/run_claim_etl",
            target=serverless_target,
            tags=tags,
        )
        assert spec["name"] == "ubunye-fraud-ingestion-claim_etl-dev"
        assert spec["tags"]["ubunye_managed"] == "true"
        assert spec["max_concurrent_runs"] == 1
        task = spec["tasks"][0]
        assert task["task_key"] == "run_ubunye_task"
        assert "new_cluster" not in task
        assert "notebook_task" in task

    def test_cluster_spec_has_new_cluster(self, cluster_target, tags):
        spec = build_job_spec(
            job_name="test-job",
            notebook_path="/Workspace/test",
            target=cluster_target,
            tags=tags,
        )
        task = spec["tasks"][0]
        assert "new_cluster" in task
        assert task["new_cluster"]["num_workers"] == 0
        assert task["new_cluster"]["node_type_id"] == "i3.xlarge"

    def test_includes_schedule(self, serverless_target, tags):
        spec = build_job_spec(
            job_name="test-job",
            notebook_path="/Workspace/test",
            target=serverless_target,
            tags=tags,
            schedule="0 0 6 * * ?",
        )
        assert spec["schedule"]["quartz_cron_expression"] == "0 0 6 * * ?"
        assert spec["schedule"]["pause_status"] == "UNPAUSED"

    def test_no_schedule_by_default(self, serverless_target, tags):
        spec = build_job_spec(
            job_name="test-job",
            notebook_path="/Workspace/test",
            target=serverless_target,
            tags=tags,
        )
        assert "schedule" not in spec

    def test_cluster_includes_spark_conf(self, tags):
        target = DatabricksTargetConfig(
            host="https://adb-123.net",
            serverless=False,
            spark_conf={"spark.master": "local[*]"},
        )
        spec = build_job_spec(
            job_name="test-job",
            notebook_path="/Workspace/test",
            target=target,
            tags=tags,
        )
        cluster = spec["tasks"][0]["new_cluster"]
        assert cluster["spark_conf"]["spark.master"] == "local[*]"


class TestDeployJob:
    def test_creates_new_job(self, serverless_target, tags):
        client = MagicMock()
        client.jobs.list.return_value = []
        client.jobs.create.return_value = MagicMock(job_id=42)

        result = deploy_job(
            client=client,
            job_name="ubunye-fraud-ingestion-claim_etl-dev",
            notebook_path="/Workspace/ubunye/fraud/ingestion/claim_etl/run_claim_etl",
            target=serverless_target,
            tags=tags,
        )
        assert result["job_id"] == 42
        assert result["job_name"] == "ubunye-fraud-ingestion-claim_etl-dev"
        client.jobs.create.assert_called_once()

    def test_updates_existing_job(self, serverless_target, tags):
        existing_job = MagicMock()
        existing_job.job_id = 99
        existing_job.settings.name = "ubunye-fraud-ingestion-claim_etl-dev"

        client = MagicMock()
        client.jobs.list.return_value = [existing_job]

        result = deploy_job(
            client=client,
            job_name="ubunye-fraud-ingestion-claim_etl-dev",
            notebook_path="/Workspace/test",
            target=serverless_target,
            tags=tags,
        )
        assert result["job_id"] == 99
        client.jobs.reset.assert_called_once()
        client.jobs.create.assert_not_called()

    def test_multiple_jobs_raises(self, serverless_target, tags):
        job1 = MagicMock()
        job1.job_id = 1
        job1.settings.name = "ubunye-fraud-ingestion-claim_etl-dev"
        job2 = MagicMock()
        job2.job_id = 2
        job2.settings.name = "ubunye-fraud-ingestion-claim_etl-dev"

        client = MagicMock()
        client.jobs.list.return_value = [job1, job2]

        with pytest.raises(BundleDeployError, match="Multiple jobs") as exc_info:
            deploy_job(
                client=client,
                job_name="ubunye-fraud-ingestion-claim_etl-dev",
                notebook_path="/Workspace/test",
                target=serverless_target,
                tags=tags,
            )
        assert exc_info.value.hint
        assert "duplicate" in exc_info.value.hint.lower()

    def test_create_failure_raises(self, serverless_target, tags):
        client = MagicMock()
        client.jobs.list.return_value = []
        client.jobs.create.side_effect = Exception("403 Forbidden")

        with pytest.raises(BundleDeployError, match="create"):
            deploy_job(
                client=client,
                job_name="test-job",
                notebook_path="/Workspace/test",
                target=serverless_target,
                tags=tags,
            )

    def test_update_failure_raises(self, serverless_target, tags):
        existing_job = MagicMock()
        existing_job.job_id = 99
        existing_job.settings.name = "test-job"

        client = MagicMock()
        client.jobs.list.return_value = [existing_job]
        client.jobs.reset.side_effect = Exception("500 Internal Server Error")

        with pytest.raises(BundleDeployError, match="update"):
            deploy_job(
                client=client,
                job_name="test-job",
                notebook_path="/Workspace/test",
                target=serverless_target,
                tags=tags,
            )
