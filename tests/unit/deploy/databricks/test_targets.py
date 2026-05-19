"""Tests for deploy target configuration loading and resolution."""

import pytest
import yaml

from ubunye.core.errors import TargetNotFoundError
from ubunye.deploy.databricks.targets import (
    DatabricksTargetConfig,
    TargetsConfig,
    _deep_merge,
    resolve_target,
)


class TestDatabricksTargetConfig:
    def test_minimal_config(self):
        cfg = DatabricksTargetConfig(host="https://adb-123.azuredatabricks.net")
        assert cfg.host == "https://adb-123.azuredatabricks.net"
        assert cfg.token_env == "DATABRICKS_TOKEN"
        assert cfg.serverless is True
        assert cfg.num_workers is None

    def test_forbids_extra_fields(self):
        with pytest.raises(Exception, match="extra"):
            DatabricksTargetConfig(host="https://x.net", bogus="val")

    def test_full_config(self):
        cfg = DatabricksTargetConfig(
            host="https://adb-123.azuredatabricks.net",
            token_env="MY_TOKEN",
            workspace_path="/Workspace/my_project",
            spark_version="14.3.x-scala2.12",
            node_type_id="m5.2xlarge",
            num_workers=4,
            spark_conf={"spark.sql.shuffle.partitions": "200"},
            aws_attributes={"first_on_demand": 1},
        )
        assert cfg.num_workers == 4
        assert cfg.spark_conf["spark.sql.shuffle.partitions"] == "200"


class TestTargetsConfig:
    def test_load_from_dict(self):
        raw = {
            "targets": {
                "dev": {"host": "https://dev.azuredatabricks.net"},
                "prod": {"host": "https://prod.azuredatabricks.net", "num_workers": 4},
            }
        }
        cfg = TargetsConfig.model_validate(raw)
        assert "dev" in cfg.targets
        assert cfg.targets["prod"].num_workers == 4


class TestDeepMerge:
    def test_simple_override(self):
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}
        assert _deep_merge(base, override) == {"a": 1, "b": 3, "c": 4}

    def test_nested_merge(self):
        base = {"targets": {"dev": {"host": "https://dev.net", "num_workers": 0}}}
        override = {"targets": {"dev": {"num_workers": 4}}}
        result = _deep_merge(base, override)
        assert result["targets"]["dev"]["host"] == "https://dev.net"
        assert result["targets"]["dev"]["num_workers"] == 4

    def test_does_not_mutate_base(self):
        base = {"a": {"x": 1}}
        override = {"a": {"x": 2}}
        _deep_merge(base, override)
        assert base["a"]["x"] == 1


class TestResolveTarget:
    def test_loads_from_usecase_dir(self, tmp_path):
        usecase = tmp_path / "my_usecase"
        pipeline = usecase / "my_pipeline"
        task_dir = pipeline / "my_task"
        task_dir.mkdir(parents=True)

        targets = {
            "targets": {
                "dev": {"host": "https://dev.azuredatabricks.net"},
            }
        }
        (usecase / "targets.yaml").write_text(yaml.dump(targets), encoding="utf-8")

        result = resolve_target("dev", task_dir)
        assert result.host == "https://dev.azuredatabricks.net"

    def test_loads_from_task_dir(self, tmp_path):
        usecase = tmp_path / "my_usecase"
        pipeline = usecase / "my_pipeline"
        task_dir = pipeline / "my_task"
        task_dir.mkdir(parents=True)

        targets = {
            "targets": {
                "dev": {"host": "https://task-level.azuredatabricks.net"},
            }
        }
        (task_dir / "targets.yaml").write_text(yaml.dump(targets), encoding="utf-8")

        result = resolve_target("dev", task_dir)
        assert result.host == "https://task-level.azuredatabricks.net"

    def test_merges_when_both_exist(self, tmp_path):
        usecase = tmp_path / "my_usecase"
        pipeline = usecase / "my_pipeline"
        task_dir = pipeline / "my_task"
        task_dir.mkdir(parents=True)

        usecase_targets = {
            "targets": {
                "dev": {"host": "https://dev.azuredatabricks.net", "num_workers": 0},
            }
        }
        task_targets = {
            "targets": {
                "dev": {"num_workers": 4},
            }
        }
        (usecase / "targets.yaml").write_text(yaml.dump(usecase_targets), encoding="utf-8")
        (task_dir / "targets.yaml").write_text(yaml.dump(task_targets), encoding="utf-8")

        result = resolve_target("dev", task_dir)
        assert result.host == "https://dev.azuredatabricks.net"
        assert result.num_workers == 4

    def test_falls_back_to_cli_flags(self, tmp_path):
        task_dir = tmp_path / "uc" / "pkg" / "task"
        task_dir.mkdir(parents=True)

        result = resolve_target("dev", task_dir, host="https://adhoc.net")
        assert result.host == "https://adhoc.net"

    def test_raises_when_target_not_found(self, tmp_path):
        usecase = tmp_path / "my_usecase"
        pipeline = usecase / "my_pipeline"
        task_dir = pipeline / "my_task"
        task_dir.mkdir(parents=True)

        targets = {
            "targets": {
                "dev": {"host": "https://dev.azuredatabricks.net"},
            }
        }
        (usecase / "targets.yaml").write_text(yaml.dump(targets), encoding="utf-8")

        with pytest.raises(TargetNotFoundError, match="staging") as exc_info:
            resolve_target("staging", task_dir)
        assert exc_info.value.hint
        assert "dev" in str(exc_info.value)

    def test_raises_when_no_targets_and_no_host(self, tmp_path):
        task_dir = tmp_path / "uc" / "pkg" / "task"
        task_dir.mkdir(parents=True)

        with pytest.raises(TargetNotFoundError, match="No targets.yaml"):
            resolve_target("dev", task_dir)

    def test_explicit_targets_path(self, tmp_path):
        task_dir = tmp_path / "uc" / "pkg" / "task"
        task_dir.mkdir(parents=True)

        targets_file = tmp_path / "custom_targets.yaml"
        targets = {
            "targets": {
                "staging": {"host": "https://staging.azuredatabricks.net"},
            }
        }
        targets_file.write_text(yaml.dump(targets), encoding="utf-8")

        result = resolve_target("staging", task_dir, targets_path=targets_file)
        assert result.host == "https://staging.azuredatabricks.net"
