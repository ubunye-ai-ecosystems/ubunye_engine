"""Integration tests — verify structured errors from real code paths."""

import pytest

from ubunye.core.errors import (
    ConfigProfileError,
    TaskClassMissingError,
    TaskNotFoundError,
    TransformNotFoundError,
)


class TestTaskNotFound:
    def test_missing_transformations_py(self, tmp_path):
        from ubunye.core.task_runner import _load_task_class

        with pytest.raises(TaskNotFoundError, match="transformations.py") as exc_info:
            _load_task_class(tmp_path)
        assert exc_info.value.hint
        assert exc_info.value.context

    def test_no_task_subclass_in_file(self, tmp_path):
        (tmp_path / "transformations.py").write_text("x = 1\n", encoding="utf-8")
        from ubunye.core.task_runner import _load_task_class

        with pytest.raises(TaskClassMissingError, match="No Task subclass"):
            _load_task_class(tmp_path)


class TestTransformNotFound:
    def test_unknown_transform_type(self):
        from ubunye.core.runtime import Engine, Registry

        reg = Registry()
        engine = Engine(registry=reg)
        with pytest.raises(TransformNotFoundError) as exc_info:
            engine._validate_transforms_exist([{"type": "nonexistent"}])
        assert "nonexistent" in str(exc_info.value)
        assert exc_info.value.hint


class TestMergedSparkConfProfile:
    def test_raises_when_profiles_defined_but_missing(self):
        from ubunye.config.schema import UbunyeConfig

        cfg = UbunyeConfig.model_validate(
            {
                "CONFIG": {
                    "inputs": {"s": {"format": "hive", "db_name": "d", "tbl_name": "t"}},
                    "outputs": {"s": {"format": "hive", "db_name": "d", "tbl_name": "t"}},
                },
                "ENGINE": {
                    "profiles": {"dev": {"spark_conf": {"spark.master": "local[*]"}}},
                },
            }
        )
        with pytest.raises(ConfigProfileError, match="staging"):
            cfg.merged_spark_conf("staging")

    def test_no_error_when_no_profiles_defined(self):
        from ubunye.config.schema import UbunyeConfig

        cfg = UbunyeConfig.model_validate(
            {
                "CONFIG": {
                    "inputs": {"s": {"format": "hive", "db_name": "d", "tbl_name": "t"}},
                    "outputs": {"s": {"format": "hive", "db_name": "d", "tbl_name": "t"}},
                },
            }
        )
        conf = cfg.merged_spark_conf("DEV")
        assert isinstance(conf, dict)

    def test_resolved_catalog_raises_when_profiles_defined(self):
        from ubunye.config.schema import UbunyeConfig

        cfg = UbunyeConfig.model_validate(
            {
                "CONFIG": {
                    "inputs": {"s": {"format": "hive", "db_name": "d", "tbl_name": "t"}},
                    "outputs": {"s": {"format": "hive", "db_name": "d", "tbl_name": "t"}},
                },
                "ENGINE": {
                    "profiles": {"prod": {"catalog": "main"}},
                },
            }
        )
        with pytest.raises(ConfigProfileError):
            cfg.resolved_catalog("staging")

    def test_resolved_schema_raises_when_profiles_defined(self):
        from ubunye.config.schema import UbunyeConfig

        cfg = UbunyeConfig.model_validate(
            {
                "CONFIG": {
                    "inputs": {"s": {"format": "hive", "db_name": "d", "tbl_name": "t"}},
                    "outputs": {"s": {"format": "hive", "db_name": "d", "tbl_name": "t"}},
                },
                "ENGINE": {
                    "profiles": {"prod": {"schema_name": "fraud"}},
                },
            }
        )
        with pytest.raises(ConfigProfileError):
            cfg.resolved_schema("staging")
