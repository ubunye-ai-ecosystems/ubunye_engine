"""Unit tests for the full config loading pipeline: YAML → resolve → validate."""
import pytest
import yaml

from ubunye.config.loader import load_config


def _write_config(tmp_path, config_dict, task_name="task"):
    task_dir = tmp_path / "project" / "use_case" / "pipeline" / task_name
    task_dir.mkdir(parents=True)
    (task_dir / "config.yaml").write_text(yaml.dump(config_dict), encoding="utf-8")
    return str(task_dir)


_BASE = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        "inputs": {"source": {"format": "hive", "db_name": "db", "tbl_name": "tbl"}},
        "transform": {"type": "noop"},
        "outputs": {"sink": {"format": "hive", "db_name": "db", "tbl_name": "out"}},
    },
}


class TestConfigLoader:

    def test_load_valid_config_from_directory(self, tmp_path):
        task_dir = _write_config(tmp_path, _BASE)
        result = load_config(task_dir)
        assert result.MODEL.value == "etl"
        assert result.VERSION == "0.1.0"

    def test_load_valid_config_from_file(self, tmp_path):
        task_dir = _write_config(tmp_path, _BASE)
        result = load_config(f"{task_dir}/config.yaml")
        assert result.MODEL.value == "etl"

    def test_load_with_jinja_env_vars(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DB_PASS", "secret")
        cfg_dict = {
            "MODEL": "etl",
            "VERSION": "0.1.0",
            "CONFIG": {
                "inputs": {
                    "source": {
                        "format": "jdbc",
                        "url": "jdbc:postgresql://db:5432/test",
                        "table": "claims",
                        "password": "{{ env.DB_PASS }}",
                    }
                },
                "transform": {"type": "noop"},
                "outputs": {"sink": {"format": "hive", "db_name": "db", "tbl_name": "out"}},
            },
        }
        task_dir = _write_config(tmp_path, cfg_dict)
        result = load_config(task_dir)
        assert result.CONFIG.inputs["source"].password == "secret"

    def test_load_with_cli_vars(self, tmp_path):
        cfg_dict = {
            "MODEL": "etl",
            "VERSION": "0.1.0",
            "CONFIG": {
                "inputs": {"source": {"format": "hive", "db_name": "db", "tbl_name": "tbl"}},
                "transform": {"type": "noop"},
                "outputs": {
                    "sink": {
                        "format": "s3",
                        "path": "s3a://bucket/{{ dt | default('1970-01-01') }}/",
                    }
                },
            },
        }
        task_dir = _write_config(tmp_path, cfg_dict)
        result = load_config(task_dir, variables={"dt": "2025-03-01"})
        assert result.CONFIG.outputs["sink"].path == "s3a://bucket/2025-03-01/"

    def test_load_invalid_config_raises_readable_error(self, tmp_path):
        bad = {
            "MODEL": "etl",
            "VERSION": "0.1.0",
            "CONFIG": {
                "inputs": {"source": {"format": "hive"}},  # missing db_name + tbl_name
                "outputs": {"sink": {"format": "hive", "db_name": "db", "tbl_name": "out"}},
            },
        }
        task_dir = _write_config(tmp_path, bad)
        with pytest.raises(ValueError) as exc_info:
            load_config(task_dir)
        err = str(exc_info.value)
        assert "hive" in err

    def test_load_missing_config_file_raises(self, tmp_path):
        task_dir = tmp_path / "project" / "use_case" / "pipeline" / "task"
        task_dir.mkdir(parents=True)
        with pytest.raises(FileNotFoundError):
            load_config(str(task_dir))

    def test_load_nonexistent_path_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(str(tmp_path / "does_not_exist" / "config.yaml"))

    def test_profile_merge(self, tmp_path):
        cfg_dict = {
            "MODEL": "etl",
            "VERSION": "0.1.0",
            "ENGINE": {
                "spark_conf": {"spark.sql.shuffle.partitions": "50"},
                "profiles": {
                    "dev": {"spark_conf": {"spark.master": "local[*]", "spark.sql.shuffle.partitions": "8"}}
                },
            },
            "CONFIG": {
                "inputs": {"s": {"format": "hive", "db_name": "db", "tbl_name": "t"}},
                "outputs": {"s": {"format": "hive", "db_name": "db", "tbl_name": "t"}},
            },
        }
        task_dir = _write_config(tmp_path, cfg_dict)
        result = load_config(task_dir, profile="dev")
        merged = result.merged_spark_conf("dev")
        assert merged["spark.sql.shuffle.partitions"] == "8"
        assert merged["spark.master"] == "local[*]"

    def test_nonexistent_profile_raises(self, tmp_path):
        task_dir = _write_config(tmp_path, _BASE)
        with pytest.raises(ValueError, match="prod"):
            load_config(task_dir, profile="prod")

    def test_empty_yaml_raises(self, tmp_path):
        task_dir = tmp_path / "project" / "use_case" / "pipeline" / "task"
        task_dir.mkdir(parents=True)
        (task_dir / "config.yaml").write_text("", encoding="utf-8")
        with pytest.raises((ValueError, TypeError)):
            load_config(str(task_dir))

    def test_error_message_contains_field_path(self, tmp_path):
        bad = {
            "MODEL": "etl",
            "VERSION": "0.1.0",
            "CONFIG": {
                "inputs": {"src": {"format": "s3"}},  # missing path
                "outputs": {"sink": {"format": "hive", "db_name": "db", "tbl_name": "out"}},
            },
        }
        task_dir = _write_config(tmp_path, bad)
        with pytest.raises(ValueError) as exc_info:
            load_config(task_dir)
        assert "s3" in str(exc_info.value)
