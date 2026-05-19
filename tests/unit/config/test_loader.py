"""Unit tests for the full config loading pipeline: YAML → resolve → validate."""

import pytest
import yaml

from ubunye.config.loader import ConfigFieldError, ConfigTemplateError, load_config


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
                    "dev": {
                        "spark_conf": {
                            "spark.master": "local[*]",
                            "spark.sql.shuffle.partitions": "8",
                        }
                    }
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


# ---------------------------------------------------------------------------
# Strict validation — unknown fields
# ---------------------------------------------------------------------------


class TestUnknownFieldDetection:
    def test_top_level_typo_with_suggestion(self, tmp_path):
        bad = dict(_BASE, ENGNE={})
        task_dir = _write_config(tmp_path, bad)
        with pytest.raises(ConfigFieldError, match="ENGNE") as exc_info:
            load_config(task_dir)
        assert "ENGINE" in str(exc_info.value)

    def test_nested_engine_typo(self, tmp_path):
        cfg = dict(_BASE, ENGINE={"profles": {"dev": {}}})
        task_dir = _write_config(tmp_path, cfg)
        with pytest.raises(ConfigFieldError, match="profles") as exc_info:
            load_config(task_dir)
        assert "profiles" in str(exc_info.value)

    def test_no_close_match_shows_valid_fields(self, tmp_path):
        bad = dict(_BASE, ZZZZZ="junk")
        task_dir = _write_config(tmp_path, bad)
        with pytest.raises(ConfigFieldError, match="Valid fields are") as exc_info:
            load_config(task_dir)
        assert "CONFIG" in str(exc_info.value)
        assert "ENGINE" in str(exc_info.value)

    def test_multiple_unknown_fields(self, tmp_path):
        bad = dict(_BASE, ENGNE={}, VERION="1.0.0")
        task_dir = _write_config(tmp_path, bad)
        with pytest.raises(ConfigFieldError) as exc_info:
            load_config(task_dir)
        msg = str(exc_info.value)
        assert "ENGNE" in msg
        assert "VERION" in msg

    def test_io_config_allows_plugin_extras(self, tmp_path):
        """IOConfig extra fields (plugin keys) must NOT trigger ConfigFieldError."""
        cfg = {
            "MODEL": "etl",
            "VERSION": "0.1.0",
            "CONFIG": {
                "inputs": {
                    "api": {
                        "format": "rest_api",
                        "url": "https://example.com",
                        "headers": {"X-Key": "val"},
                        "pagination": {"type": "offset"},
                    }
                },
                "outputs": {"s": {"format": "hive", "db_name": "db", "tbl_name": "t"}},
            },
        }
        task_dir = _write_config(tmp_path, cfg)
        result = load_config(task_dir)
        assert result.CONFIG.inputs["api"].model_extra["headers"]["X-Key"] == "val"

    def test_orchestration_typo(self, tmp_path):
        bad = dict(_BASE, ORCHESTRATION={"type": "airflow", "retires": 5})
        task_dir = _write_config(tmp_path, bad)
        with pytest.raises(ConfigFieldError, match="retires") as exc_info:
            load_config(task_dir)
        assert "retries" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Strict validation — undefined template variables
# ---------------------------------------------------------------------------


class TestUndefinedTemplateVariables:
    def test_undefined_env_var_raises_template_error(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NOPE", raising=False)
        cfg = {
            "MODEL": "etl",
            "VERSION": "0.1.0",
            "CONFIG": {
                "inputs": {
                    "src": {
                        "format": "jdbc",
                        "url": "jdbc:postgresql://db:5432/test",
                        "table": "t",
                        "password": "{{ env.NOPE }}",
                    }
                },
                "outputs": {"s": {"format": "hive", "db_name": "db", "tbl_name": "t"}},
            },
        }
        task_dir = _write_config(tmp_path, cfg)
        with pytest.raises(ConfigTemplateError, match="NOPE"):
            load_config(task_dir)

    def test_undefined_cli_var_raises_template_error(self, tmp_path):
        cfg = {
            "MODEL": "etl",
            "VERSION": "0.1.0",
            "CONFIG": {
                "inputs": {"s": {"format": "hive", "db_name": "db", "tbl_name": "t"}},
                "outputs": {
                    "s": {"format": "s3", "path": "s3://bucket/{{ ds }}/"}
                },
            },
        }
        task_dir = _write_config(tmp_path, cfg)
        with pytest.raises(ConfigTemplateError, match="ds"):
            load_config(task_dir)

    def test_default_filter_still_works(self, tmp_path):
        cfg = {
            "MODEL": "etl",
            "VERSION": "0.1.0",
            "CONFIG": {
                "inputs": {"s": {"format": "hive", "db_name": "db", "tbl_name": "t"}},
                "outputs": {
                    "s": {
                        "format": "s3",
                        "path": "s3://bucket/{{ ds | default('1970-01-01') }}/",
                    }
                },
            },
        }
        task_dir = _write_config(tmp_path, cfg)
        result = load_config(task_dir)
        assert result.CONFIG.outputs["s"].path == "s3://bucket/1970-01-01/"
