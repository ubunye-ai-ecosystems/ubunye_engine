"""``secret://`` in ENGINE.spark_conf is checked by validate and resolved for Spark (F-104)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ubunye.config.schema import UbunyeConfig
from ubunye.core import backends
from ubunye.core.secrets import known_secrets, mask_text

S3_IN = {"format": "s3", "path": "in.csv", "file_format": "csv"}
S3_OUT = {"format": "s3", "path": "out", "file_format": "parquet"}


def _cfg(engine):
    return {
        "MODEL": "etl",
        "VERSION": "1.0.0",
        "ENGINE": engine,
        "CONFIG": {"inputs": {"src": S3_IN}, "outputs": {"out": S3_OUT}},
    }


def test_an_unknown_provider_in_spark_conf_fails_validation():
    with pytest.raises(ValidationError) as err:
        UbunyeConfig(**_cfg({"spark_conf": {"spark.x.key": "secret://nosuch/k"}}))
    assert "ENGINE.spark_conf.spark.x.key: no secret provider named 'nosuch'" in str(err.value)


def test_a_profile_spark_conf_is_checked_too():
    engine = {"profiles": {"prod": {"spark_conf": {"spark.x.key": "secret://nosuch/k"}}}}
    with pytest.raises(ValidationError, match="ENGINE.profiles.prod.spark_conf.spark.x.key"):
        UbunyeConfig(**_cfg(engine))


def test_a_spark_backend_gets_the_secret_value_not_the_reference(monkeypatch):
    pytest.importorskip("pyspark")
    monkeypatch.setenv("UBUNYE_TEST_SPARK_VALUE", "s3cr3t-value")
    backend = backends.create("spark", conf={"spark.x.key": "secret://env/UBUNYE_TEST_SPARK_VALUE"})
    assert backend.conf_input["spark.x.key"] == "s3cr3t-value"
    # The value is known to the masker, so no log or record shows it.
    assert "s3cr3t-value" not in mask_text("key is s3cr3t-value", known_secrets())


def test_a_backend_without_spark_is_not_made_to_fetch(monkeypatch):
    pytest.importorskip("pandas")
    monkeypatch.delenv("UBUNYE_TEST_UNSET_KEY", raising=False)
    backends.create("pandas", conf={"spark.x.key": "secret://env/UBUNYE_TEST_UNSET_KEY"})
