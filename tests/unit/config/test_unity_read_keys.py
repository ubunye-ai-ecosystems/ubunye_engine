"""A unity read names its table as the unity reader reads it (F-102).

The reader reads ``table``, ``sql`` or ``catalog`` + ``schema`` + ``tbl_name``, like
the unity writer. ``db_name`` + ``tbl_name`` (a Hive name) used to pass validation and
fail at run time.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ubunye.config.schema import UbunyeConfig

S3_OUT = {"format": "s3", "path": "out", "file_format": "parquet"}


def _cfg(src):
    return {
        "MODEL": "etl",
        "VERSION": "1.0.0",
        "CONFIG": {"inputs": {"src": src}, "outputs": {"out": S3_OUT}},
    }


def test_a_unity_read_with_db_name_fails_validation_and_says_what_to_use():
    with pytest.raises(ValidationError) as err:
        UbunyeConfig(**_cfg({"format": "unity", "db_name": "raw", "tbl_name": "events"}))
    message = str(err.value)
    assert "inputs.src" in message and "'db_name'" in message
    assert "table: 'catalog.schema.table'" in message
    assert "catalog + schema + tbl_name" in message


@pytest.mark.parametrize(
    "src",
    [
        {"format": "unity", "table": "main.raw.events"},
        {"format": "unity", "catalog": "main", "schema": "raw", "tbl_name": "events"},
        {"format": "unity", "sql": "SELECT 1"},
    ],
)
def test_the_names_the_reader_reads_pass(src):
    UbunyeConfig(**_cfg(src))


def test_tbl_name_alone_is_not_enough():
    with pytest.raises(ValidationError, match="catalog \\+ schema \\+ tbl_name"):
        UbunyeConfig(**_cfg({"format": "unity", "tbl_name": "events"}))
