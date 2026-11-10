"""``ubunye validate`` reports a write mode the writer cannot do, as ``plan`` does (F-103)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ubunye.config.schema import UbunyeConfig

S3_IN = {"format": "s3", "path": "in.csv", "file_format": "csv"}
JDBC = {"format": "jdbc", "url": "jdbc:postgresql://db/x", "table": "public.t"}


def _cfg(out):
    return {
        "MODEL": "etl",
        "VERSION": "1.0.0",
        "CONFIG": {"inputs": {"src": S3_IN}, "outputs": {"out": out}},
    }


def test_a_jdbc_merge_fails_validation_with_the_message_plan_gives():
    with pytest.raises(ValidationError) as err:
        UbunyeConfig(**_cfg(dict(JDBC, mode="merge", merge_keys=["id"])))
    message = str(err.value)
    assert "outputs.out: Write mode 'merge' is not supported by the 'jdbc' connector." in message


def test_a_jdbc_append_still_passes():
    UbunyeConfig(**_cfg(dict(JDBC, mode="append")))


def test_a_writer_that_declares_no_modes_is_not_held_to_any(monkeypatch):
    from ubunye.config import schema
    from ubunye.core.interfaces import Writer

    class Free(Writer):
        def write(self, df, cfg, backend):  # pragma: no cover
            pass

    real = schema._connectors

    def connectors(group):
        found = real(group)
        return {**found, "free": Free} if group == "ubunye.writers" else found

    monkeypatch.setattr(schema, "_connectors", connectors)
    UbunyeConfig(**_cfg({"format": "free", "mode": "merge"}))
