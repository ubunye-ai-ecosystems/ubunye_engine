"""A typo inside a connector block fails validation, with a suggestion.

``IOConfig`` must accept keys it does not know, because connectors read their own
settings. So ``paht: data/in.csv`` used to validate, and the run then read the wrong
thing or failed far from the cause. Now a connector can declare the settings it reads
(``CONFIG_KEYS``); for one that does, any other key is an error naming the closest
real one. A connector that declares nothing (most third-party ones) still accepts
any key, so nothing outside the engine breaks.
"""

from __future__ import annotations

import importlib.metadata as md
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from ubunye.config import schema
from ubunye.config.schema import UbunyeConfig
from ubunye.core.interfaces import Reader, Writer


def _cfg(inputs, outputs):
    return {"MODEL": "etl", "VERSION": "1.0.0", "CONFIG": {"inputs": inputs, "outputs": outputs}}


S3_IN = {"format": "s3", "path": "in.csv", "file_format": "csv"}
S3_OUT = {"format": "s3", "path": "out", "file_format": "parquet"}


def test_a_typo_in_an_input_block_fails_and_suggests_the_real_key():
    bad = dict(S3_IN)
    bad["paht"] = bad.pop("path")
    with pytest.raises(ValidationError) as err:
        UbunyeConfig(**_cfg({"src": bad}, {"out": S3_OUT}))
    message = str(err.value)
    assert "inputs.src" in message
    assert "'paht'" in message
    assert "did you mean 'path'" in message


def test_a_typo_in_an_output_block_fails_too():
    bad = dict(S3_OUT, partitonBy=["dt"])
    with pytest.raises(ValidationError) as err:
        UbunyeConfig(**_cfg({"src": S3_IN}, {"out": bad}))
    assert "outputs.out" in str(err.value)
    assert "did you mean 'partitionBy'" in str(err.value)


def test_a_key_with_no_near_match_lists_what_the_connector_reads():
    bad = dict(S3_IN, colour="blue")
    with pytest.raises(ValidationError) as err:
        UbunyeConfig(**_cfg({"src": bad}, {"out": S3_OUT}))
    message = str(err.value)
    assert "'colour'" in message and "file_format" in message and "options" in message


def test_a_parsed_config_dumped_and_read_back_still_validates():
    """model_dump() writes every common field, most as None; None means not set."""
    cfg = UbunyeConfig(**_cfg({"src": S3_IN}, {"out": S3_OUT}))
    UbunyeConfig(**cfg.model_dump(mode="json"))


def test_every_output_accepts_the_engines_write_mode_keys():
    out = dict(S3_OUT, mode="merge", merge_keys=["id"], file_format="delta")
    UbunyeConfig(**_cfg({"src": S3_IN}, {"out": out}))
    out = dict(S3_OUT, mode="overwrite", replace_where="dt = '2026-01-01'", file_format="delta")
    UbunyeConfig(**_cfg({"src": S3_IN}, {"out": out}))


def test_write_mode_keys_are_not_input_settings():
    with pytest.raises(ValidationError, match="merge_keys"):
        UbunyeConfig(**_cfg({"src": dict(S3_IN, merge_keys=["id"])}, {"out": S3_OUT}))


def test_options_is_accepted_everywhere():
    UbunyeConfig(
        **_cfg({"src": dict(S3_IN, options={"header": "true"})}, {"out": dict(S3_OUT, options={})})
    )


def test_one_output_block_can_serve_s3_and_unity():
    """Example 11's pattern: `path` for the s3 writer, `table` for unity, in one block."""
    block = {
        "path": "s3a://bucket/documents",
        "table": "workspace.ubunye.documents",
        "mode": "merge",
        "merge_keys": ["doc_id"],
        "file_format": "delta",
        "options": {"mergeSchema": "true"},
    }
    for fmt in ("s3", "unity"):
        UbunyeConfig(**_cfg({"src": S3_IN}, {"out": dict(block, format=fmt)}))


@pytest.mark.parametrize(
    "group, base",
    [("ubunye.readers", Reader), ("ubunye.writers", Writer)],
)
def test_every_built_in_connector_declares_its_settings(group, base):
    """A built-in that forgets to declare would silently accept typos again."""
    missing = []
    for ep in md.entry_points(group=group):
        if not ep.value.startswith("ubunye."):
            continue
        cls = ep.load()
        if getattr(cls, "CONFIG_KEYS", None) is None:
            missing.append(ep.name)
    assert missing == []


class LooseReader(Reader):
    """A third-party connector that declares nothing."""

    def read(self, cfg, backend):  # pragma: no cover - never executed here
        raise NotImplementedError


class StrictWriter(Writer):
    """A third-party connector that declares its settings, and gets the same check."""

    CONFIG_KEYS = frozenset({"bucket"})

    def write(self, df, cfg, backend):  # pragma: no cover
        raise NotImplementedError


@pytest.fixture
def third_party_installed():
    schema._connectors.cache_clear()
    readers = dict(schema._connectors("ubunye.readers"), loose=LooseReader)
    writers = dict(schema._connectors("ubunye.writers"), strict=StrictWriter)

    def fake(group):
        return readers if group == "ubunye.readers" else writers

    with patch.object(schema, "_connectors", side_effect=fake):
        yield
    schema._connectors.cache_clear()


def test_a_connector_that_declares_nothing_still_accepts_any_key(third_party_installed):
    UbunyeConfig(**_cfg({"src": {"format": "loose", "anything": 1}}, {"out": S3_OUT}))


def test_a_third_party_connector_that_declares_gets_the_check(third_party_installed):
    UbunyeConfig(**_cfg({"src": S3_IN}, {"out": {"format": "strict", "bucket": "b"}}))
    with pytest.raises(ValidationError, match="did you mean 'bucket'"):
        UbunyeConfig(**_cfg({"src": S3_IN}, {"out": {"format": "strict", "bukcet": "b"}}))
