"""Property-based tests using Hypothesis for config schema edge cases."""
import pytest
from hypothesis import given, settings, strategies as st

from ubunye.config.schema import FormatType, IOConfig, UbunyeConfig

_KNOWN_FORMATS = {f.value for f in FormatType}

_MINIMAL_CONFIG = {
    "inputs": {"s": {"format": "hive", "db_name": "db", "tbl_name": "t"}},
    "outputs": {"s": {"format": "hive", "db_name": "db", "tbl_name": "t"}},
}


# ---------------------------------------------------------------------------
# FormatType enum
# ---------------------------------------------------------------------------

@given(fmt=st.sampled_from(list(_KNOWN_FORMATS)))
def test_known_formats_accepted(fmt):
    """Every known format string should be accepted by FormatType."""
    assert FormatType(fmt).value == fmt


@given(
    fmt=st.text(min_size=1, max_size=20).filter(lambda s: s not in _KNOWN_FORMATS)
)
def test_unknown_formats_rejected(fmt):
    """Format strings not in the enum should be rejected."""
    with pytest.raises(Exception):
        IOConfig(format=fmt, path="/dummy")


# ---------------------------------------------------------------------------
# Hive IOConfig
# ---------------------------------------------------------------------------

@given(
    db=st.text(min_size=1, max_size=50).filter(str.strip),
    tbl=st.text(min_size=1, max_size=50).filter(str.strip),
)
@settings(max_examples=50)
def test_valid_hive_always_parses(db, tbl):
    """Any hive config with db_name and tbl_name should be valid."""
    io = IOConfig(format="hive", db_name=db, tbl_name=tbl)
    assert io.format == FormatType.HIVE
    assert io.db_name == db
    assert io.tbl_name == tbl


# ---------------------------------------------------------------------------
# VERSION semver
# ---------------------------------------------------------------------------

@given(version=st.from_regex(r"[0-9]+\.[0-9]+\.[0-9]+", fullmatch=True))
@settings(max_examples=50)
def test_valid_semver_accepted(version):
    """Valid semver strings should always pass VERSION validation."""
    cfg = UbunyeConfig(MODEL="etl", VERSION=version, CONFIG=_MINIMAL_CONFIG)
    assert cfg.VERSION == version


@given(
    version=st.text(min_size=1, max_size=20).filter(
        lambda s: not __import__("re").match(r"^\d+\.\d+\.\d+$", s)
    )
)
@settings(max_examples=50)
def test_invalid_semver_rejected(version):
    """Strings that are not valid semver should be rejected."""
    with pytest.raises(Exception):
        UbunyeConfig(MODEL="etl", VERSION=version, CONFIG=_MINIMAL_CONFIG)


# ---------------------------------------------------------------------------
# WriteMode
# ---------------------------------------------------------------------------

@given(mode=st.sampled_from(["overwrite", "append", "merge"]))
def test_valid_write_modes_accepted(mode):
    io = IOConfig(format="hive", db_name="db", tbl_name="t", mode=mode)
    assert io.mode.value == mode


@given(
    mode=st.text(min_size=1, max_size=20).filter(
        lambda s: s not in ("overwrite", "append", "merge")
    )
)
@settings(max_examples=30)
def test_invalid_write_modes_rejected(mode):
    with pytest.raises(Exception):
        IOConfig(format="hive", db_name="db", tbl_name="t", mode=mode)
