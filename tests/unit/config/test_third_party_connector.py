"""A third-party connector must work with ZERO edits to the engine.

That is what the docs have always promised — "write the class, register the entry point"
— and it was not true. `format` was a closed enum, `config/schema.py` carried an
`if/elif` chain spelling out what each connector needed, and `core/write_modes.py`
decided which formats were allowed to MERGE. To add a connector you had to edit the
engine in three places.

So this test invents one the engine has never heard of, registers it the way a plugin
package would, and asserts the engine does the right thing without knowing anything
about it. If someone reintroduces a hardcoded list, this fails.
"""

from __future__ import annotations

from typing import Any, Dict, List
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from ubunye.config import schema
from ubunye.core.interfaces import Reader, Writer


class IcebergReader(Reader):
    """A connector the engine has never heard of."""

    @classmethod
    def validate_config(cls, cfg: Dict[str, Any]) -> List[str]:
        return [] if cfg.get("table") else ["format 'iceberg' requires 'table'"]

    def read(self, cfg, backend):  # pragma: no cover - never executed here
        raise NotImplementedError


class IcebergWriter(Writer):
    """...and it can MERGE, which the core used to reserve for Delta alone."""

    SUPPORTED_MODES = frozenset({"append", "overwrite", "merge"})
    SUPPORTS_MERGE = True
    MERGE_FILE_FORMATS = frozenset({"iceberg"})

    @classmethod
    def validate_config(cls, cfg: Dict[str, Any]) -> List[str]:
        return [] if cfg.get("table") else ["format 'iceberg' requires 'table'"]

    def write(self, df, cfg, backend):  # pragma: no cover
        raise NotImplementedError


@pytest.fixture
def iceberg_installed():
    """Register the plugin the way an installed package's entry points would."""
    schema._connectors.cache_clear()
    readers = dict(schema._connectors("ubunye.readers"), iceberg=IcebergReader)
    writers = dict(schema._connectors("ubunye.writers"), iceberg=IcebergWriter)

    def fake(group):
        return readers if group == "ubunye.readers" else writers

    with patch.object(schema, "_connectors", fake):
        yield
    schema._connectors.cache_clear()


def test_a_connector_the_engine_has_never_heard_of_is_accepted(iceberg_installed):
    """No enum was edited. No if/elif was extended. It simply works."""
    cfg = schema.TaskConfig(
        inputs={"src": {"format": "iceberg", "table": "lake.db.events"}},
        transform={},
        outputs={"dst": {"format": "iceberg", "table": "lake.db.curated", "mode": "merge"}},
    )
    assert cfg.inputs["src"].format == "iceberg"


def test_it_enforces_its_OWN_requirements(iceberg_installed):
    """The engine does not know what iceberg needs. It asks, and reports the answer."""
    with pytest.raises(ValidationError, match="requires 'table'"):
        schema.TaskConfig(
            inputs={"src": {"format": "iceberg"}},
            transform={},
            outputs={"dst": {"format": "iceberg", "table": "t"}},
        )


def test_it_can_declare_that_it_supports_merge():
    """`_MERGE_FORMATS = {"delta"}` in the core made this impossible.

    A connector that genuinely upserts could not say so without a patch to the engine.
    """
    from ubunye.core import write_modes

    resolved = write_modes.resolve(
        {"mode": "merge", "merge_keys": ["id"]},
        connector="iceberg",
        supported=IcebergWriter.SUPPORTED_MODES,
        default="append",
        file_format="iceberg",
        merge_formats=IcebergWriter.MERGE_FILE_FORMATS,
    )
    assert resolved.is_merge
    assert resolved.merge_keys == ("id",)


def test_a_connector_that_cannot_merge_is_still_refused():
    """Open does not mean anything goes. A JDBC target has no MERGE, and says so."""
    from ubunye.core import write_modes
    from ubunye.core.errors import SinkWriteError

    with pytest.raises(SinkWriteError):
        write_modes.resolve(
            {"mode": "merge", "merge_keys": ["id"]},
            connector="jdbc",
            supported=frozenset({"append", "overwrite"}),
            default="append",
            file_format="jdbc",
            merge_formats=frozenset(),
        )


def test_the_base_writer_claims_nothing_it_has_not_earned():
    """`SUPPORTS_MERGE = False` and `MERGE_FILE_FORMATS = {"delta"}` contradicted each
    other: a connector that had not said it could upsert was still asserting which
    formats it upserted into. A default should not put words in a plugin's mouth.
    """
    assert Writer.SUPPORTS_MERGE is False
    assert Writer.MERGE_FILE_FORMATS == frozenset()


def test_every_shipped_writer_states_its_own_capability():
    """The built-ins must not be relying on a default that says nothing."""
    from ubunye.core.runtime import Registry

    for name, cls in Registry.from_entrypoints().writers.items():
        if cls.SUPPORTS_MERGE:
            assert cls.MERGE_FILE_FORMATS, f"'{name}' says it merges but not into what"
        else:
            assert not cls.MERGE_FILE_FORMATS, f"'{name}' cannot merge but names formats"
