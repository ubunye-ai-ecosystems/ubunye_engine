"""`format` must accept any connector the plugin registry can load.

The engine's docs say: "adding a connector = write the class, register the entry point."
That was **false**. `format` was a closed enum, so a correctly-registered third-party
plugin was rejected by config validation *before the registry was ever consulted*. The
extension story the engine advertises did not work at all.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from ubunye.config.schema import IOConfig, _registered_formats


def test_every_builtin_connector_is_accepted():
    for name in ["hive", "jdbc", "unity", "s3", "delta", "binary", "rest_api"]:
        assert name in _registered_formats()


def test_the_engine_keeps_no_list_of_implementations():
    """There is no enum any more. There is no hardcoded list. There is the registry.

    If someone reintroduces one, `_registered_formats()` will stop tracking what is
    actually installed and this drifts.
    """
    import ubunye.config.schema as schema_mod

    assert not hasattr(schema_mod, "FormatType"), "the hardcoded enum is back"

    # The names come from the entry points, and from nowhere else.
    assert _registered_formats() == frozenset(schema_mod._connectors("ubunye.readers")) | frozenset(
        schema_mod._connectors("ubunye.writers")
    )


def test_an_unknown_format_still_fails_and_says_what_is_available():
    """Open does not mean anything goes. A typo must be caught, with the real options."""
    with pytest.raises(ValidationError) as exc:
        IOConfig(format="postgres_hyperdrive", path="/tmp/x")

    message = str(exc.value)
    assert "Unknown format" in message
    assert "unity" in message  # tells you what you COULD have said
    assert "entry-point" in message  # tells you how to add your own


def test_the_connector_declares_its_own_requirements():
    """The core holds no table of requirements. It asks the plugin.

    Validated through a TASK, because the requirement depends on the ROLE: `unity` as a
    source may be given `sql`; as a sink it must have a `table`. One shared rule in the
    core could express neither, which is why it is gone.
    """
    from ubunye.config.schema import TaskConfig

    with pytest.raises(ValidationError, match="requires 'path'"):
        TaskConfig(
            inputs={"src": {"format": "s3"}},
            transform={},
            outputs={"dst": {"format": "s3", "path": "/tmp/out"}},
        )

    with pytest.raises(ValidationError, match="requires 'url'"):
        TaskConfig(
            inputs={"src": {"format": "jdbc", "table": "t"}},
            transform={},
            outputs={"dst": {"format": "s3", "path": "/tmp/out"}},
        )


def test_a_reader_and_a_writer_of_the_same_name_can_require_different_things():
    """The distinction the old core could not express.

    `unity` as an INPUT may be a query. As an OUTPUT it must be a table -- writing to a
    SELECT statement is meaningless. One shared if/elif chain had to accept `sql`
    everywhere, so this was silently allowed.
    """
    from ubunye.config.schema import TaskConfig

    # as a source: a query is fine
    TaskConfig(
        inputs={"src": {"format": "unity", "sql": "SELECT 1"}},
        transform={},
        outputs={"dst": {"format": "s3", "path": "/tmp/out"}},
    )

    # as a sink: it is not
    with pytest.raises(ValidationError, match="output"):
        TaskConfig(
            inputs={"src": {"format": "s3", "path": "/tmp/in"}},
            transform={},
            outputs={"dst": {"format": "unity", "sql": "SELECT 1"}},
        )
