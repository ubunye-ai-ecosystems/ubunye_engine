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


def test_a_registered_plugin_is_accepted_even_though_it_is_not_in_the_enum():
    """This is the assertion that proves the documented extension story is now true.

    The names below come from the entry points, not from the enum. If validation went
    back to consulting a hard-coded list, this fails.
    """
    from ubunye.config.schema import FormatType

    registered = _registered_formats()
    enum_names = {e.value for e in FormatType}

    # Every entry-point name resolves, whether or not somebody remembered the enum.
    assert registered >= enum_names


def test_an_unknown_format_still_fails_and_says_what_is_available():
    """Open does not mean anything goes. A typo must be caught, with the real options."""
    with pytest.raises(ValidationError) as exc:
        IOConfig(format="postgres_hyperdrive", path="/tmp/x")

    message = str(exc.value)
    assert "Unknown format" in message
    assert "unity" in message  # tells you what you COULD have said
    assert "entry-point" in message  # tells you how to add your own


def test_format_specific_requirements_still_apply():
    """Opening the field up must not have quietly disabled the per-connector checks."""
    with pytest.raises(ValidationError, match="requires 'path'"):
        IOConfig(format="s3")

    with pytest.raises(ValidationError, match="requires 'url'"):
        IOConfig(format="jdbc", table="t")
