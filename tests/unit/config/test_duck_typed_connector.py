"""A connector that inherits NOTHING must work. That is the design philosophy.

docs/interfaces.md: "Any class that implements the required methods satisfies the
protocol — no inheritance needed." blog.md names it: hexagonal architecture, ports and
adapters, structural typing.

The first version of the plugin contract broke this: the engine called
plugin.validate_config() unconditionally, so a duck-typed reader — the exact thing the
docs promise will work — crashed with AttributeError. This test is the promise, kept.
"""

from __future__ import annotations

from unittest.mock import patch

from ubunye.config import schema


class DuckReader:
    """Implements read(). Inherits nothing. Declares nothing. Owes nothing."""

    def read(self, cfg, backend):  # pragma: no cover
        raise NotImplementedError


class DuckWriter:
    """Implements write(). No base class, no SUPPORTED_MODES, no validate_config."""

    def write(self, df, cfg, backend):  # pragma: no cover
        raise NotImplementedError


def _installed():
    schema._connectors.cache_clear()
    readers = dict(schema._connectors("ubunye.readers"), duck=DuckReader)
    writers = dict(schema._connectors("ubunye.writers"), duck=DuckWriter)

    def fake(group):
        return readers if group == "ubunye.readers" else writers

    return patch.object(schema, "_connectors", fake)


def test_a_connector_with_no_base_class_and_no_declarations_just_works():
    """It answers no questions, so it is asked to meet no requirements."""
    with _installed():
        cfg = schema.TaskConfig(
            inputs={"src": {"format": "duck", "anything": "goes"}},
            transform={},
            outputs={"dst": {"format": "duck"}},
        )
        assert cfg.inputs["src"].format == "duck"
    schema._connectors.cache_clear()


def test_declaring_is_opt_in_not_a_toll():
    """A duck that CHOOSES to declare requirements gets them enforced — same as anyone."""

    class OpinionatedDuck:
        @classmethod
        def validate_config(cls, cfg):
            return [] if cfg.get("pond") else ["format 'duck' requires 'pond'"]

        def read(self, cfg, backend):  # pragma: no cover
            raise NotImplementedError

    import pytest
    from pydantic import ValidationError

    schema._connectors.cache_clear()
    readers = dict(schema._connectors("ubunye.readers"), duck=OpinionatedDuck)
    writers = dict(schema._connectors("ubunye.writers"), duck=DuckWriter)

    with patch.object(
        schema, "_connectors", lambda g: readers if g == "ubunye.readers" else writers
    ):
        with pytest.raises(ValidationError, match="requires 'pond'"):
            schema.TaskConfig(
                inputs={"src": {"format": "duck"}},
                transform={},
                outputs={"dst": {"format": "duck"}},
            )
    schema._connectors.cache_clear()
