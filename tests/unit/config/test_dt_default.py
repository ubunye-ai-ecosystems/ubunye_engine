"""`{{ dt | default('x') }}` must actually fall back when no -dt was given.

`ubunye run` passes {"dt": dt, "dtf": dtf, "mode": mode} unconditionally, and an omitted
flag arrives as None. Jinja treats None as DEFINED, so `default()` never fired -- the
template rendered the literal string "None", and pipelines wrote to paths like
`out/dt=None/`. Nothing errored. The data just went somewhere nobody meant.
"""

from __future__ import annotations

import pytest

from ubunye.config.resolver import resolve_config
from ubunye.core.errors import ConfigTemplateError


def test_an_omitted_dt_falls_back_to_the_default():
    out = resolve_config({"path": "out/dt={{ dt | default('latest') }}"}, cli_vars={"dt": None})
    assert out["path"] == "out/dt=latest"


def test_the_literal_string_None_never_appears():
    """The exact bug. If this regresses, real pipelines write to `dt=None`."""
    out = resolve_config(
        {"a": "{{ dt | default('x') }}", "b": "{{ dtf | default('y') }}"},
        cli_vars={"dt": None, "dtf": None, "mode": "PROD"},
    )
    assert "None" not in str(out)


def test_a_supplied_dt_is_still_used():
    out = resolve_config({"p": "dt={{ dt }}"}, cli_vars={"dt": "2026-07-13"})
    assert out["p"] == "dt=2026-07-13"


def test_a_missing_variable_with_no_default_still_fails_loudly():
    """Dropping Nones must not weaken StrictUndefined. A typo is still an error."""
    with pytest.raises((ConfigTemplateError, ValueError)):
        resolve_config({"p": "{{ nonesuch }}"}, cli_vars={"dt": "2026-07-13"})
