"""Public names nothing uses still work in 0.9.1, with a DeprecationWarning; gone in 1.0."""

from __future__ import annotations

import pytest


def test_the_unused_helpers_still_work_and_warn():
    from ubunye.core.planning import plan_problems
    from ubunye.proving.matrix import render_many

    with pytest.warns(DeprecationWarning, match="plan_problems"):
        assert plan_problems([{"task": "a", "problems": ["x"]}, {"task": "b", "problems": []}]) == {
            "a"
        }
    with pytest.warns(DeprecationWarning, match="render_many"):
        assert render_many([]).startswith("# Proving ground")


def test_to_numpy_from_pandas_warns():
    pd = pytest.importorskip("pandas")
    from ubunye.plugins.ml.adapters import to_numpy_from_pandas

    with pytest.warns(DeprecationWarning, match="ensure_Xy_numpy"):
        assert to_numpy_from_pandas(pd.DataFrame({"a": [1, 2]}), ["a"]).shape == (2, 1)


def test_init_tracer_warns():
    from ubunye.telemetry import otel

    with pytest.warns(DeprecationWarning, match="setup"):
        otel.init_tracer("ubunye-test")


def test_capabilities_or_exit_warns():
    from ubunye.cli.backend_choice import capabilities_or_exit

    with pytest.warns(DeprecationWarning, match="class_or_exit"):
        assert capabilities_or_exit("pandas") is not None


def test_registry_auto_version_does_nothing_and_says_so(monkeypatch):
    from ubunye.plugins.transforms import model_transform as mt

    class Stop(Exception):
        pass

    monkeypatch.setattr(mt, "_get_df", lambda inputs, name: object())
    cfg = {"model_class": "m.M", "registry": {"store": "s", "auto_version": True}}
    monkeypatch.setattr(mt, "ModelRegistry", lambda *a, **kw: (_ for _ in ()).throw(Stop()))
    monkeypatch.setattr(
        mt, "load_model_class", lambda *a: type("M", (), {"train": lambda s, d: {}})
    )
    with pytest.warns(DeprecationWarning, match="auto_version"), pytest.raises(Stop):
        mt.ModelTransform()._train({}, cfg, backend=object())
