"""Backends are plugins (ADR 001) found by name, resolved in one order (ADR 003).

Spark, Databricks and pandas register through the ``ubunye.backends`` entry point
group exactly like a third party engine would. The core finds them by name and
never imports one itself.
"""

from __future__ import annotations

import subprocess
import sys
from typing import Any, Dict, Optional

import pytest

from ubunye.core import backends as registry
from ubunye.core.capabilities import Capabilities
from ubunye.core.errors import BackendNotFoundError
from ubunye.core.interfaces import Backend


class _EntryPoint:
    """A stand-in for importlib.metadata.EntryPoint."""

    def __init__(self, name: str, target: Any, error: Optional[Exception] = None) -> None:
        self.name = name
        self.value = f"third_party.module:{name}"
        self._target = target
        self._error = error

    def load(self) -> Any:
        if self._error:
            raise self._error
        return self._target


class ToyBackend(Backend):
    """A third party backend: nothing but the port."""

    name = "toy"
    created_with: Dict[str, Any] = {}

    def __init__(self, app_name: str = "ubunye", conf: Optional[Dict[str, Any]] = None) -> None:
        ToyBackend.created_with = {"app_name": app_name, "conf": dict(conf or {})}

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    @property
    def capabilities(self) -> Capabilities:
        return Capabilities(features=frozenset({"path_io"}), file_formats=frozenset({"csv"}))


@pytest.fixture
def with_toy(monkeypatch):
    real = registry._entry_points

    def fake():
        return [*real(), _EntryPoint("toy", ToyBackend)]

    monkeypatch.setattr(registry, "_entry_points", fake)
    return ToyBackend


class TestRegistry:
    def test_the_shipped_backends_are_registered_by_name(self):
        assert {"spark", "databricks", "pandas"} <= set(registry.available())

    def test_listing_does_not_import_the_engines(self):
        code = (
            "import sys; from ubunye.core import backends; backends.available(); "
            "print('pyspark' in sys.modules, 'pandas' in sys.modules)"
        )
        out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
        assert out.stdout.split() == ["False", "False"], out.stderr

    def test_a_third_party_backend_is_found_and_built(self, with_toy):
        backend = registry.create("toy", app_name="ubunye:x", conf={"a": "1"})
        assert isinstance(backend, ToyBackend)
        assert ToyBackend.created_with == {"app_name": "ubunye:x", "conf": {"a": "1"}}

    def test_names_are_case_blind(self):
        assert registry.load_class("PANDAS").__name__ == "PandasBackend"

    def test_an_unknown_name_lists_what_is_installed(self):
        with pytest.raises(BackendNotFoundError) as caught:
            registry.load_class("duckdb")
        message = str(caught.value)
        assert "'duckdb'" in message and "pandas" in message and "spark" in message

    def test_a_broken_plugin_says_which_and_why(self, monkeypatch):
        real = registry._entry_points
        broken = _EntryPoint("broken", None, error=ImportError("No module named 'polars'"))
        monkeypatch.setattr(registry, "_entry_points", lambda: [*real(), broken])
        with pytest.raises(BackendNotFoundError, match="polars"):
            registry.load_class("broken")
        # ...and it does not break the others.
        assert registry.load_class("pandas").__name__ == "PandasBackend"

    def test_missing_packages_are_named_with_the_install(self, monkeypatch):
        monkeypatch.setattr(registry, "_installed", lambda name: name != "pyarrow")
        assert registry.load_class("pandas").name == "pandas"  # looking needs nothing
        with pytest.raises(BackendNotFoundError) as caught:
            registry.create("pandas")  # using does
        message = str(caught.value)
        assert "needs pyarrow" in message and "ubunye-engine[pandas]" in message

    def test_spark_without_pyspark_says_so_before_starting(self, monkeypatch):
        monkeypatch.setattr(registry, "_installed", lambda name: name != "pyspark")
        monkeypatch.setattr(registry, "_platform_backend", lambda **kw: None)
        with pytest.raises(BackendNotFoundError, match=r"ubunye-engine\[spark\]"):
            registry.resolve(None)

    def test_the_listing_shows_a_backend_that_cannot_run_here(self, monkeypatch):
        from ubunye.cli.backend_choice import describe_all

        monkeypatch.setattr(registry, "_installed", lambda name: name != "pandas")
        rows = {r["name"]: r for r in describe_all()}
        assert rows["pandas"]["loaded"] is False and "pandas" in rows["pandas"]["error"]
        assert rows["spark"]["loaded"] is True

    def test_a_non_backend_entry_point_is_refused(self, monkeypatch):
        real = registry._entry_points
        monkeypatch.setattr(registry, "_entry_points", lambda: [*real(), _EntryPoint("odd", dict)])
        with pytest.raises(BackendNotFoundError, match="not a Backend"):
            registry.load_class("odd")


class TestResolutionOrder:
    def test_explicit_name_wins(self, monkeypatch):
        monkeypatch.setattr(registry, "_platform_backend", lambda **kw: pytest.fail("asked"))
        backend = registry.resolve("pandas", app_name="a", conf={})
        assert type(backend).__name__ == "PandasBackend"

    def test_then_the_platform_session(self, monkeypatch):
        sentinel = ToyBackend()
        monkeypatch.setattr(registry, "_platform_backend", lambda **kw: sentinel)
        assert registry.resolve(None, app_name="a", conf={}) is sentinel

    def test_then_spark(self, monkeypatch):
        monkeypatch.setattr(registry, "_platform_backend", lambda **kw: None)
        monkeypatch.setattr(registry, "_installed", lambda name: True)  # with or without pyspark
        backend = registry.resolve(None, app_name="ubunye:p", conf={"k": "v"})
        assert type(backend).__name__ == "SparkBackend"
        assert backend.app_name == "ubunye:p"

    def test_the_default_is_one_named_slot(self):
        # A future engine can become the default by changing this one value.
        assert registry.DEFAULT_BACKEND == "spark"

    def test_platform_detection_without_pyspark_is_none(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "pyspark", None)
        monkeypatch.setitem(sys.modules, "pyspark.sql", None)
        assert registry._platform_backend(app_name="a", conf={}) is None


class TestShippedBackendsDeclareThemselves:
    @pytest.mark.parametrize("name", ["spark", "databricks", "pandas"])
    def test_each_has_a_name_and_declared_capabilities(self, name):
        cls = registry.load_class(name)
        assert cls.name == name
        assert cls.CAPABILITIES.declared

    def test_spark_can_do_what_pandas_cannot(self):
        spark = registry.load_class("spark").CAPABILITIES
        pandas = registry.load_class("pandas").CAPABILITIES
        assert "spark" in spark.features and "spark" not in pandas.features
        assert spark.distributed and not pandas.distributed
        assert pandas.file_formats == frozenset({"csv", "json", "parquet"})
        assert "merge" not in pandas.write_modes

    def test_is_spark_follows_the_capabilities(self, with_toy):
        assert ToyBackend().is_spark is False


ENGINE_MODULES = ("pyspark", "pandas", "pyarrow", "ubunye.backends", "ubunye.adapters.spark")

# The only core code allowed to name an engine module: the deprecation shims that
# forward old import paths (issue #37) until they are removed in a major release.
SHIMS = {("catalog.py", "__getattr__"), ("write_modes.py", "__getattr__")}


def test_no_core_module_imports_an_engine():
    """The hexagon, checked on the source: every import in ubunye/core, even lazy ones."""
    import ast
    from pathlib import Path

    import ubunye.core

    offenders = []
    for path in sorted(Path(ubunye.core.__file__).parent.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for func in [tree, *[n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]]:
            owner = getattr(func, "name", "<module>")
            nodes = ast.iter_child_nodes(func) if func is tree else ast.walk(func)
            for node in nodes:
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names = [node.module]
                else:
                    continue
                for name in names:
                    if name.startswith(ENGINE_MODULES) and (path.name, owner) not in SHIMS:
                        offenders.append(f"{path.name}:{node.lineno} {owner} imports {name}")
    assert offenders == []


def test_importing_the_engine_loads_no_dataframe_library():
    code = (
        "import sys; import ubunye, ubunye.core.runtime, ubunye.core.backends; "
        "print(sorted(m for m in sys.modules if m.split('.')[0] in "
        "('pyspark', 'pandas', 'pyarrow')))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert out.stdout.strip() == "[]", out.stdout + out.stderr
