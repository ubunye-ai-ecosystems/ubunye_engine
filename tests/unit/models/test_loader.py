"""Unit tests for ubunye.models.loader.load_model_class.

All tests are Spark-free. Real Python model files are written to tmp_path
and loaded dynamically, matching the production pattern.
"""
import sys

import pytest

from ubunye.models.loader import load_model_class

# ---------------------------------------------------------------------------
# Shared model code snippets
# ---------------------------------------------------------------------------

_VALID_MODEL_CODE = """\
from ubunye.models.base import UbunyeModel

class MyModel(UbunyeModel):
    def train(self, df):
        return {"acc": 0.9}
    def predict(self, df):
        return df
    def save(self, path):
        from pathlib import Path
        Path(path).mkdir(parents=True, exist_ok=True)
    @classmethod
    def load(cls, path):
        return cls()
    def metadata(self):
        return {"library": "test", "library_version": "1.0", "features": [], "params": {}}
"""

_NOT_UBUNYE_MODEL_CODE = """\
class BadModel:
    def train(self, df): pass
"""

_VALID_NESTED_CODE = """\
from ubunye.models.base import UbunyeModel

class NestedModel(UbunyeModel):
    def train(self, df): return {"acc": 0.8}
    def predict(self, df): return df
    def save(self, path):
        from pathlib import Path
        Path(path).mkdir(parents=True, exist_ok=True)
    @classmethod
    def load(cls, path): return cls()
    def metadata(self): return {"library": "test", "library_version": "0", "features": [], "params": {}}
"""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLoadModelClass:
    def test_load_valid_model(self, tmp_path):
        """Loading a valid UbunyeModel subclass returns the class."""
        (tmp_path / "model.py").write_text(_VALID_MODEL_CODE, encoding="utf-8")
        cls = load_model_class(str(tmp_path), "model.MyModel")
        from ubunye.models.base import UbunyeModel

        assert issubclass(cls, UbunyeModel)

    def test_loaded_class_is_instantiable(self, tmp_path):
        """The loaded class can be instantiated with no arguments."""
        (tmp_path / "model.py").write_text(_VALID_MODEL_CODE, encoding="utf-8")
        cls = load_model_class(str(tmp_path), "model.MyModel")
        instance = cls()
        assert instance is not None

    def test_missing_model_file_raises_file_not_found(self, tmp_path):
        """If model.py does not exist in task_dir, FileNotFoundError is raised."""
        with pytest.raises(FileNotFoundError, match="Model file not found"):
            load_model_class(str(tmp_path), "model.MyModel")

    def test_missing_class_raises_import_error(self, tmp_path):
        """If the class name is not in the module, ImportError is raised."""
        (tmp_path / "model.py").write_text("class OtherClass: pass", encoding="utf-8")
        with pytest.raises(ImportError, match="NonExistent"):
            load_model_class(str(tmp_path), "model.NonExistent")

    def test_non_ubunye_model_raises_type_error(self, tmp_path):
        """A class not inheriting UbunyeModel raises TypeError."""
        (tmp_path / "model.py").write_text(_NOT_UBUNYE_MODEL_CODE, encoding="utf-8")
        with pytest.raises(TypeError, match="UbunyeModel"):
            load_model_class(str(tmp_path), "model.BadModel")

    def test_load_from_nested_module(self, tmp_path):
        """Handles dotted module paths like 'models.risk.NestedModel'."""
        models_dir = tmp_path / "models" / "risk"
        models_dir.mkdir(parents=True)
        (models_dir.parent / "__init__.py").write_text("", encoding="utf-8")
        (models_dir / "__init__.py").write_text("", encoding="utf-8")
        (models_dir / "model.py").write_text(_VALID_NESTED_CODE, encoding="utf-8")
        # "models.risk.model.NestedModel" with task_dir=tmp_path
        cls = load_model_class(str(tmp_path), "models.risk.model.NestedModel")
        assert cls.__name__ == "NestedModel"

    def test_load_without_task_dir_uses_sys_path(self, tmp_path):
        """When task_dir is None, falls back to importlib.import_module."""
        # Write model.py and add tmp_path to sys.path manually
        mod_name = "ubunye_test_dynamic_model_xyz"
        (tmp_path / f"{mod_name}.py").write_text(
            _VALID_MODEL_CODE.replace("class MyModel", "class DynModel"), encoding="utf-8",
        )
        sys.path.insert(0, str(tmp_path))
        try:
            cls = load_model_class(None, f"{mod_name}.DynModel")
            from ubunye.models.base import UbunyeModel

            assert issubclass(cls, UbunyeModel)
        finally:
            sys.path.remove(str(tmp_path))

    def test_missing_dot_in_class_name_raises(self, tmp_path):
        """class_name without a dot raises ImportError."""
        with pytest.raises(ImportError, match="module.ClassName"):
            load_model_class(str(tmp_path), "ModelWithNoDot")
