"""Unit tests for ubunye.core.task_runner.

Focus: the `_with_task_dir_on_path` context manager must not let adjacent
modules (`model.py`, `utils.py`, …) leak between successive tasks. Two
tasks whose dirs each ship their own `model.py` would otherwise get the
first task's module cached in `sys.modules` and fail on the second task
with stale-module errors.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

from ubunye.core.task_runner import _load_task_class, _with_task_dir_on_path


def _write_task(
    dir_path: Path,
    *,
    model_class_name: str,
    task_class_name: str,
    model_marker: str,
) -> None:
    """Emit a minimal task_dir with an adjacent ``model.py`` and ``transformations.py``."""
    dir_path.mkdir(parents=True, exist_ok=True)
    (dir_path / "model.py").write_text(
        textwrap.dedent(
            f"""
            class {model_class_name}:
                marker = {model_marker!r}
            """
        )
    )
    (dir_path / "transformations.py").write_text(
        textwrap.dedent(
            f"""
            from ubunye.core.interfaces import Task
            from model import {model_class_name}

            class {task_class_name}(Task):
                def transform(self, sources):
                    return {{"marker": {model_class_name}.marker}}
            """
        )
    )


class TestSiblingModuleIsolation:
    """Regression guard: sibling modules from one task must not leak into the next."""

    def test_two_tasks_with_same_named_model_do_not_collide(self, tmp_path):
        task_a = tmp_path / "task_a"
        task_b = tmp_path / "task_b"

        _write_task(
            task_a,
            model_class_name="ModelA",
            task_class_name="TaskA",
            model_marker="i-am-a",
        )
        _write_task(
            task_b,
            model_class_name="ModelB",
            task_class_name="TaskB",
            model_marker="i-am-b",
        )

        with _with_task_dir_on_path(task_a):
            cls_a = _load_task_class(task_a)
            assert cls_a.__name__ == "TaskA"

        with _with_task_dir_on_path(task_b):
            cls_b = _load_task_class(task_b)
            assert cls_b.__name__ == "TaskB"

    def test_sibling_module_is_evicted_after_context_exit(self, tmp_path):
        task_a = tmp_path / "task_a"
        _write_task(
            task_a,
            model_class_name="ModelA",
            task_class_name="TaskA",
            model_marker="only-a",
        )

        with _with_task_dir_on_path(task_a):
            _load_task_class(task_a)
            assert "model" in sys.modules, "sanity: model loaded while in context"

        assert "model" not in sys.modules, (
            "model.py from task_a leaked into sys.modules after context exit — "
            "a second task with its own model.py would see stale state."
        )

    def test_unrelated_modules_are_not_evicted(self, tmp_path):
        """Safety: we only evict modules loaded from task_dir, not stdlib/site-packages."""
        import json  # pre-loaded, lives in stdlib, not under tmp_path

        task_a = tmp_path / "task_a"
        _write_task(
            task_a,
            model_class_name="ModelA",
            task_class_name="TaskA",
            model_marker="only-a",
        )

        with _with_task_dir_on_path(task_a):
            _load_task_class(task_a)

        assert "json" in sys.modules and sys.modules["json"] is json, (
            "Context manager evicted an unrelated stdlib module — too aggressive."
        )
