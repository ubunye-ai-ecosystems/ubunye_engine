"""When holding an output fails: fall back only if the platform refused (ADR 009).

A failing job is raised, never retried by the fallback, since a second attempt
repeats every side effect of the transform (skeptic attack 5). A frame that comes
back unheld is not trusted as held (skeptic attack 8).
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("pyspark")

from ubunye.adapters.spark import materialise  # noqa: E402


class PythonException(Exception):
    """Named like pyspark's: a Python worker failed inside the job."""


class Plan:
    def __init__(self, name: str) -> None:
        self.name = name

    def getClass(self) -> Any:
        name = self.name

        class Cls:
            def getSimpleName(self) -> str:
                return name

        return Cls()


class Frame:
    isStreaming = False

    def __init__(self, raises: BaseException | None = None, plan: str = "LogicalRDD") -> None:
        self.raises = raises
        self.plan = plan

    def localCheckpoint(self, eager: bool = True, storageLevel: Any = None) -> Any:
        if self.raises is not None:
            raise self.raises
        held = Frame(plan=self.plan)
        held._jdf = self._jdf_for(self.plan)
        return held

    @staticmethod
    def _jdf_for(plan: str) -> Any:
        class QE:
            def logical(self) -> Plan:
                return Plan(plan)

        class JDF:
            def queryExecution(self) -> QE:
                return QE()

        return JDF()


def test_a_held_frame_is_returned():
    assert materialise.materialise(Frame()) is not None


@pytest.mark.parametrize(
    "exc",
    [
        NotImplementedError("localCheckpoint"),
        RuntimeError("[NOT_SUPPORTED_WITH_SERVERLESS] localCheckpoint is not supported"),
        RuntimeError("[UNSUPPORTED_FEATURE] checkpointing"),
    ],
)
def test_a_platform_refusal_falls_back(exc):
    assert materialise.materialise(Frame(raises=exc)) is None


@pytest.mark.parametrize(
    "exc",
    [
        PythonException("An exception was thrown from the Python worker: ValueError: row 2999"),
        RuntimeError("Job aborted due to stage failure: Task 0 in stage 3.0 failed"),
        PythonException("[NOT_SUPPORTED] raised by the user's own UDF"),
    ],
)
def test_a_failing_job_is_raised_not_retried(exc):
    with pytest.raises(type(exc)):
        materialise.materialise(Frame(raises=exc))


def test_a_frame_that_is_not_held_is_not_trusted():
    assert materialise.materialise(Frame(plan="Project")) is None
