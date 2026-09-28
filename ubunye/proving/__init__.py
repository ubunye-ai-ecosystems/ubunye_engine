"""The proving ground: the same workload in many places, compared by its receipts.

An **observation** is what one environment did with one workload: the run record it
left (ADR 006), or why there is none (it failed to launch, it was not run, the
environment does not support it), plus what the record cannot know about itself (the
platform, the cost and where that figure came from, a link to the run).

A **matrix** compares every observation of a workload with a reference environment,
dimension by dimension, from the records alone:

========== ==================================================================
execute    the run succeeded (``status: success``)
identity   the same workload: the same task code (``code_hash``) and outputs
inputs     every input hashed to the same rows (``rows-v1``), where recorded
data       every output hashed to the same rows, by the same method
schema     every output has the same canonical schema
rows       every output has the same row count
========== ==================================================================

Each is PASS, FAIL, PARTIAL (some of it could not be compared), NOT RECORDED, or
NOT RUN / UNSUPPORTED for an environment that did not execute. An environment that was
expected and has no observation is NOT RUN, never a pass. The configuration hash is
shown but not compared: it covers paths, and a cloud reads ``s3://`` where a laptop
reads a folder. Byte-for-byte equality of written files is not claimed.

Nothing here runs a workload; it reads what runs left behind.
"""

from __future__ import annotations

from ubunye.proving.evidence import (
    COST_BASES,
    STATUSES,
    Observation,
    load_observations,
    observe_record,
    skipped,
)
from ubunye.proving.matrix import VERDICTS, compare, render_markdown

__all__ = [
    "COST_BASES",
    "STATUSES",
    "VERDICTS",
    "Observation",
    "compare",
    "load_observations",
    "observe_record",
    "render_markdown",
    "skipped",
]
