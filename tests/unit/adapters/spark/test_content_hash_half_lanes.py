"""The Spark hash sums 32 bit half lanes as long (F-041); the digest must not change.

A lane is ``hi * 2**32 + lo``. These tests need no Spark session: they check the
arithmetic that turns four half lane sums into the two lane sums, including sums
that wrapped in a Spark ``long`` (ANSI off, past about 2.1 billion rows). The
decimal fallback is tested on live Spark in tests/integration.
"""

from __future__ import annotations

import random

import pytest

pytest.importorskip("pyspark")

from ubunye.adapters.spark import content_hash as sch  # noqa: E402
from ubunye.lineage import content_hash as ch  # noqa: E402

MASK = (1 << 64) - 1


def _as_spark_long(n: int) -> int:
    """What a Spark long holds after summing past its range: n modulo 2**64, signed."""
    n &= MASK
    return n - (1 << 64) if n >= 1 << 63 else n


def _digest(sums):
    return ch.data_hash([("id", "int64")], 3, sums)


def test_half_lanes_give_the_lane_sums():
    rng = random.Random(41)
    lanes = [(rng.getrandbits(64), rng.getrandbits(64)) for _ in range(1000)]
    a, b = sum(x for x, _ in lanes), sum(y for _, y in lanes)
    halves = [
        sum(x >> 32 for x, _ in lanes),
        sum(x & 0xFFFFFFFF for x, _ in lanes),
        sum(y >> 32 for _, y in lanes),
        sum(y & 0xFFFFFFFF for _, y in lanes),
    ]
    assert sch.lanes_from_halves(halves) == (a, b)


def test_sums_that_wrapped_in_a_spark_long_give_the_same_digest():
    # Three billion rows of the largest half lane: every sum is past 2**63.
    rows, top = 3_000_000_000, 0xFFFFFFFF
    true = [rows * top] * 4
    wrapped = [_as_spark_long(s) for s in true]
    assert wrapped != true
    assert _digest(sch.lanes_from_halves(wrapped)) == _digest(sch.lanes_from_halves(true))


def test_null_sums_of_an_empty_frame_are_zero():
    assert sch.lanes_from_halves([None, None, None, None]) == (0, 0)
