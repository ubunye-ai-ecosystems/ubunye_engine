"""The guarantees that make lineage safe at a billion rows.

The old path executed the output's whole plan two to three times per output (count in
the hasher, unbounded sample collect, then a SECOND count in the recorder on the same
uncached DataFrame), and had a fallback that collected the ENTIRE table to the driver.

These tests pin the new contract:
  1. count() runs exactly once.
  2. Every collect() is bounded by limit(); the whole-table collect is gone.
  3. The DataFrame is persisted for the work and unpersisted after, even on failure.
  4. The recorder gets everything from one fingerprint call.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from ubunye.lineage.hasher import MAX_SAMPLE_ROWS, fingerprint_dataframe


def _df(n_rows=5, sample_empty=False):
    """A mock that counts how it is used."""
    df = MagicMock()
    df.count.return_value = n_rows

    row = MagicMock()
    row.asDict.return_value = {"id": 1}

    sampled = MagicMock()
    limited = MagicMock()
    limited.collect.return_value = [] if sample_empty else [row]
    sampled.limit.return_value = limited
    df.sample.return_value = sampled

    head = MagicMock()
    head.collect.return_value = [row]
    df.limit.return_value = head
    return df


def test_count_runs_exactly_once():
    """The recorder used to count AGAIN after the hasher had already counted.

    On an uncached DataFrame each count re-executes the entire upstream plan, so
    lineage silently doubled the cost of every pipeline it observed.
    """
    df = _df()
    result = fingerprint_dataframe(df)

    assert df.count.call_count == 1
    assert result.row_count == 5  # and the caller gets the count from that same pass


def test_no_collect_is_ever_unbounded():
    """collect() may only be called on the result of a limit().

    The old empty-sample fallback was `df.collect()` — the ENTIRE table onto one JVM.
    """
    df = _df(sample_empty=True)
    fingerprint_dataframe(df)

    df.collect.assert_not_called()  # never directly on the frame
    df.sample.return_value.limit.assert_called_once_with(MAX_SAMPLE_ROWS)
    df.limit.assert_called_once_with(MAX_SAMPLE_ROWS)  # the bounded fallback


def test_persisted_for_the_duration_and_released():
    """One materialisation serves both actions, and the cache never leaks."""
    df = _df()
    fingerprint_dataframe(df)

    df.persist.assert_called_once()
    df.unpersist.assert_called_once()


def test_unpersist_happens_even_when_fingerprinting_fails():
    """A cache leak on the failure path would grow the cluster's memory forever."""
    df = _df()
    df.sample.side_effect = RuntimeError("executor lost")

    result = fingerprint_dataframe(df)

    df.unpersist.assert_called_once()
    assert result.schema_hash  # and the failure still yielded a best-effort record


def test_a_frame_that_cannot_persist_still_fingerprints():
    """persist() is an optimisation, not a requirement. Mocks, pandas and
    already-persisted frames must not break lineage."""
    df = _df()
    df.persist.side_effect = AttributeError("no persist here")

    result = fingerprint_dataframe(df)

    assert result.row_count == 5
    df.unpersist.assert_not_called()  # nothing was persisted, nothing to release


def test_empty_frame_is_cheap_and_honest():
    df = _df(n_rows=0)
    result = fingerprint_dataframe(df)

    assert result.row_count == 0
    assert result.data_hash == result.schema_hash  # nothing to hash but the shape
    df.sample.assert_not_called()  # no pointless second action on an empty frame
