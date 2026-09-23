"""Write-mode resolution and the two lakehouse modes (merge, overwrite_partitions).

This is the backend-agnostic decision layer: no Spark, no mocks — ``resolve()``
turns a config into a :class:`~ubunye.core.write_modes.ResolvedWriteMode`. The
Spark *execution* of those modes moved to the adapter under issue #37 and is
tested in ``tests/unit/adapters/spark/test_write_exec.py``.
"""

from __future__ import annotations

import pytest

from ubunye.core import write_modes
from ubunye.core.errors import SinkWriteError

ALL = write_modes.ALL_MODES


# ---------------------------------------------------------------------------
# resolve() — validation
# ---------------------------------------------------------------------------


class TestResolve:
    @pytest.mark.parametrize("mode", sorted(write_modes.NATIVE_SAVE_MODES))
    def test_native_modes_pass_through_to_spark(self, mode):
        resolved = write_modes.resolve(
            {"mode": mode}, connector="s3", supported=ALL, default="overwrite"
        )
        assert resolved.mode == mode
        assert resolved.save_mode == mode
        assert not resolved.is_merge
        assert not resolved.is_overwrite_partitions

    def test_error_is_an_alias_for_errorifexists(self):
        resolved = write_modes.resolve(
            {"mode": "error"}, connector="s3", supported=ALL, default="append"
        )
        assert resolved.save_mode == "errorifexists"

    def test_mode_is_case_insensitive(self):
        resolved = write_modes.resolve(
            {"mode": "APPEND"}, connector="s3", supported=ALL, default="overwrite"
        )
        assert resolved.save_mode == "append"

    def test_default_applies_when_mode_absent(self):
        resolved = write_modes.resolve({}, connector="jdbc", supported=ALL, default="append")
        assert resolved.save_mode == "append"

    def test_accepts_a_writemode_enum_not_just_a_string(self):
        """Engine.write_outputs() is public: a caller passing a bare model_dump()
        (rather than model_dump(mode="json")) hands the writer a WriteMode enum,
        whose str() is 'WriteMode.APPEND'."""
        from ubunye.config.schema import WriteMode

        resolved = write_modes.resolve(
            {"mode": WriteMode.APPEND}, connector="s3", supported=ALL, default="overwrite"
        )
        assert resolved.save_mode == "append"

    def test_unknown_mode_raises(self):
        with pytest.raises(SinkWriteError, match="Unknown write mode"):
            write_modes.resolve(
                {"mode": "truncate"}, connector="s3", supported=ALL, default="append"
            )

    def test_unsupported_by_connector_raises_and_lists_what_works(self):
        with pytest.raises(SinkWriteError, match="not supported by the 'jdbc' connector"):
            write_modes.resolve(
                {"mode": "merge", "merge_keys": ["id"]},
                connector="jdbc",
                supported=write_modes.NATIVE_SAVE_MODES,
                default="append",
            )


# ---------------------------------------------------------------------------
# resolve() — merge
# ---------------------------------------------------------------------------


class TestResolveMerge:
    def test_merge_keys_from_list(self):
        resolved = write_modes.resolve(
            {"mode": "merge", "merge_keys": ["id", "dt"]},
            connector="unity",
            supported=ALL,
            default="append",
            file_format="delta",
        )
        assert resolved.is_merge
        assert resolved.merge_keys == ("id", "dt")

    def test_merge_keys_from_comma_string_in_options(self):
        """The shape the connector docs use: options.merge_keys: 'id,dt'."""
        resolved = write_modes.resolve(
            {"mode": "merge", "options": {"merge_keys": "id, dt"}},
            connector="s3",
            supported=ALL,
            default="overwrite",
            file_format="delta",
        )
        assert resolved.merge_keys == ("id", "dt")

    def test_merge_without_keys_raises(self):
        with pytest.raises(SinkWriteError, match="requires 'merge_keys'"):
            write_modes.resolve(
                {"mode": "merge"},
                connector="s3",
                supported=ALL,
                default="overwrite",
                file_format="delta",
            )

    def test_merge_on_non_delta_format_raises(self):
        with pytest.raises(SinkWriteError, match="requires a Delta target"):
            write_modes.resolve(
                {"mode": "merge", "merge_keys": ["id"]},
                connector="s3",
                supported=ALL,
                default="overwrite",
                file_format="parquet",
            )

    def test_merge_falls_back_to_overwrite_for_target_creation(self):
        """First run has nothing to merge into — the target is created outright."""
        resolved = write_modes.resolve(
            {"mode": "merge", "merge_keys": ["id"]},
            connector="unity",
            supported=ALL,
            default="append",
            file_format="delta",
        )
        assert resolved.save_mode == "overwrite"


# ---------------------------------------------------------------------------
# resolve() — overwrite_partitions
# ---------------------------------------------------------------------------


class TestResolveOverwritePartitions:
    def test_partitioned_target_uses_dynamic_conf(self):
        resolved = write_modes.resolve(
            {"mode": "overwrite_partitions", "partitionBy": ["dt"]},
            connector="s3",
            supported=ALL,
            default="overwrite",
            file_format="parquet",
        )
        assert resolved.is_overwrite_partitions
        assert resolved.save_mode == "overwrite"

    def test_replace_where_predicate_wins_over_dynamic_conf(self):
        resolved = write_modes.resolve(
            {"mode": "overwrite_partitions", "replace_where": "dt = '2026-01-01'"},
            connector="unity",
            supported=ALL,
            default="append",
            file_format="delta",
        )
        assert resolved.options == {"replaceWhere": "dt = '2026-01-01'"}

    def test_unpartitioned_without_predicate_raises(self):
        """Dynamic overwrite of an unpartitioned target is a silent full wipe."""
        with pytest.raises(SinkWriteError, match="requires 'partitionBy' or 'replace_where'"):
            write_modes.resolve(
                {"mode": "overwrite_partitions"},
                connector="s3",
                supported=ALL,
                default="overwrite",
                file_format="parquet",
            )

    def test_replace_where_on_non_delta_raises(self):
        with pytest.raises(SinkWriteError, match="'replace_where' requires a Delta target"):
            write_modes.resolve(
                {"mode": "overwrite_partitions", "replace_where": "dt = '2026-01-01'"},
                connector="s3",
                supported=ALL,
                default="overwrite",
                file_format="parquet",
            )


# ---------------------------------------------------------------------------
# Backward-compat shim (issue #37) — moved names still reachable, with a warning
# ---------------------------------------------------------------------------


class TestDeprecationShim:
    @pytest.mark.parametrize(
        "name", ["apply", "merge_into", "target_exists", "dynamic_partition_overwrite"]
    )
    def test_moved_name_reexported_with_deprecation_warning(self, name):
        from ubunye.adapters.spark import write_exec

        with pytest.warns(DeprecationWarning, match="moved to"):
            shimmed = getattr(write_modes, name)
        assert shimmed is getattr(write_exec, name)

    def test_genuinely_unknown_attribute_still_raises(self):
        with pytest.raises(AttributeError):
            write_modes.does_not_exist  # noqa: B018
