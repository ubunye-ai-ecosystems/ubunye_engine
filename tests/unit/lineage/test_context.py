"""Unit tests for RunContext and StepRecord dataclasses."""

from ubunye.lineage.context import RunContext, StepRecord, _location_from_io_cfg

# ---------------------------------------------------------------------------
# _location_from_io_cfg helper
# ---------------------------------------------------------------------------


class TestLocationHelper:
    def test_hive_uses_db_and_tbl(self):
        assert (
            _location_from_io_cfg({"format": "hive", "db_name": "raw", "tbl_name": "claims"})
            == "raw.claims"
        )

    def test_unity_uses_db_and_tbl(self):
        assert _location_from_io_cfg({"format": "unity", "db_name": "d", "tbl_name": "t"}) == "d.t"

    def test_hive_falls_back_to_table_when_no_db(self):
        loc = _location_from_io_cfg({"format": "hive", "table": "my_table"})
        assert loc == "my_table"

    def test_jdbc_url_with_table(self):
        loc = _location_from_io_cfg(
            {"format": "jdbc", "url": "jdbc:pg://host/db", "table": "claims"}
        )
        assert "jdbc:pg://host/db" in loc
        assert "claims" in loc

    def test_s3_uses_path(self):
        assert (
            _location_from_io_cfg({"format": "s3", "path": "s3a://bucket/key"})
            == "s3a://bucket/key"
        )

    def test_binary_uses_path(self):
        assert _location_from_io_cfg({"format": "binary", "path": "/tmp/file"}) == "/tmp/file"

    def test_delta_uses_path(self):
        assert _location_from_io_cfg({"format": "delta", "path": "/delta/table"}) == "/delta/table"

    def test_rest_api_uses_url(self):
        assert (
            _location_from_io_cfg({"format": "rest_api", "url": "https://api.example.com/v1"})
            == "https://api.example.com/v1"
        )

    def test_unknown_format_falls_back_gracefully(self):
        loc = _location_from_io_cfg({"format": "custom", "path": "/some/path"})
        assert loc  # non-empty


# ---------------------------------------------------------------------------
# StepRecord
# ---------------------------------------------------------------------------


class TestStepRecord:
    def _make(self, **kwargs):
        defaults = dict(name="src", direction="input", format="hive", location="raw.claims")
        defaults.update(kwargs)
        return StepRecord(**defaults)

    def test_basic_fields(self):
        s = self._make()
        assert s.name == "src"
        assert s.direction == "input"
        assert s.format == "hive"
        assert s.location == "raw.claims"

    def test_optional_fields_default_none(self):
        s = self._make()
        assert s.row_count is None
        assert s.schema_hash is None
        assert s.data_hash is None

    def test_to_dict_contains_all_keys(self):
        s = self._make(row_count=100, schema_hash="sha256:abc", data_hash="sha256:def")
        d = s.to_dict()
        assert d["name"] == "src"
        assert d["row_count"] == 100
        assert d["schema_hash"] == "sha256:abc"
        assert d["data_hash"] == "sha256:def"

    def test_from_dict_round_trip(self):
        s = self._make(row_count=50, schema_hash="sha256:x")
        s2 = StepRecord.from_dict(s.to_dict())
        assert s2.name == s.name
        assert s2.row_count == s.row_count
        assert s2.schema_hash == s.schema_hash

    def test_from_dict_handles_missing_optionals(self):
        d = {"name": "sink", "direction": "output", "format": "s3", "location": "s3a://b/k"}
        s = StepRecord.from_dict(d)
        assert s.row_count is None
        assert s.schema_hash is None

    def test_from_io_cfg_hive(self):
        io = {"format": "hive", "db_name": "raw", "tbl_name": "tbl"}
        s = StepRecord.from_io_cfg("source", "input", io)
        assert s.name == "source"
        assert s.direction == "input"
        assert s.format == "hive"
        assert s.location == "raw.tbl"

    def test_from_io_cfg_s3(self):
        io = {"format": "s3", "path": "s3a://bucket/prefix"}
        s = StepRecord.from_io_cfg("sink", "output", io)
        assert s.location == "s3a://bucket/prefix"


# ---------------------------------------------------------------------------
# RunContext
# ---------------------------------------------------------------------------

_MINIMAL = dict(
    run_id="run-123",
    task_path="fraud/ingestion/etl",
    usecase="fraud",
    package="ingestion",
    task_name="etl",
    profile="dev",
    model="etl",
    version="0.1.0",
    config_hash="sha256:abc",
    started_at="2025-03-01T10:00:00+00:00",
)


class TestRunContext:
    def test_basic_creation(self):
        ctx = RunContext(**_MINIMAL)
        assert ctx.run_id == "run-123"
        assert ctx.status == "running"
        assert ctx.inputs == []
        assert ctx.outputs == []
        assert ctx.error is None

    def test_to_dict_has_all_required_keys(self):
        ctx = RunContext(**_MINIMAL)
        d = ctx.to_dict()
        for key in (
            "run_id",
            "task_path",
            "usecase",
            "package",
            "task_name",
            "profile",
            "model",
            "version",
            "config_hash",
            "started_at",
            "status",
            "inputs",
            "outputs",
        ):
            assert key in d, f"Missing key: {key}"

    def test_to_dict_inputs_outputs_are_lists(self):
        ctx = RunContext(**_MINIMAL)
        d = ctx.to_dict()
        assert isinstance(d["inputs"], list)
        assert isinstance(d["outputs"], list)

    def test_from_dict_round_trip(self):
        ctx = RunContext(**_MINIMAL)
        ctx.status = "success"
        ctx.duration_sec = 5.3
        ctx.ended_at = "2025-03-01T10:00:05+00:00"
        ctx.inputs = [StepRecord("src", "input", "hive", "raw.claims", row_count=100)]
        d = ctx.to_dict()
        ctx2 = RunContext.from_dict(d)
        assert ctx2.run_id == ctx.run_id
        assert ctx2.status == "success"
        assert ctx2.duration_sec == 5.3
        assert len(ctx2.inputs) == 1
        assert ctx2.inputs[0].name == "src"
        assert ctx2.inputs[0].row_count == 100

    def test_from_dict_handles_missing_optional_fields(self):
        d = dict(_MINIMAL)  # no ended_at, duration_sec, error
        ctx = RunContext.from_dict(d)
        assert ctx.ended_at is None
        assert ctx.duration_sec is None
        assert ctx.error is None

    def test_status_default_is_running(self):
        ctx = RunContext(**_MINIMAL)
        assert ctx.status == "running"

    def test_error_field_round_trips(self):
        ctx = RunContext(**_MINIMAL)
        ctx.status = "error"
        ctx.error = "Something went wrong"
        ctx2 = RunContext.from_dict(ctx.to_dict())
        assert ctx2.error == "Something went wrong"
        assert ctx2.status == "error"
