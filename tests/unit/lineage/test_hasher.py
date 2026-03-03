"""Unit tests for the lineage hasher module (no Spark required)."""

import pytest

from ubunye.lineage.hasher import _sha256, hash_dataframe, hash_file, hash_schema

# ---------------------------------------------------------------------------
# Mock DataFrame (Spark-free)
# ---------------------------------------------------------------------------


class MockField:
    def __init__(self, name, dtype="string", nullable=True):
        self.name = name
        self.dataType = dtype
        self.nullable = nullable


class MockSchema:
    def __init__(self, fields):
        self._fields = fields

    def jsonValue(self):
        return {
            "type": "struct",
            "fields": [
                {"name": f.name, "type": f.dataType, "nullable": f.nullable} for f in self._fields
            ],
        }


class MockRow:
    def __init__(self, **kwargs):
        self._data = kwargs

    def asDict(self, recursive=False):
        return dict(self._data)


class MockDF:
    def __init__(self, rows, schema_fields=None):
        self._rows = rows
        self.schema = MockSchema(schema_fields or [MockField("id"), MockField("value")])

    def count(self):
        return len(self._rows)

    def sample(self, fraction=0.01, seed=42, withReplacement=False):
        # Return all rows for simplicity
        return MockDF(self._rows, self.schema._fields)

    def collect(self):
        return self._rows


# ---------------------------------------------------------------------------
# _sha256
# ---------------------------------------------------------------------------


class TestSha256:
    def test_returns_prefix(self):
        assert _sha256(b"hello").startswith("sha256:")

    def test_deterministic(self):
        assert _sha256(b"data") == _sha256(b"data")

    def test_different_data_different_hash(self):
        assert _sha256(b"foo") != _sha256(b"bar")

    def test_hex_length(self):
        h = _sha256(b"test")
        # "sha256:" + 64 hex chars
        assert len(h) == len("sha256:") + 64


# ---------------------------------------------------------------------------
# hash_schema
# ---------------------------------------------------------------------------


class TestHashSchema:
    def test_returns_sha256_string(self):
        df = MockDF([MockRow(id=1)])
        h = hash_schema(df)
        assert h.startswith("sha256:")

    def test_same_schema_same_hash(self):
        df1 = MockDF([MockRow(id=1)], [MockField("id"), MockField("val")])
        df2 = MockDF([MockRow(id=2)], [MockField("id"), MockField("val")])
        assert hash_schema(df1) == hash_schema(df2)

    def test_different_schema_different_hash(self):
        df1 = MockDF([], [MockField("id")])
        df2 = MockDF([], [MockField("name")])
        assert hash_schema(df1) != hash_schema(df2)

    def test_column_order_matters(self):
        df1 = MockDF([], [MockField("a"), MockField("b")])
        df2 = MockDF([], [MockField("b"), MockField("a")])
        # column order changes the schema JSON, so hashes differ
        assert hash_schema(df1) != hash_schema(df2)


# ---------------------------------------------------------------------------
# hash_dataframe
# ---------------------------------------------------------------------------


class TestHashDataframe:
    def test_returns_sha256_string(self):
        rows = [MockRow(id=i, val=str(i)) for i in range(10)]
        df = MockDF(rows)
        h = hash_dataframe(df)
        assert h.startswith("sha256:")

    def test_empty_df_falls_back_to_schema_hash(self):
        df = MockDF([])
        h = hash_dataframe(df)
        schema_h = hash_schema(df)
        assert h == schema_h

    def test_same_rows_same_hash(self):
        rows = [MockRow(id=1, val="a"), MockRow(id=2, val="b")]
        df1 = MockDF(rows)
        df2 = MockDF(rows)
        assert hash_dataframe(df1) == hash_dataframe(df2)

    def test_different_rows_different_hash(self):
        df1 = MockDF([MockRow(id=1)])
        df2 = MockDF([MockRow(id=999)])
        assert hash_dataframe(df1) != hash_dataframe(df2)

    def test_sample_fraction_clamped(self):
        rows = [MockRow(id=i) for i in range(100)]
        df = MockDF(rows)
        # fraction > 1 should not crash — clamped to 1.0
        h = hash_dataframe(df, sample_fraction=5.0)
        assert h.startswith("sha256:")

    def test_sample_fraction_small_does_not_crash(self):
        rows = [MockRow(id=i) for i in range(5)]
        df = MockDF(rows)
        h = hash_dataframe(df, sample_fraction=0.0001)
        assert h.startswith("sha256:")

    def test_empty_sample_falls_back_to_collect_not_schema(self):
        """Regression: PySpark .sample(0.01) on 2-3 rows often returns [].
        hash_dataframe must fall back to df.collect(), not hash_schema(), so
        DataFrames with different data (same schema) get different hashes.
        """

        class EmptySampleDF:
            """Mock where sample() always returns [] but collect() has rows."""

            def __init__(self, rows):
                self._rows = rows
                self.schema = MockSchema([MockField("id"), MockField("val")])

            def count(self):
                return len(self._rows)

            def sample(self, **_kwargs):
                return EmptySampleDF([])  # always empty, like tiny fraction on small df

            def collect(self):
                return self._rows

        rows_a = [MockRow(id=1, val="x"), MockRow(id=2, val="y"), MockRow(id=3, val="z")]
        rows_b = [MockRow(id=10, val="p"), MockRow(id=20, val="q")]

        df_a = EmptySampleDF(rows_a)
        df_b = EmptySampleDF(rows_b)

        hash_a = hash_dataframe(df_a)
        hash_b = hash_dataframe(df_b)

        # Must differ — different data, same schema
        assert hash_a != hash_b, (
            "hash_dataframe fell back to schema hash instead of collecting rows; "
            f"both returned {hash_a}"
        )


# ---------------------------------------------------------------------------
# hash_file
# ---------------------------------------------------------------------------


class TestHashFile:
    def test_returns_sha256_string(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_bytes(b"hello world")
        h = hash_file(str(f))
        assert h.startswith("sha256:")

    def test_same_file_same_hash(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(b"\x00\x01\x02" * 1000)
        assert hash_file(str(f)) == hash_file(str(f))

    def test_different_content_different_hash(self, tmp_path):
        f1 = tmp_path / "a.bin"
        f2 = tmp_path / "b.bin"
        f1.write_bytes(b"content_a")
        f2.write_bytes(b"content_b")
        assert hash_file(str(f1)) != hash_file(str(f2))

    def test_missing_file_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            hash_file(str(tmp_path / "nonexistent.txt"))

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.bin"
        f.write_bytes(b"")
        h = hash_file(str(f))
        assert h.startswith("sha256:")
