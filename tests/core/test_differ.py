"""Tests for the data differ module."""

from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import (
    ColumnInfo,
    DataDiffer,
    DiffResult,
    DiffType,
    SchemaComparisonResult,
)


class TestColumnInfo:
    """Tests for ColumnInfo dataclass."""

    def test_column_equality(self):
        """Test column name comparison (case-insensitive)."""
        col1 = ColumnInfo(name="Name", data_type="VARCHAR")
        col2 = ColumnInfo(name="name", data_type="TEXT")

        assert col1 == col2

    def test_type_matching(self):
        """Test type compatibility checking."""
        col1 = ColumnInfo(name="id", data_type="INTEGER")
        col2 = ColumnInfo(name="id", data_type="INT")

        assert col1.type_matches(col2)

    def test_type_normalization(self):
        """Test type normalization."""
        assert ColumnInfo._normalize_type("INT4") == "INTEGER"
        assert ColumnInfo._normalize_type("FLOAT8") == "DOUBLE"
        assert ColumnInfo._normalize_type("TEXT") == "VARCHAR"
        assert ColumnInfo._normalize_type("VARCHAR(255)") == "VARCHAR"


class TestSchemaComparison:
    """Tests for schema comparison."""

    def test_compare_identical_schemas(self, connector: DuckDBConnector):
        """Test comparing identical schemas."""
        connector.execute("""
            CREATE TABLE schema_a (id INT, name VARCHAR, value DECIMAL(10, 2))
        """)
        connector.execute("""
            CREATE TABLE schema_b (id INT, name VARCHAR, value DECIMAL(10, 2))
        """)

        differ = DataDiffer(connector)
        result = differ.compare_schemas("schema_a", "schema_b")

        assert result.is_identical
        assert result.is_compatible
        assert len(result.matching_columns) == 3
        assert len(result.source_only_columns) == 0
        assert len(result.target_only_columns) == 0

    def test_compare_different_schemas(self, schema_mismatch_tables: DuckDBConnector):
        """Test comparing different schemas."""
        differ = DataDiffer(schema_mismatch_tables)
        result = differ.compare_schemas("schema_source", "schema_target")

        assert not result.is_identical
        assert result.is_compatible  # Has some matching columns
        assert "old_column" in result.source_only_columns
        assert "new_column" in result.target_only_columns


class TestDataDiffer:
    """Tests for DataDiffer class."""

    def test_diff_with_differences(self, sample_tables: DuckDBConnector):
        """Test diffing tables with differences."""
        differ = DataDiffer(sample_tables)
        result = differ.diff(
            source_table="source_table",
            target_table="target_table",
            key_column="id",
        )

        assert isinstance(result, DiffResult)
        assert result.source_row_count == 5
        assert result.target_row_count == 5
        assert result.total_differences > 0

        # Check specific differences
        assert result.added_count == 1  # Frank added
        assert result.removed_count == 1  # Diana removed
        assert result.modified_count >= 1  # Bob and/or Charlie modified

    def test_diff_identical_tables(self, identical_tables: DuckDBConnector):
        """Test diffing identical tables."""
        differ = DataDiffer(identical_tables)
        result = differ.diff(
            source_table="identical_source",
            target_table="identical_target",
            key_column="id",
        )

        assert result.is_match
        assert result.total_differences == 0

    def test_diff_with_null_handling(self, null_handling_tables: DuckDBConnector):
        """Test that NULL values are handled correctly."""
        differ = DataDiffer(null_handling_tables)
        result = differ.diff(
            source_table="null_source",
            target_table="null_target",
            key_column="id",
        )

        # Tables have identical data including NULLs
        assert result.is_match
        assert result.total_differences == 0

    def test_diff_with_threshold(self, sample_tables: DuckDBConnector):
        """Test diff with threshold."""
        differ = DataDiffer(sample_tables)
        result = differ.diff(
            source_table="source_table",
            target_table="target_table",
            key_column="id",
            threshold=0.5,  # 50% threshold
        )

        # With 50% threshold, some differences should be within tolerance
        assert result.threshold == 0.5
        # Check if within threshold (depends on actual diff percentage)
        assert result.diff_percentage < 100

    def test_diff_with_column_selection(self, sample_tables: DuckDBConnector):
        """Test diff with specific columns."""
        differ = DataDiffer(sample_tables)
        result = differ.diff(
            source_table="source_table",
            target_table="target_table",
            key_column="id",
            columns=["id", "name"],  # Only compare id and name
        )

        assert "id" in result.columns_compared
        assert "name" in result.columns_compared
        # email changes should not be detected since we didn't include it
        # (but this depends on what changed)

    def test_diff_with_limit(self, sample_tables: DuckDBConnector):
        """Test diff with result limit."""
        differ = DataDiffer(sample_tables)
        result = differ.diff(
            source_table="source_table",
            target_table="target_table",
            key_column="id",
            limit=1,
        )

        # Should only return up to 1 difference
        assert len(result.differences) <= 1

    def test_quick_check_matching_tables(self, identical_tables: DuckDBConnector):
        """Test quick check on identical tables."""
        differ = DataDiffer(identical_tables)
        is_match = differ.quick_check(
            source_table="identical_source",
            target_table="identical_target",
            key_column="id",
        )

        assert is_match is True

    def test_quick_check_different_tables(self, sample_tables: DuckDBConnector):
        """Test quick check on different tables."""
        differ = DataDiffer(sample_tables)
        is_match = differ.quick_check(
            source_table="source_table",
            target_table="target_table",
            key_column="id",
        )

        assert is_match is False


class TestDiffResult:
    """Tests for DiffResult dataclass."""

    def test_diff_percentage_calculation(self):
        """Test diff percentage calculation."""
        result = DiffResult(
            source_table="src",
            target_table="tgt",
            source_row_count=100,
            target_row_count=100,
            schema_comparison=SchemaComparisonResult([], []),
            differences=[],
        )

        assert result.diff_percentage == 0.0

    def test_diff_percentage_with_differences(self):
        """Test diff percentage with differences."""
        from quack_diff.core.differ import RowDiff

        result = DiffResult(
            source_table="src",
            target_table="tgt",
            source_row_count=100,
            target_row_count=100,
            schema_comparison=SchemaComparisonResult([], []),
            differences=[
                RowDiff(key=1, diff_type=DiffType.MODIFIED),
                RowDiff(key=2, diff_type=DiffType.MODIFIED),
            ],
        )

        assert result.diff_percentage == 2.0

    def test_is_within_threshold(self):
        """Test threshold checking."""
        from quack_diff.core.differ import RowDiff

        result = DiffResult(
            source_table="src",
            target_table="tgt",
            source_row_count=100,
            target_row_count=100,
            schema_comparison=SchemaComparisonResult([], []),
            differences=[RowDiff(key=1, diff_type=DiffType.MODIFIED)],
            threshold=0.05,  # 5% threshold
        )

        assert result.is_within_threshold  # 1% < 5%

    def test_is_not_within_threshold(self):
        """Test when differences exceed threshold."""
        from quack_diff.core.differ import RowDiff

        result = DiffResult(
            source_table="src",
            target_table="tgt",
            source_row_count=100,
            target_row_count=100,
            schema_comparison=SchemaComparisonResult([], []),
            differences=[RowDiff(key=i, diff_type=DiffType.MODIFIED) for i in range(10)],
            threshold=0.05,  # 5% threshold
        )

        assert not result.is_within_threshold  # 10% > 5%
