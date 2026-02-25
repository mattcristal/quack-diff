"""Tests for the data differ module."""

import pytest

from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import (
    ColumnInfo,
    CountResult,
    DataDiffer,
    DiffResult,
    DiffType,
    SchemaComparisonResult,
    TableCount,
)
from quack_diff.core.sql_utils import KeyColumnError, SchemaError, TableNotFoundError


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

    def test_count_check_matching_tables(self, identical_tables: DuckDBConnector):
        """Test count_check when all tables have the same row count."""
        differ = DataDiffer(identical_tables)
        result = differ.count_check(
            tables=["identical_source", "identical_target"],
            key_column=None,
        )

        assert isinstance(result, CountResult)
        assert result.is_match
        assert len(result.table_counts) == 2
        assert result.table_counts[0].count == result.table_counts[1].count == 3
        assert result.mode == "rows"
        assert result.expected_count == 3

    def test_count_check_mismatching_tables(self, connector: DuckDBConnector):
        """Test count_check when tables have different row counts."""
        connector.execute("CREATE TABLE small (id INT)")
        connector.execute("INSERT INTO small VALUES (1), (2)")
        connector.execute("CREATE TABLE large (id INT)")
        connector.execute("INSERT INTO large VALUES (1), (2), (3), (4)")

        differ = DataDiffer(connector)
        result = differ.count_check(
            tables=["small", "large"],
            key_column=None,
        )

        assert isinstance(result, CountResult)
        assert not result.is_match
        assert result.table_counts[0].table == "small"
        assert result.table_counts[0].count == 2
        assert result.table_counts[1].table == "large"
        assert result.table_counts[1].count == 4

    def test_count_check_three_tables_mismatch(self, connector: DuckDBConnector):
        """Test count_check with three tables where one has different count."""
        connector.execute("CREATE TABLE t1 (id INT)")
        connector.execute("INSERT INTO t1 VALUES (1), (2), (3)")
        connector.execute("CREATE TABLE t2 (id INT)")
        connector.execute("INSERT INTO t2 VALUES (1), (2), (3)")
        connector.execute("CREATE TABLE t3 (id INT)")
        connector.execute("INSERT INTO t3 VALUES (1), (2)")

        differ = DataDiffer(connector)
        result = differ.count_check(tables=["t1", "t2", "t3"], key_column=None)

        assert not result.is_match
        assert [tc.count for tc in result.table_counts] == [3, 3, 2]

    def test_count_check_distinct_key(self, identical_tables: DuckDBConnector):
        """Test count_check with COUNT(DISTINCT key)."""
        differ = DataDiffer(identical_tables)
        result = differ.count_check(
            tables=["identical_source", "identical_target"],
            key_column="id",
        )

        assert result.is_match
        assert result.mode == "distinct"
        assert result.key_column == "id"
        assert result.table_counts[0].count == 3
        assert result.table_counts[1].count == 3

    def test_count_check_empty_tables_list(self, connector: DuckDBConnector):
        """Test count_check with empty table list returns match."""
        differ = DataDiffer(connector)
        result = differ.count_check(tables=[], key_column=None)

        assert result.is_match
        assert result.table_counts == []
        assert result.expected_count is None

    def test_count_check_single_table(self, identical_tables: DuckDBConnector):
        """Test count_check with single table is match."""
        differ = DataDiffer(identical_tables)
        result = differ.count_check(
            tables=["identical_source"],
            key_column=None,
        )

        assert result.is_match
        assert len(result.table_counts) == 1
        assert result.table_counts[0].count == 3


class TestCountResult:
    """Tests for CountResult dataclass."""

    def test_mode_rows_when_no_key(self):
        """Test mode is 'rows' when key_column is None."""
        result = CountResult(
            table_counts=[TableCount("a", 10), TableCount("b", 10)],
            key_column=None,
            is_match=True,
        )
        assert result.mode == "rows"

    def test_mode_distinct_when_key_set(self):
        """Test mode is 'distinct' when key_column is set."""
        result = CountResult(
            table_counts=[TableCount("a", 10), TableCount("b", 10)],
            key_column="id",
            is_match=True,
        )
        assert result.mode == "distinct"

    def test_expected_count_first_table(self):
        """Test expected_count is first table's count."""
        result = CountResult(
            table_counts=[TableCount("a", 100), TableCount("b", 100)],
            key_column=None,
            is_match=True,
        )
        assert result.expected_count == 100

    def test_expected_count_empty(self):
        """Test expected_count is None when no tables."""
        result = CountResult(table_counts=[], key_column=None, is_match=True)
        assert result.expected_count is None


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


class TestDataDifferErrorHandling:
    """Tests for DataDiffer error handling."""

    def test_diff_nonexistent_source_table(self, connector: DuckDBConnector):
        """Test diff with non-existent source table raises TableNotFoundError."""
        connector.execute("CREATE TABLE real_target (id INT, name VARCHAR)")
        connector.execute("INSERT INTO real_target VALUES (1, 'test')")

        differ = DataDiffer(connector)

        with pytest.raises(TableNotFoundError) as exc_info:
            differ.diff(
                source_table="nonexistent_source",
                target_table="real_target",
                key_column="id",
            )

        assert "nonexistent_source" in exc_info.value.table

    def test_diff_nonexistent_target_table(self, connector: DuckDBConnector):
        """Test diff with non-existent target table raises TableNotFoundError."""
        connector.execute("CREATE TABLE real_source (id INT, name VARCHAR)")
        connector.execute("INSERT INTO real_source VALUES (1, 'test')")

        differ = DataDiffer(connector)

        with pytest.raises(TableNotFoundError) as exc_info:
            differ.diff(
                source_table="real_source",
                target_table="nonexistent_target",
                key_column="id",
            )

        assert "nonexistent_target" in exc_info.value.table

    def test_diff_key_column_not_in_source(self, connector: DuckDBConnector):
        """Test diff with key column not in source raises KeyColumnError."""
        connector.execute("CREATE TABLE source_no_key (name VARCHAR, value INT)")
        connector.execute("INSERT INTO source_no_key VALUES ('test', 1)")

        connector.execute("CREATE TABLE target_with_key (id INT, name VARCHAR, value INT)")
        connector.execute("INSERT INTO target_with_key VALUES (1, 'test', 1)")

        differ = DataDiffer(connector)

        with pytest.raises(KeyColumnError) as exc_info:
            differ.diff(
                source_table="source_no_key",
                target_table="target_with_key",
                key_column="id",
            )

        assert exc_info.value.key_column == "id"
        assert "source" in exc_info.value.message.lower()
        assert "Available columns" in exc_info.value.details

    def test_diff_key_column_not_in_target(self, connector: DuckDBConnector):
        """Test diff with key column not in target raises KeyColumnError."""
        connector.execute("CREATE TABLE source_with_key (id INT, name VARCHAR)")
        connector.execute("INSERT INTO source_with_key VALUES (1, 'test')")

        connector.execute("CREATE TABLE target_no_key (name VARCHAR, value INT)")
        connector.execute("INSERT INTO target_no_key VALUES ('test', 1)")

        differ = DataDiffer(connector)

        with pytest.raises(KeyColumnError) as exc_info:
            differ.diff(
                source_table="source_with_key",
                target_table="target_no_key",
                key_column="id",
            )

        assert exc_info.value.key_column == "id"
        assert "target" in exc_info.value.message.lower()

    def test_diff_key_column_type_mismatch(self, connector: DuckDBConnector):
        """Test diff with incompatible key column types raises KeyColumnError."""
        connector.execute("CREATE TABLE int_key_source (id INT, name VARCHAR)")
        connector.execute("INSERT INTO int_key_source VALUES (1, 'test')")

        connector.execute("CREATE TABLE str_key_target (id VARCHAR, name VARCHAR)")
        connector.execute("INSERT INTO str_key_target VALUES ('1', 'test')")

        differ = DataDiffer(connector)

        with pytest.raises(KeyColumnError) as exc_info:
            differ.diff(
                source_table="int_key_source",
                target_table="str_key_target",
                key_column="id",
            )

        assert exc_info.value.key_column == "id"
        assert "incompatible" in exc_info.value.message.lower()
        assert "INT" in exc_info.value.details.upper()
        assert "VARCHAR" in exc_info.value.details.upper()

    def test_compare_schemas_nonexistent_source(self, connector: DuckDBConnector):
        """Test schema comparison with non-existent source raises TableNotFoundError."""
        connector.execute("CREATE TABLE real_table (id INT)")

        differ = DataDiffer(connector)

        with pytest.raises(TableNotFoundError) as exc_info:
            differ.compare_schemas("ghost_source", "real_table")

        assert "ghost_source" in exc_info.value.table

    def test_compare_schemas_nonexistent_target(self, connector: DuckDBConnector):
        """Test schema comparison with non-existent target raises TableNotFoundError."""
        connector.execute("CREATE TABLE real_table (id INT)")

        differ = DataDiffer(connector)

        with pytest.raises(TableNotFoundError) as exc_info:
            differ.compare_schemas("real_table", "ghost_target")

        assert "ghost_target" in exc_info.value.table

    def test_get_schema_nonexistent_table(self, connector: DuckDBConnector):
        """Test getting schema of non-existent table raises TableNotFoundError."""
        differ = DataDiffer(connector)

        with pytest.raises(TableNotFoundError) as exc_info:
            differ.get_schema("nonexistent_table")

        assert "nonexistent_table" in exc_info.value.table

    def test_get_row_count_nonexistent_table(self, connector: DuckDBConnector):
        """Test getting row count of non-existent table raises TableNotFoundError."""
        differ = DataDiffer(connector)

        with pytest.raises(TableNotFoundError) as exc_info:
            differ.get_row_count("nonexistent_table")

        assert "nonexistent_table" in exc_info.value.table

    def test_quick_check_key_column_not_in_source(self, connector: DuckDBConnector):
        """Test quick_check with key column not in source raises KeyColumnError."""
        connector.execute("CREATE TABLE src (name VARCHAR)")
        connector.execute("CREATE TABLE tgt (id INT, name VARCHAR)")

        differ = DataDiffer(connector)

        with pytest.raises(KeyColumnError) as exc_info:
            differ.quick_check(
                source_table="src",
                target_table="tgt",
                key_column="id",
            )

        assert exc_info.value.key_column == "id"

    def test_quick_check_nonexistent_table(self, connector: DuckDBConnector):
        """Test quick_check with non-existent table raises TableNotFoundError."""
        connector.execute("CREATE TABLE real_table (id INT)")

        differ = DataDiffer(connector)

        with pytest.raises(TableNotFoundError):
            differ.quick_check(
                source_table="real_table",
                target_table="ghost_table",
                key_column="id",
            )

    def test_diff_no_common_columns(self, connector: DuckDBConnector):
        """Test diff with no common columns raises SchemaError."""
        connector.execute("CREATE TABLE disjoint_source (a INT, b VARCHAR)")
        connector.execute("INSERT INTO disjoint_source VALUES (1, 'test')")

        connector.execute("CREATE TABLE disjoint_target (c INT, d VARCHAR)")
        connector.execute("INSERT INTO disjoint_target VALUES (1, 'test')")

        differ = DataDiffer(connector)

        # When tables have no common columns at all, key column validation fails first
        with pytest.raises((KeyColumnError, SchemaError)):
            differ.diff(
                source_table="disjoint_source",
                target_table="disjoint_target",
                key_column="a",
            )
