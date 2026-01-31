"""Tests for the query builder module."""

import pytest

from quack_diff.core.adapters.base import Dialect
from quack_diff.core.query_builder import QueryBuilder


class TestQueryBuilder:
    """Tests for QueryBuilder class."""

    @pytest.fixture
    def builder(self):
        """Create a QueryBuilder instance."""
        return QueryBuilder()

    def test_build_hash_query_duckdb(self, builder: QueryBuilder):
        """Test hash query generation for DuckDB."""
        query = builder.build_hash_query(
            table="my_table",
            columns=["id", "name", "email"],
            key_column="id",
            dialect=Dialect.DUCKDB,
        )

        assert "SELECT" in query
        assert "id" in query
        assert "row_hash" in query
        assert "MD5" in query
        assert "CONCAT_WS" in query
        assert "COALESCE" in query
        assert "my_table" in query

    def test_build_hash_query_snowflake(self, builder: QueryBuilder):
        """Test hash query generation for Snowflake."""
        query = builder.build_hash_query(
            table="my_table",
            columns=["id", "name"],
            key_column="id",
            dialect=Dialect.SNOWFLAKE,
        )

        assert "CAST" in query
        assert "VARCHAR" in query

    def test_build_count_query(self, builder: QueryBuilder):
        """Test count query generation."""
        query = builder.build_count_query(
            table="my_table",
            dialect=Dialect.DUCKDB,
        )

        assert "SELECT COUNT(*)" in query
        assert "my_table" in query

    def test_build_schema_query(self, builder: QueryBuilder):
        """Test schema query generation."""
        query = builder.build_schema_query(
            table="my_table",
            dialect=Dialect.DUCKDB,
        )

        assert "DESCRIBE" in query
        assert "my_table" in query

    def test_build_hash_comparison_query(self, builder: QueryBuilder):
        """Test hash comparison query generation."""
        query = builder.build_hash_comparison_query(
            source_table="source",
            target_table="target",
            columns=["id", "name"],
            key_column="id",
            dialect=Dialect.DUCKDB,
        )

        assert "source_hashes" in query
        assert "target_hashes" in query
        assert "FULL OUTER JOIN" in query
        assert "diff_type" in query
        assert "'added'" in query
        assert "'removed'" in query
        assert "'modified'" in query

    def test_null_sentinel_in_query(self, builder: QueryBuilder):
        """Test that NULL sentinel is included in queries."""
        query = builder.build_hash_query(
            table="my_table",
            columns=["name"],
            key_column="id",
            dialect=Dialect.DUCKDB,
        )

        assert builder.null_sentinel in query

    def test_custom_null_sentinel(self):
        """Test custom NULL sentinel."""
        builder = QueryBuilder(null_sentinel="__NULL__")
        query = builder.build_hash_query(
            table="my_table",
            columns=["name"],
            key_column="id",
            dialect=Dialect.DUCKDB,
        )

        assert "__NULL__" in query

    def test_custom_delimiter(self):
        """Test custom column delimiter."""
        builder = QueryBuilder(column_delimiter="|||")
        query = builder.build_hash_query(
            table="my_table",
            columns=["name", "email"],
            key_column="id",
            dialect=Dialect.DUCKDB,
        )

        assert "|||" in query


class TestSnowflakeTimeTravel:
    """Tests for Snowflake time-travel query generation."""

    @pytest.fixture
    def builder(self):
        """Create a QueryBuilder instance."""
        return QueryBuilder()

    def test_time_travel_with_offset(self, builder: QueryBuilder):
        """Test time-travel query with offset."""
        query = builder.build_hash_query(
            table="my_table",
            columns=["id", "name"],
            key_column="id",
            dialect=Dialect.SNOWFLAKE,
            offset="5 minutes ago",
        )

        assert "AT(OFFSET" in query
        assert "-300" in query  # 5 minutes = 300 seconds

    def test_time_travel_with_timestamp(self, builder: QueryBuilder):
        """Test time-travel query with timestamp."""
        query = builder.build_hash_query(
            table="my_table",
            columns=["id", "name"],
            key_column="id",
            dialect=Dialect.SNOWFLAKE,
            timestamp="2024-01-01 12:00:00",
        )

        assert "AT(TIMESTAMP" in query
        assert "2024-01-01 12:00:00" in query
