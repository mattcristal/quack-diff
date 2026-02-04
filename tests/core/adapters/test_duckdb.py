"""Tests for DuckDBAdapter."""

import pytest

from quack_diff.core.adapters.base import Dialect
from quack_diff.core.adapters.duckdb import DuckDBAdapter


class TestDuckDBAdapterProperties:
    """Tests for DuckDBAdapter properties."""

    def test_dialect_returns_duckdb(self):
        """Adapter reports DuckDB dialect."""
        adapter = DuckDBAdapter()
        assert adapter.dialect is Dialect.DUCKDB

    def test_supports_time_travel_is_false(self):
        """DuckDB does not support time-travel."""
        adapter = DuckDBAdapter()
        assert adapter.supports_time_travel is False


class TestDuckDBAdapterSqlGeneration:
    """Tests for SQL generation methods."""

    @pytest.fixture
    def adapter(self):
        return DuckDBAdapter()

    def test_cast_to_varchar(self, adapter: DuckDBAdapter):
        """Cast to VARCHAR produces DuckDB syntax."""
        assert adapter.cast_to_varchar("col") == "CAST(col AS VARCHAR)"

    def test_coalesce_null_default_sentinel(self, adapter: DuckDBAdapter):
        """Coalesce NULL with default sentinel."""
        assert adapter.coalesce_null("expr") == "COALESCE(expr, '<NULL>')"

    def test_coalesce_null_custom_sentinel(self, adapter: DuckDBAdapter):
        """Coalesce NULL with custom sentinel."""
        assert adapter.coalesce_null("x", "__NULL__") == "COALESCE(x, '__NULL__')"

    def test_concat_with_separator(self, adapter: DuckDBAdapter):
        """CONCAT_WS with default separator."""
        result = adapter.concat_with_separator(["a", "b", "c"])
        assert result == "CONCAT_WS('|#|', a, b, c)"

    def test_concat_with_separator_custom(self, adapter: DuckDBAdapter):
        """CONCAT_WS with custom separator."""
        result = adapter.concat_with_separator(["x", "y"], separator="||")
        assert result == "CONCAT_WS('||', x, y)"

    def test_md5_hash(self, adapter: DuckDBAdapter):
        """MD5 hash expression."""
        assert adapter.md5_hash("expr") == "MD5(expr)"


class TestDuckDBRowHashExpression:
    """Integration: row_hash_expression uses adapter methods."""

    def test_row_hash_expression_produces_duckdb_sql(self):
        """row_hash_expression yields DuckDB-style SQL."""
        adapter = DuckDBAdapter()
        result = adapter.row_hash_expression(columns=["id", "name"])
        assert "CAST(" in result
        assert "VARCHAR" in result
        assert "COALESCE(" in result
        assert "CONCAT_WS(" in result
        assert "MD5(" in result
