"""Tests for SnowflakeAdapter."""

import pytest

from quack_diff.core.adapters.base import Dialect
from quack_diff.core.adapters.snowflake import SnowflakeAdapter


class TestSnowflakeAdapterProperties:
    """Tests for SnowflakeAdapter properties."""

    def test_dialect_returns_snowflake(self):
        """Adapter reports Snowflake dialect."""
        adapter = SnowflakeAdapter()
        assert adapter.dialect is Dialect.SNOWFLAKE

    def test_supports_time_travel_is_true(self):
        """Snowflake supports time-travel."""
        adapter = SnowflakeAdapter()
        assert adapter.supports_time_travel is True


class TestSnowflakeAdapterSqlGeneration:
    """Tests for SQL generation methods."""

    @pytest.fixture
    def adapter(self):
        return SnowflakeAdapter()

    def test_cast_to_varchar(self, adapter: SnowflakeAdapter):
        """Cast to VARCHAR produces Snowflake syntax."""
        assert adapter.cast_to_varchar("col") == "CAST(col AS VARCHAR)"

    def test_coalesce_null_default_sentinel(self, adapter: SnowflakeAdapter):
        """Coalesce NULL with default sentinel."""
        assert adapter.coalesce_null("expr") == "COALESCE(expr, '<NULL>')"

    def test_coalesce_null_custom_sentinel(self, adapter: SnowflakeAdapter):
        """Coalesce NULL with custom sentinel."""
        assert adapter.coalesce_null("x", "__NULL__") == "COALESCE(x, '__NULL__')"

    def test_concat_with_separator(self, adapter: SnowflakeAdapter):
        """CONCAT_WS with default separator."""
        result = adapter.concat_with_separator(["a", "b", "c"])
        assert result == "CONCAT_WS('|#|', a, b, c)"

    def test_concat_with_separator_custom(self, adapter: SnowflakeAdapter):
        """CONCAT_WS with custom separator."""
        result = adapter.concat_with_separator(["x", "y"], separator="||")
        assert result == "CONCAT_WS('||', x, y)"

    def test_md5_hash(self, adapter: SnowflakeAdapter):
        """MD5 hash expression."""
        assert adapter.md5_hash("expr") == "MD5(expr)"


class TestSnowflakeTimeTravelClause:
    """Tests for time_travel_clause and related logic."""

    @pytest.fixture
    def adapter(self):
        return SnowflakeAdapter()

    def test_time_travel_with_timestamp(self, adapter: SnowflakeAdapter):
        """Timestamp produces AT(TIMESTAMP => ...)."""
        result = adapter.time_travel_clause(timestamp="2024-01-01 12:00:00")
        assert result == "AT(TIMESTAMP => '2024-01-01 12:00:00'::TIMESTAMP_LTZ)"

    def test_time_travel_with_offset(self, adapter: SnowflakeAdapter):
        """Offset produces AT(OFFSET => -seconds)."""
        result = adapter.time_travel_clause(offset="5 minutes")
        assert result == "AT(OFFSET => -300)"

    def test_time_travel_neither_timestamp_nor_offset_raises(self, adapter: SnowflakeAdapter):
        """Neither timestamp nor offset raises ValueError."""
        with pytest.raises(ValueError, match="Either timestamp or offset must be provided"):
            adapter.time_travel_clause()


class TestParseOffsetToSeconds:
    """Tests for _parse_offset_to_seconds (valid and invalid inputs)."""

    @pytest.fixture
    def adapter(self):
        return SnowflakeAdapter()

    def test_seconds(self, adapter: SnowflakeAdapter):
        assert adapter._parse_offset_to_seconds("30 seconds") == 30

    def test_seconds_plural(self, adapter: SnowflakeAdapter):
        assert adapter._parse_offset_to_seconds("1 seconds") == 1

    def test_minutes(self, adapter: SnowflakeAdapter):
        assert adapter._parse_offset_to_seconds("5 minutes") == 300

    def test_hours(self, adapter: SnowflakeAdapter):
        assert adapter._parse_offset_to_seconds("1 hour") == 3600

    def test_days(self, adapter: SnowflakeAdapter):
        assert adapter._parse_offset_to_seconds("2 days") == 172800

    def test_ago_format_stripped(self, adapter: SnowflakeAdapter):
        """'X ago' format has ' ago' stripped before parsing."""
        assert adapter._parse_offset_to_seconds("5 minutes ago") == 300

    def test_invalid_format_wrong_number_of_parts(self, adapter: SnowflakeAdapter):
        """Single word or too many parts raises ValueError."""
        with pytest.raises(ValueError, match="Invalid offset format"):
            adapter._parse_offset_to_seconds("minutes")
        with pytest.raises(ValueError, match="Invalid offset format"):
            adapter._parse_offset_to_seconds("5 minutes extra")

    def test_invalid_numeric_value(self, adapter: SnowflakeAdapter):
        """Non-numeric first part raises ValueError."""
        with pytest.raises(ValueError, match="Invalid numeric value"):
            adapter._parse_offset_to_seconds("five minutes")

    def test_unknown_time_unit(self, adapter: SnowflakeAdapter):
        """Unknown unit raises ValueError."""
        with pytest.raises(ValueError, match="Unknown time unit"):
            adapter._parse_offset_to_seconds("5 weeks")


class TestWrapTableWithTimeTravel:
    """Tests for wrap_table_with_time_travel."""

    @pytest.fixture
    def adapter(self):
        return SnowflakeAdapter()

    def test_no_timestamp_no_offset_returns_table_unchanged(self, adapter: SnowflakeAdapter):
        """When both are None, table name is returned unchanged."""
        assert adapter.wrap_table_with_time_travel("my_db.my_schema.my_table") == "my_db.my_schema.my_table"

    def test_with_timestamp_wraps_table(self, adapter: SnowflakeAdapter):
        """Table is wrapped with AT(TIMESTAMP => ...)."""
        result = adapter.wrap_table_with_time_travel("t", timestamp="2024-01-01 12:00:00")
        assert result == "t AT(TIMESTAMP => '2024-01-01 12:00:00'::TIMESTAMP_LTZ)"

    def test_with_offset_wraps_table(self, adapter: SnowflakeAdapter):
        """Table is wrapped with AT(OFFSET => ...)."""
        result = adapter.wrap_table_with_time_travel("t", offset="1 hour")
        assert result == "t AT(OFFSET => -3600)"


class TestSnowflakeRowHashExpression:
    """Integration: row_hash_expression uses adapter methods."""

    def test_row_hash_expression_produces_snowflake_sql(self):
        """row_hash_expression yields Snowflake-specific SQL."""
        adapter = SnowflakeAdapter()
        result = adapter.row_hash_expression(columns=["id", "name"])
        assert "CAST(" in result
        assert "VARCHAR" in result
        assert "COALESCE(" in result
        assert "CONCAT_WS(" in result
        assert "MD5(" in result
