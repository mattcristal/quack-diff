"""Tests for the count command: table-spec parsing, group-by, and direct execution."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from quack_diff.cli.commands.count import (
    TableSpec,
    _build_count_query,
    _parse_table_spec,
    _split_table_arg,
)
from quack_diff.cli.main import app
from quack_diff.config import Settings

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**db_overrides: dict) -> Settings:
    """Return a minimal Settings with optional database aliases."""
    return Settings(databases=db_overrides)


# ---------------------------------------------------------------------------
# _split_table_arg
# ---------------------------------------------------------------------------


class TestSplitTableArg:
    """Tests for comma splitting that respects bracket syntax."""

    def test_simple_comma_separated(self):
        assert _split_table_arg("sf.A,sf.B") == ["sf.A", "sf.B"]

    def test_no_comma(self):
        assert _split_table_arg("sf.A") == ["sf.A"]

    def test_bracket_not_split(self):
        result = _split_table_arg("sf.A,sf.B[x,y]")
        assert result == ["sf.A", "sf.B[x,y]"]

    def test_multiple_brackets(self):
        result = _split_table_arg("sf.A[a,b],sf.B[c,d]")
        assert result == ["sf.A[a,b]", "sf.B[c,d]"]

    def test_whitespace_stripped(self):
        result = _split_table_arg(" sf.A , sf.B ")
        assert result == ["sf.A", "sf.B"]

    def test_empty_string(self):
        assert _split_table_arg("") == []


# ---------------------------------------------------------------------------
# _parse_table_spec
# ---------------------------------------------------------------------------


class TestParseTableSpec:
    """Tests for parsing -t values into TableSpec."""

    def test_plain_snowflake_table(self):
        settings = _make_settings()
        spec = _parse_table_spec("sf.DB.SCHEMA.TABLE", settings)
        assert spec.alias == "sf"
        assert spec.table == "DB.SCHEMA.TABLE"
        assert spec.group_by is None
        assert spec.is_snowflake is True

    def test_snowflake_with_group_by(self):
        settings = _make_settings()
        spec = _parse_table_spec("sf.DB.SCHEMA.TABLE[col1,col2,col3]", settings)
        assert spec.alias == "sf"
        assert spec.table == "DB.SCHEMA.TABLE"
        assert spec.group_by == ["col1", "col2", "col3"]
        assert spec.is_snowflake is True

    def test_configured_alias(self):
        settings = _make_settings(mydb={"type": "snowflake"})
        spec = _parse_table_spec("mydb.SCHEMA.TABLE", settings)
        assert spec.alias == "mydb"
        assert spec.is_snowflake is True

    def test_duckdb_alias(self):
        settings = _make_settings(loc={"type": "duckdb", "path": "/tmp/x.duckdb"})
        spec = _parse_table_spec("loc.main.t1", settings)
        assert spec.alias == "loc"
        assert spec.is_snowflake is False
        assert spec.group_by is None

    def test_no_alias(self):
        settings = _make_settings()
        spec = _parse_table_spec("my_table", settings)
        assert spec.alias is None
        assert spec.table == "my_table"
        assert spec.is_snowflake is False

    def test_group_by_whitespace_stripped(self):
        settings = _make_settings()
        spec = _parse_table_spec("sf.T[ a , b ]", settings)
        assert spec.group_by == ["a", "b"]

    def test_empty_bracket_raises(self):
        settings = _make_settings()
        with pytest.raises(ValueError, match="Empty group-by column list"):
            _parse_table_spec("sf.T[]", settings)


# ---------------------------------------------------------------------------
# _build_count_query
# ---------------------------------------------------------------------------


class TestBuildCountQuery:
    """Tests for SQL count query generation."""

    def _spec(self, table: str, group_by: list[str] | None = None) -> TableSpec:
        return TableSpec(
            raw=f"sf.{table}",
            alias="sf",
            table=table,
            group_by=group_by,
            is_snowflake=True,
        )

    def test_plain_count(self):
        q = _build_count_query(self._spec("SCHEMA.TABLE"))
        assert q == "SELECT COUNT(*) FROM SCHEMA.TABLE"

    def test_count_with_key(self):
        q = _build_count_query(self._spec("SCHEMA.TABLE"), key_column="id")
        assert "COUNT(DISTINCT id)" in q
        assert "SCHEMA.TABLE" in q

    def test_count_with_group_by(self):
        q = _build_count_query(self._spec("SCHEMA.TABLE", group_by=["a", "b"]))
        assert "GROUP BY a, b" in q
        assert "SELECT COUNT(*) FROM" in q

    def test_group_by_and_key_conflict(self):
        with pytest.raises(ValueError, match="Cannot combine"):
            _build_count_query(
                self._spec("T", group_by=["a"]),
                key_column="id",
            )


# ---------------------------------------------------------------------------
# CLI integration: local DuckDB group-by
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_group_by_db():
    """DuckDB file with a table having duplicate groups for group-by testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        db_path = tmpdir_path / "gb.duckdb"
        config_path = tmpdir_path / "quack-diff.yaml"

        conn = duckdb.connect(str(db_path))
        # t_plain: 4 rows
        conn.execute("CREATE TABLE t_plain (id INT, cat VARCHAR)")
        conn.execute("INSERT INTO t_plain VALUES (1,'a'),(2,'b'),(3,'a'),(4,'b')")
        # t_dup: 4 rows but only 2 distinct (cat) groups
        conn.execute("CREATE TABLE t_dup (id INT, cat VARCHAR, sub VARCHAR)")
        conn.execute("INSERT INTO t_dup VALUES (1,'a','x'),(2,'a','y'),(3,'b','x'),(4,'b','y')")
        conn.close()

        config_path.write_text(
            f"databases:\n  gb:\n    type: duckdb\n    path: {db_path}\n",
            encoding="utf-8",
        )
        yield str(config_path)


class TestCountGroupByLocal:
    """End-to-end tests for group-by with local DuckDB tables."""

    def test_group_by_counts_distinct_groups(self, temp_group_by_db):
        """Counting t_dup[cat] should give 2, while t_plain has 4 rows -> mismatch."""
        result = runner.invoke(
            app,
            [
                "count",
                "-t",
                "gb.t_plain",
                "-t",
                "gb.t_dup[cat]",
                "--config",
                temp_group_by_db,
            ],
        )
        # 4 != 2, so should be exit 1 (mismatch)
        assert result.exit_code == 1

    def test_group_by_match(self, temp_group_by_db):
        """Two tables with same group-by count should match."""
        # t_dup[cat] = 2 groups; t_dup[sub] = 2 groups
        result = runner.invoke(
            app,
            [
                "count",
                "-t",
                "gb.t_dup[cat]",
                "-t",
                "gb.t_dup[sub]",
                "--config",
                temp_group_by_db,
            ],
        )
        assert result.exit_code == 0
        assert "match" in result.output.lower()

    def test_group_by_json(self, temp_group_by_db):
        """JSON output works with group-by specs."""
        result = runner.invoke(
            app,
            [
                "count",
                "-t",
                "gb.t_dup[cat]",
                "-t",
                "gb.t_dup[sub]",
                "--json",
                "--config",
                temp_group_by_db,
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["status"] == "match"

    def test_group_by_and_key_conflict_cli(self, temp_group_by_db):
        """--key combined with [group_by] should error."""
        result = runner.invoke(
            app,
            [
                "count",
                "-t",
                "gb.t_dup[cat]",
                "-t",
                "gb.t_plain",
                "--key",
                "id",
                "--config",
                temp_group_by_db,
            ],
        )
        assert result.exit_code == 2


class TestCountLegacyPath:
    """Ensure the legacy (no group-by, no Snowflake) path still works."""

    def test_plain_count_match(self, temp_duckdb_count_tables):
        config_path, table_refs = temp_duckdb_count_tables
        result = runner.invoke(
            app,
            ["count", "-t", table_refs[0], "-t", table_refs[1], "--config", config_path],
        )
        assert result.exit_code == 0
        assert "match" in result.output.lower()

    def test_plain_count_key(self, temp_duckdb_count_tables):
        config_path, table_refs = temp_duckdb_count_tables
        result = runner.invoke(
            app,
            [
                "count",
                "-t",
                table_refs[0],
                "-t",
                table_refs[1],
                "--key",
                "id",
                "--config",
                config_path,
            ],
        )
        assert result.exit_code == 0
