"""Tests for SQL utility functions."""

from __future__ import annotations

import pytest

from quack_diff.core.sql_utils import (
    SQLInjectionError,
    build_parameterized_in_clause,
    escape_like_pattern,
    quote_identifier,
    sanitize_identifier,
    sanitize_path,
    validate_limit,
)


class TestSanitizeIdentifier:
    """Tests for sanitize_identifier function."""

    def test_valid_simple_identifier(self):
        """Test valid simple identifiers."""
        assert sanitize_identifier("users") == "users"
        assert sanitize_identifier("my_table") == "my_table"
        assert sanitize_identifier("table123") == "table123"
        assert sanitize_identifier("_private") == "_private"

    def test_valid_qualified_identifier(self):
        """Test valid qualified identifiers (schema.table)."""
        assert sanitize_identifier("schema.table") == "schema.table"
        assert sanitize_identifier("db.schema.table") == "db.schema.table"
        assert sanitize_identifier("my_db.my_schema.my_table") == "my_db.my_schema.my_table"

    def test_valid_with_hyphens(self):
        """Test identifiers with hyphens."""
        assert sanitize_identifier("my-table") == "my-table"
        assert sanitize_identifier("schema-1.table-2") == "schema-1.table-2"

    def test_empty_identifier(self):
        """Test empty identifier raises error."""
        with pytest.raises(ValueError, match="Identifier cannot be empty"):
            sanitize_identifier("")

        with pytest.raises(ValueError, match="Identifier cannot be empty"):
            sanitize_identifier("   ")

    def test_too_long_identifier(self):
        """Test identifier that exceeds max length."""
        long_name = "a" * 256
        with pytest.raises(ValueError, match="Identifier too long"):
            sanitize_identifier(long_name)

    def test_sql_injection_semicolon(self):
        """Test detection of semicolon injection."""
        with pytest.raises(SQLInjectionError, match="SQL injection pattern"):
            sanitize_identifier("users; DROP TABLE students")

    def test_sql_injection_comment(self):
        """Test detection of SQL comment injection."""
        with pytest.raises(SQLInjectionError, match="SQL injection pattern"):
            sanitize_identifier("users-- comment")

        with pytest.raises(SQLInjectionError, match="SQL injection pattern"):
            sanitize_identifier("users/* comment */")

    def test_sql_injection_or(self):
        """Test detection of OR injection."""
        # Spaces make this fail at character validation, which is fine
        with pytest.raises(SQLInjectionError):
            sanitize_identifier("users OR 1=1")

    def test_sql_injection_union(self):
        """Test detection of UNION injection."""
        with pytest.raises(SQLInjectionError, match="SQL injection pattern"):
            sanitize_identifier("users UNION SELECT * FROM passwords")

    def test_sql_injection_drop(self):
        """Test detection of DROP statement."""
        with pytest.raises(SQLInjectionError, match="SQL injection pattern"):
            sanitize_identifier("users DROP TABLE")

    def test_sql_injection_nested_select(self):
        """Test detection of nested SELECT."""
        with pytest.raises(SQLInjectionError, match="SQL injection pattern"):
            sanitize_identifier("(SELECT * FROM users)")

    def test_invalid_characters(self):
        """Test identifiers with invalid characters."""
        with pytest.raises(SQLInjectionError, match="unsafe characters"):
            sanitize_identifier("users!@#")

        with pytest.raises(SQLInjectionError, match="unsafe characters"):
            sanitize_identifier("table$name")

        with pytest.raises(SQLInjectionError, match="unsafe characters"):
            sanitize_identifier("table&name")

    def test_too_many_parts(self):
        """Test identifier with too many dotted parts."""
        with pytest.raises(SQLInjectionError, match="too many parts"):
            sanitize_identifier("a.b.c.d")

    def test_empty_parts(self):
        """Test identifier with empty parts."""
        with pytest.raises(SQLInjectionError, match="empty part"):
            sanitize_identifier("schema..table")

        with pytest.raises(SQLInjectionError, match="empty part"):
            sanitize_identifier(".table")

    def test_invalid_start_character(self):
        """Test identifier starting with invalid character."""
        with pytest.raises(SQLInjectionError, match="must start with letter or underscore"):
            sanitize_identifier("123table")

        with pytest.raises(SQLInjectionError, match="must start with letter or underscore"):
            sanitize_identifier("schema.123table")


class TestQuoteIdentifier:
    """Tests for quote_identifier function."""

    def test_quote_simple_identifier(self):
        """Test quoting simple identifiers."""
        assert quote_identifier("table") == '"table"'
        assert quote_identifier("my_table") == '"my_table"'

    def test_quote_qualified_identifier(self):
        """Test quoting qualified identifiers."""
        assert quote_identifier("schema.table") == '"schema"."table"'
        assert quote_identifier("db.schema.table") == '"db"."schema"."table"'

    def test_quote_removes_existing_quotes(self):
        """Test that existing quotes are handled correctly."""
        assert quote_identifier("`table`") == '"table"'
        assert quote_identifier('"table"') == '"table"'
        assert quote_identifier("'table'") == '"table"'

    def test_quote_invalid_identifier(self):
        """Test that quoting still validates the identifier."""
        with pytest.raises(SQLInjectionError):
            quote_identifier("table; DROP TABLE users")


class TestSanitizePath:
    """Tests for sanitize_path function."""

    def test_valid_unix_path(self):
        """Test valid Unix-style paths."""
        assert sanitize_path("/path/to/database.duckdb") == "/path/to/database.duckdb"
        assert sanitize_path("./relative/path.db") == "./relative/path.db"
        assert sanitize_path("../parent/dir/file.duckdb") == "../parent/dir/file.duckdb"
        assert sanitize_path("/home/user/data.db") == "/home/user/data.db"
        assert sanitize_path("~/data/mydb.duckdb") == "~/data/mydb.duckdb"

    def test_valid_windows_path(self):
        """Test valid Windows-style paths."""
        assert sanitize_path("C:/Users/data.db") == "C:/Users/data.db"
        assert sanitize_path("D:/Program Files/app/db.duckdb") == "D:/Program Files/app/db.duckdb"

    def test_path_with_spaces(self):
        """Test paths with spaces."""
        assert sanitize_path("/path/to/my database.duckdb") == "/path/to/my database.duckdb"

    def test_path_with_special_chars(self):
        """Test paths with parentheses and brackets."""
        assert sanitize_path("/path/to/db (backup).duckdb") == "/path/to/db (backup).duckdb"
        assert sanitize_path("/path/to/db[1].duckdb") == "/path/to/db[1].duckdb"

    def test_empty_path(self):
        """Test empty path raises error."""
        with pytest.raises(ValueError, match="Path cannot be empty"):
            sanitize_path("")

    def test_path_sql_injection_semicolon(self):
        """Test detection of semicolon in path."""
        with pytest.raises(SQLInjectionError, match="SQL injection pattern"):
            sanitize_path("/path/to/db.duckdb; DROP TABLE users")

    def test_path_sql_injection_comment(self):
        """Test detection of SQL comments in path."""
        with pytest.raises(SQLInjectionError, match="SQL injection pattern"):
            sanitize_path("/path/to/db.duckdb-- comment")

        with pytest.raises(SQLInjectionError, match="SQL injection pattern"):
            sanitize_path("/path/to/db.duckdb/* comment */")

    def test_path_invalid_characters(self):
        """Test paths with invalid characters."""
        with pytest.raises(SQLInjectionError, match="unsafe characters"):
            sanitize_path("/path/to/db.duckdb|malicious")

        with pytest.raises(SQLInjectionError, match="unsafe characters"):
            sanitize_path("/path/to/db.duckdb&command")


class TestBuildParameterizedInClause:
    """Tests for build_parameterized_in_clause function."""

    def test_single_value(self):
        """Test IN clause with single value."""
        placeholders, params = build_parameterized_in_clause([1])
        assert placeholders == "?"
        assert params == [1]

    def test_multiple_values(self):
        """Test IN clause with multiple values."""
        placeholders, params = build_parameterized_in_clause([1, 2, 3])
        assert placeholders == "?, ?, ?"
        assert params == [1, 2, 3]

    def test_string_values(self):
        """Test IN clause with string values."""
        placeholders, params = build_parameterized_in_clause(["a", "b", "c"])
        assert placeholders == "?, ?, ?"
        assert params == ["a", "b", "c"]

    def test_empty_list(self):
        """Test that empty list raises error."""
        with pytest.raises(ValueError, match="Cannot create IN clause with empty values list"):
            build_parameterized_in_clause([])


class TestValidateLimit:
    """Tests for validate_limit function."""

    def test_valid_limit(self):
        """Test valid LIMIT values."""
        assert validate_limit(10) == 10
        assert validate_limit(1) == 1
        assert validate_limit(1000) == 1000

    def test_none_limit(self):
        """Test None LIMIT (no limit)."""
        assert validate_limit(None) is None

    def test_zero_limit(self):
        """Test zero LIMIT raises error."""
        with pytest.raises(ValueError, match="LIMIT must be a positive integer"):
            validate_limit(0)

    def test_negative_limit(self):
        """Test negative LIMIT raises error."""
        with pytest.raises(ValueError, match="LIMIT must be a positive integer"):
            validate_limit(-1)

    def test_non_integer_limit(self):
        """Test non-integer LIMIT raises error."""
        with pytest.raises(ValueError, match="LIMIT must be a positive integer"):
            validate_limit(10.5)  # type: ignore


class TestEscapeLikePattern:
    """Tests for escape_like_pattern function."""

    def test_escape_percent(self):
        """Test escaping percent sign."""
        assert escape_like_pattern("test%") == "test\\%"

    def test_escape_underscore(self):
        """Test escaping underscore."""
        assert escape_like_pattern("test_pattern") == "test\\_pattern"

    def test_escape_backslash(self):
        """Test escaping backslash."""
        assert escape_like_pattern("test\\path") == "test\\\\path"

    def test_escape_multiple(self):
        """Test escaping multiple special characters."""
        assert escape_like_pattern("test_%pattern\\") == "test\\_\\%pattern\\\\"

    def test_no_special_characters(self):
        """Test pattern without special characters."""
        assert escape_like_pattern("testpattern") == "testpattern"
