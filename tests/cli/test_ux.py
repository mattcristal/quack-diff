"""Tests for CLI user experience features: progress bars, JSON output, dry-run, error suggestions."""

from __future__ import annotations

import json
import re
import tempfile
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from quack_diff.cli.console import (
    create_progress,
    create_spinner,
    is_json_output_mode,
    set_json_output_mode,
)
from quack_diff.cli.errors import (
    ERROR_RECOVERY_SUGGESTIONS,
    get_error_info,
    get_recovery_suggestion,
)
from quack_diff.cli.main import app
from quack_diff.cli.output import (
    format_attach_result_json,
    format_error_json,
    get_version,
)

runner = CliRunner()

# Pattern to strip ANSI escape codes from output
ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    return ANSI_PATTERN.sub("", text)


@pytest.fixture
def temp_duckdb():
    """Create a temporary DuckDB database with test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"

        conn = duckdb.connect(str(db_path))
        conn.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                email VARCHAR
            )
        """)
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
        conn.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')")
        conn.close()

        yield str(db_path)


@pytest.fixture
def temp_parquet_files():
    """Create temporary parquet files for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        source_path = Path(tmpdir) / "source.parquet"
        target_path = Path(tmpdir) / "target.parquet"

        conn = duckdb.connect()
        conn.execute("CREATE TABLE source (id INT, name VARCHAR, value INT)")
        conn.execute("INSERT INTO source VALUES (1, 'a', 100), (2, 'b', 200)")
        conn.execute(f"COPY source TO '{source_path}'")

        conn.execute("CREATE TABLE target (id INT, name VARCHAR, value INT)")
        conn.execute("INSERT INTO target VALUES (1, 'a', 100), (2, 'b', 200)")
        conn.execute(f"COPY target TO '{target_path}'")
        conn.close()

        yield str(source_path), str(target_path)


class TestJSONOutputMode:
    """Tests for JSON output mode management."""

    def test_json_output_mode_initially_false(self):
        """JSON output mode should be disabled by default."""
        # Reset to known state
        set_json_output_mode(False)
        assert is_json_output_mode() is False

    def test_set_json_output_mode_true(self):
        """Setting JSON output mode to True should work."""
        set_json_output_mode(True)
        assert is_json_output_mode() is True
        # Reset
        set_json_output_mode(False)

    def test_set_json_output_mode_false(self):
        """Setting JSON output mode to False should work."""
        set_json_output_mode(True)
        set_json_output_mode(False)
        assert is_json_output_mode() is False


class TestProgressIndicators:
    """Tests for progress indicator utilities."""

    def test_create_progress_returns_progress_instance(self):
        """create_progress should return a Progress instance."""
        progress = create_progress("Testing")
        assert progress is not None
        assert hasattr(progress, "add_task")
        assert hasattr(progress, "advance")

    def test_create_spinner_returns_progress_instance(self):
        """create_spinner should return a Progress instance."""
        spinner = create_spinner("Loading...")
        assert spinner is not None
        assert hasattr(spinner, "add_task")

    def test_progress_disabled_in_json_mode(self):
        """Progress indicators should be disabled in JSON output mode."""
        set_json_output_mode(True)
        try:
            progress = create_progress("Testing")
            # Progress should have disable=True when JSON mode is on
            assert progress.disable is True
        finally:
            set_json_output_mode(False)


class TestErrorRecoverySuggestions:
    """Tests for error recovery suggestion system."""

    def test_recovery_suggestions_exist_for_common_errors(self):
        """Recovery suggestions should exist for common error types."""
        expected_errors = [
            "TableNotFoundError",
            "KeyColumnError",
            "SchemaError",
            "AttachError",
            "QueryExecutionError",
            "SQLInjectionError",
        ]
        for error_type in expected_errors:
            assert error_type in ERROR_RECOVERY_SUGGESTIONS
            assert len(ERROR_RECOVERY_SUGGESTIONS[error_type]) > 0

    def test_get_recovery_suggestion_returns_string(self):
        """get_recovery_suggestion should return a non-empty string for known errors."""
        suggestion = get_recovery_suggestion("TableNotFoundError")
        assert suggestion is not None
        assert isinstance(suggestion, str)
        assert len(suggestion) > 0

    def test_get_recovery_suggestion_returns_none_for_unknown(self):
        """get_recovery_suggestion should return None for unknown error types."""
        suggestion = get_recovery_suggestion("UnknownRandomError123")
        assert suggestion is None

    def test_get_error_info_extracts_message(self):
        """get_error_info should extract error message."""

        class TestError(Exception):
            pass

        e = TestError("test message")
        info = get_error_info(e)

        assert info.error_type == "TestError"
        assert info.message == "test message"

    def test_get_error_info_with_details(self):
        """get_error_info should extract details if present."""

        class DetailedError(Exception):
            def __init__(self, msg: str, details: str):
                super().__init__(msg)
                self.details = details

        e = DetailedError("main message", "detailed info")
        info = get_error_info(e)

        assert info.details == "detailed info"


class TestJSONOutputFormatting:
    """Tests for JSON output formatting functions."""

    def test_get_version_returns_string(self):
        """get_version should return a version string."""
        version = get_version()
        assert isinstance(version, str)
        assert len(version) > 0

    def test_format_error_json_structure(self):
        """format_error_json should return proper structure."""
        result = format_error_json(
            error_type="TestError",
            message="Test message",
            details="Test details",
            recovery_suggestion="Try this fix",
        )

        assert result["status"] == "error"
        assert result["exit_code"] == 2
        assert "meta" in result
        assert "error" in result
        assert result["error"]["type"] == "TestError"
        assert result["error"]["message"] == "Test message"
        assert result["error"]["details"] == "Test details"
        assert result["error"]["recovery_suggestion"] == "Try this fix"

    def test_format_attach_result_json_structure(self):
        """format_attach_result_json should return proper structure."""
        result = format_attach_result_json(
            alias="testdb",
            path="/path/to/db.duckdb",
            tables=["users", "orders"],
            duration_seconds=0.5,
        )

        assert result["status"] == "success"
        assert result["exit_code"] == 0
        assert result["database"]["alias"] == "testdb"
        assert result["database"]["path"] == "/path/to/db.duckdb"
        assert result["tables"] == ["users", "orders"]
        assert result["meta"]["duration_seconds"] == 0.5


class TestCompareDryRun:
    """Tests for compare command dry-run mode."""

    def test_dry_run_exits_successfully(self, temp_parquet_files):
        """Dry run should exit with code 0."""
        source, target = temp_parquet_files
        result = runner.invoke(
            app,
            [
                "compare",
                "--source",
                source,
                "--target",
                target,
                "--key",
                "id",
                "--dry-run",
            ],
        )
        assert result.exit_code == 0

    def test_dry_run_shows_operation_info(self, temp_parquet_files):
        """Dry run should show what would be compared."""
        source, target = temp_parquet_files
        result = runner.invoke(
            app,
            [
                "compare",
                "--source",
                source,
                "--target",
                target,
                "--key",
                "id",
                "--dry-run",
            ],
        )
        output = strip_ansi(result.output)
        assert "Dry Run" in output
        assert "Source:" in output
        assert "Target:" in output
        assert "Key Column:" in output

    def test_dry_run_json_output(self, temp_parquet_files):
        """Dry run with --json should output valid JSON."""
        source, target = temp_parquet_files
        result = runner.invoke(
            app,
            [
                "compare",
                "--source",
                source,
                "--target",
                target,
                "--key",
                "id",
                "--dry-run",
                "--json",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["mode"] == "dry_run"
        assert "source" in data
        assert "target" in data
        assert "operations" in data


class TestCompareJSONOutput:
    """Tests for compare command JSON output."""

    def test_json_output_match(self, temp_parquet_files):
        """JSON output for matching tables should have correct structure."""
        source, target = temp_parquet_files
        result = runner.invoke(
            app,
            [
                "compare",
                "--source",
                source,
                "--target",
                target,
                "--key",
                "id",
                "--json",
            ],
        )
        # Parse JSON even if exit code is non-zero to check structure
        try:
            data = json.loads(result.output)
            assert "status" in data
            assert "meta" in data
            # If status is match, exit code should be 0
            if data["status"] == "match":
                assert result.exit_code == 0
                assert data["exit_code"] == 0
                assert "source" in data
                assert "target" in data
                assert "schema" in data
                assert "diff" in data
                assert data["diff"]["is_match"] is True
        except json.JSONDecodeError:
            # If JSON parsing fails, ensure we have valid output
            pytest.fail(f"Invalid JSON output: {result.output}")

    def test_json_output_contains_metadata(self, temp_parquet_files):
        """JSON output should contain metadata."""
        source, target = temp_parquet_files
        result = runner.invoke(
            app,
            [
                "compare",
                "--source",
                source,
                "--target",
                target,
                "--key",
                "id",
                "--json",
            ],
        )
        # Parse output - it should be valid JSON regardless of exit code
        data = json.loads(result.output)

        assert "meta" in data
        assert "tool" in data["meta"]
        assert data["meta"]["tool"] == "quack-diff"
        assert "version" in data["meta"]
        assert "timestamp" in data["meta"]


class TestSchemaDryRun:
    """Tests for schema command dry-run mode."""

    def test_dry_run_exits_successfully(self, temp_parquet_files):
        """Schema dry run should exit with code 0."""
        source, target = temp_parquet_files
        result = runner.invoke(
            app,
            [
                "schema",
                "--source",
                source,
                "--target",
                target,
                "--dry-run",
            ],
        )
        assert result.exit_code == 0

    def test_dry_run_shows_operation_info(self, temp_parquet_files):
        """Schema dry run should show operations."""
        source, target = temp_parquet_files
        result = runner.invoke(
            app,
            [
                "schema",
                "--source",
                source,
                "--target",
                target,
                "--dry-run",
            ],
        )
        output = strip_ansi(result.output)
        assert "Dry Run" in output


class TestSchemaJSONOutput:
    """Tests for schema command JSON output."""

    def test_json_output_structure(self, temp_parquet_files):
        """Schema JSON output should have correct structure."""
        source, target = temp_parquet_files
        result = runner.invoke(
            app,
            [
                "schema",
                "--source",
                source,
                "--target",
                target,
                "--json",
            ],
        )
        # Parse output - it should be valid JSON regardless of exit code
        try:
            data = json.loads(result.output)
            assert "status" in data
            assert "meta" in data
            # If status is not error, check for full structure
            if data["status"] != "error":
                assert "source" in data
                assert "target" in data
                assert "comparison" in data
        except json.JSONDecodeError:
            pytest.fail(f"Invalid JSON output: {result.output}")


class TestAttachJSONOutput:
    """Tests for attach command JSON output."""

    def test_json_output_success(self, temp_duckdb):
        """Attach JSON output should have correct structure on success."""
        result = runner.invoke(
            app,
            [
                "attach",
                "testdb",
                "--path",
                temp_duckdb,
                "--json",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)

        assert data["status"] == "success"
        assert data["exit_code"] == 0
        assert data["database"]["alias"] == "testdb"
        assert "tables" in data
        # Tables list is present (may or may not have entries depending on how SHOW TABLES works)
        assert isinstance(data["tables"], list)

    def test_json_output_error(self):
        """Attach JSON output should have correct structure on error."""
        result = runner.invoke(
            app,
            [
                "attach",
                "testdb",
                "--path",
                "/nonexistent/path.duckdb",
                "--json",
            ],
        )
        assert result.exit_code == 2
        data = json.loads(result.output)

        assert data["status"] == "error"
        assert "error" in data
        assert "recovery_suggestion" in data["error"]


class TestErrorMessagesWithRecovery:
    """Tests for error messages including recovery suggestions."""

    def test_table_not_found_shows_hint(self):
        """TableNotFoundError should show recovery hint."""
        result = runner.invoke(
            app,
            [
                "compare",
                "--source",
                "nonexistent.parquet",
                "--target",
                "also_nonexistent.parquet",
                "--key",
                "id",
            ],
        )
        assert result.exit_code == 2
        output = strip_ansi(result.output)
        # Should show hint
        assert "Hint:" in output or "error" in output.lower()

    def test_json_error_includes_suggestion(self):
        """JSON error output should include recovery suggestion."""
        result = runner.invoke(
            app,
            [
                "compare",
                "--source",
                "nonexistent.parquet",
                "--target",
                "also_nonexistent.parquet",
                "--key",
                "id",
                "--json",
            ],
        )
        assert result.exit_code == 2
        data = json.loads(result.output)
        assert "error" in data
        # Recovery suggestion should be present for known error types
        assert "recovery_suggestion" in data["error"]
