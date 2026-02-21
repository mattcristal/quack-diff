"""Tests for quack_diff.cli.main module."""

from __future__ import annotations

import re

from typer.testing import CliRunner

from quack_diff import __version__
from quack_diff.cli.main import app

runner = CliRunner()

# Pattern to strip ANSI escape codes from output
ANSI_PATTERN = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text."""
    return ANSI_PATTERN.sub("", text)


class TestCLIHelp:
    """Test CLI help functionality."""

    def test_help_exits_successfully(self):
        """Test that --help exits with code 0."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    def test_help_shows_app_name(self):
        """Test that --help output contains the app name."""
        result = runner.invoke(app, ["--help"])
        assert "quack-diff" in result.output

    def test_help_shows_description(self):
        """Test that --help output contains the app description."""
        result = runner.invoke(app, ["--help"])
        assert "regression testing" in result.output.lower()

    def test_help_shows_available_commands(self):
        """Test that --help lists available commands."""
        result = runner.invoke(app, ["--help"])
        assert "compare" in result.output
        assert "count" in result.output
        assert "schema" in result.output
        assert "attach" in result.output

    def test_help_shows_version_option(self):
        """Test that --help mentions the version option."""
        result = runner.invoke(app, ["--help"])
        # Strip ANSI codes as Rich may insert them between characters
        assert "--version" in strip_ansi(result.output)


class TestCLIVersion:
    """Test CLI version functionality."""

    def test_version_exits_successfully(self):
        """Test that --version exits with code 0."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0

    def test_version_shows_version_number(self):
        """Test that --version shows the correct version."""
        result = runner.invoke(app, ["--version"])
        assert __version__ in result.output

    def test_short_version_flag(self):
        """Test that -v also shows version."""
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert __version__ in result.output


class TestCLINoArgs:
    """Test CLI behavior with no arguments."""

    def test_no_args_shows_help(self):
        """Test that running without arguments shows help."""
        result = runner.invoke(app, [])
        # App is configured with no_args_is_help=True
        # Typer/Click exits with code 2 when showing help due to missing args
        assert result.exit_code == 2
        assert "quack-diff" in result.output


class TestCountCommand:
    """Test count command."""

    def test_count_help(self):
        """Test count --help shows options."""
        result = runner.invoke(app, ["count", "--help"])
        assert result.exit_code == 0
        assert "count" in result.output.lower()
        assert "--tables" in result.output or "-t" in result.output
        assert "--key" in result.output or "-k" in result.output
        assert "COUNT" in result.output or "count" in result.output

    def test_count_requires_at_least_two_tables(self):
        """Test count with one table fails with clear message."""
        result = runner.invoke(app, ["count", "-t", "some_table"])
        assert result.exit_code == 2
        assert "two tables" in result.output.lower() or "At least two" in result.output

    def test_count_with_two_tables_match(self, temp_duckdb_count_tables):
        """Test count with two tables (same count) exits 0."""
        config_path, table_refs = temp_duckdb_count_tables
        result = runner.invoke(
            app,
            ["count", "-t", table_refs[0], "-t", table_refs[1], "--config", config_path],
        )
        assert result.exit_code == 0
        assert "match" in result.output.lower() or "Match" in result.output

    def test_count_with_key_distinct(self, temp_duckdb_count_tables):
        """Test count --key with two matching tables."""
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
        assert "match" in result.output.lower() or "Match" in result.output

    def test_count_json_output(self, temp_duckdb_count_tables):
        """Test count --json produces valid JSON with expected structure."""
        import json

        config_path, table_refs = temp_duckdb_count_tables
        result = runner.invoke(
            app,
            [
                "count",
                "-t",
                table_refs[0],
                "-t",
                table_refs[1],
                "--json",
                "--config",
                config_path,
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["status"] == "match"
        assert data["exit_code"] == 0
        assert "tables" in data
        assert len(data["tables"]) == 2
        assert data["is_match"] is True
        assert "meta" in data
        assert data["mode"] in ("rows", "distinct")
