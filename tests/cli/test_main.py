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
        assert "diff" in result.output
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
