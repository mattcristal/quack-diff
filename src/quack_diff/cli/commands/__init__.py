"""CLI commands for quack-diff."""

from quack_diff.cli.commands.attach import attach
from quack_diff.cli.commands.diff import diff
from quack_diff.cli.commands.schema import schema

__all__ = ["attach", "diff", "schema"]
