"""CLI commands for quack-diff."""

from quack_diff.cli.commands.attach import attach
from quack_diff.cli.commands.compare import compare
from quack_diff.cli.commands.schema import schema

__all__ = ["attach", "compare", "schema"]
