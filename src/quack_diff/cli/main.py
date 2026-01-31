"""Typer CLI entrypoint for quack-diff."""

from __future__ import annotations

import typer

from quack_diff import __version__
from quack_diff.cli.commands import attach, diff, schema
from quack_diff.cli.console import console

app = typer.Typer(
    name="quack-diff",
    help="The zero-dependency regression testing tool for modern data warehouses.",
    add_completion=False,
    no_args_is_help=True,
)

# Register commands
app.command()(diff)
app.command()(schema)
app.command()(attach)


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"quack-diff version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    """quack-diff: DuckDB-powered data diffing."""
    pass


if __name__ == "__main__":
    app()
