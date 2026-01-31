"""Attach command for attaching external databases."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from quack_diff.cli.console import console, print_error, print_success
from quack_diff.config import get_settings
from quack_diff.core.adapters.base import Dialect
from quack_diff.core.connector import DuckDBConnector


def attach(
    name: Annotated[
        str,
        typer.Argument(help="Name/alias for the attached database"),
    ],
    db_type: Annotated[
        str,
        typer.Option(
            "--type",
            "-t",
            help="Database type (snowflake, duckdb)",
        ),
    ] = "duckdb",
    path: Annotated[
        str | None,
        typer.Option(
            "--path",
            "-p",
            help="Path to database file (for DuckDB)",
        ),
    ] = None,
    config_file: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Path to configuration file (YAML)",
        ),
    ] = None,
) -> None:
    """Attach an external database and list its tables.

    This is a utility command to verify database connectivity
    and explore available tables.

    Example:

        quack-diff attach mydb --type duckdb --path ./data/mydb.duckdb
    """
    try:
        settings = get_settings(config_file=config_file)

        with DuckDBConnector(settings=settings) as connector:
            dialect = Dialect(db_type.lower())

            if dialect == Dialect.DUCKDB:
                if not path:
                    print_error("--path is required for DuckDB databases")
                    raise typer.Exit(2)
                connector.attach_duckdb(name, path)

            elif dialect == Dialect.SNOWFLAKE:
                connector.attach_snowflake(name)

            else:
                print_error(f"Unsupported database type: {db_type}")
                raise typer.Exit(2)

            print_success(f"Attached {db_type} database as '{name}'")

            # List tables
            result = connector.execute_fetchall(f"SHOW TABLES IN {name}")
            if result:
                console.print("\n[bold]Tables:[/bold]")
                for row in result:
                    console.print(f"  - {row[0]}")
            else:
                console.print("\n[muted]No tables found[/muted]")

    except Exception as e:
        print_error(f"Error: {e}")
        raise typer.Exit(2) from None
