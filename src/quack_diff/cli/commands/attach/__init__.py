"""Attach command for attaching external databases."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from quack_diff.cli.console import console, print_error, print_success
from quack_diff.config import get_settings
from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.sql_utils import AttachError, QueryExecutionError, SQLInjectionError


def attach(
    name: Annotated[
        str,
        typer.Argument(help="Name/alias for the attached database"),
    ],
    path: Annotated[
        str,
        typer.Option(
            "--path",
            "-p",
            help="Path to DuckDB database file",
        ),
    ],
    config_file: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Path to configuration file (YAML)",
        ),
    ] = None,
) -> None:
    """Attach a DuckDB database and list its tables.

    This is a utility command to verify database connectivity
    and explore available tables.

    Note: For Snowflake tables, use the 'compare' command directly with
    sf.SCHEMA.TABLE syntax. Snowflake data is pulled using the native
    connector which supports time-travel queries.

    Example:

        quack-diff attach mydb --path ./data/mydb.duckdb
    """
    try:
        settings = get_settings(config_file=config_file)

        with DuckDBConnector(settings=settings) as connector:
            connector.attach_duckdb(name, path)

            print_success(f"Attached DuckDB database as '{name}'")

            # List tables
            try:
                result = connector.execute_fetchall(f"SHOW TABLES IN {name}")
                if result:
                    console.print("\n[bold]Tables:[/bold]")
                    for row in result:
                        console.print(f"  - {row[0]}")
                else:
                    console.print("\n[muted]No tables found[/muted]")
            except QueryExecutionError as e:
                console.print(f"\n[yellow]Warning: Could not list tables: {e.message}[/yellow]")

    except AttachError as e:
        print_error(f"Failed to attach database: {e.message}")
        if e.details:
            console.print(f"[dim]{e.details}[/dim]")
        raise typer.Exit(2) from None
    except SQLInjectionError as e:
        print_error(f"Invalid input: {e}")
        raise typer.Exit(2) from None
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        raise typer.Exit(2) from None
