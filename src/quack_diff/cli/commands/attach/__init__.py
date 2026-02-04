"""Attach command for attaching external databases."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated

import typer

from quack_diff.cli.console import (
    console,
    print_error,
    print_success,
    set_json_output_mode,
    status_context,
)
from quack_diff.cli.errors import get_error_info
from quack_diff.cli.output import (
    format_attach_result_json,
    format_error_json,
    print_json,
)
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
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output results as JSON for CI/CD integration",
        ),
    ] = False,
) -> None:
    """Attach a DuckDB database and list its tables.

    This is a utility command to verify database connectivity
    and explore available tables.

    Note: For Snowflake tables, use the 'compare' command directly with
    sf.SCHEMA.TABLE syntax. Snowflake data is pulled using the native
    connector which supports time-travel queries.

    Examples:

        quack-diff attach mydb --path ./data/mydb.duckdb

        # JSON output for CI/CD pipelines

        quack-diff attach mydb --path ./data/mydb.duckdb --json
    """
    # Enable JSON output mode if requested
    if json_output:
        set_json_output_mode(True)

    start_time = time.time()

    try:
        settings = get_settings(config_file=config_file)

        with DuckDBConnector(settings=settings) as connector:
            with status_context(f"Attaching database '{name}'..."):
                connector.attach_duckdb(name, path)

            # List tables
            tables: list[str] = []
            try:
                result = connector.execute_fetchall(f"SHOW TABLES IN {name}")
                tables = [row[0] for row in result] if result else []
            except QueryExecutionError as e:
                if not json_output:
                    console.print(f"\n[yellow]Warning: Could not list tables: {e.message}[/yellow]")

            duration = time.time() - start_time

            # Output results based on format
            if json_output:
                json_data = format_attach_result_json(
                    alias=name,
                    path=path,
                    tables=tables,
                    duration_seconds=duration,
                )
                print_json(json_data)
            else:
                print_success(f"Attached DuckDB database as '{name}'")

                if tables:
                    console.print("\n[bold]Tables:[/bold]")
                    for table in tables:
                        console.print(f"  - {table}")
                else:
                    console.print("\n[muted]No tables found[/muted]")

            raise typer.Exit(0)

    except typer.Exit:
        raise
    except AttachError as e:
        _handle_error(e, "Failed to attach database", json_output, start_time)
    except SQLInjectionError as e:
        _handle_error(e, "Invalid input", json_output, start_time)
    except Exception as e:
        _handle_error(e, "Unexpected error", json_output, start_time)


def _handle_error(
    e: Exception,
    prefix: str,
    json_output: bool,
    start_time: float,
) -> None:
    """Handle an error with appropriate output format.

    Args:
        e: The exception
        prefix: Error message prefix
        json_output: Whether JSON output is enabled
        start_time: Start time for duration calculation
    """
    duration = time.time() - start_time
    error_info = get_error_info(e)

    if json_output:
        json_data = format_error_json(
            error_type=error_info.error_type,
            message=f"{prefix}: {error_info.message}",
            details=error_info.details,
            recovery_suggestion=error_info.recovery_suggestion,
        )
        json_data["meta"]["duration_seconds"] = duration
        print_json(json_data)
    else:
        print_error(f"{prefix}: {error_info.message}")
        if error_info.details:
            console.print(f"[dim]{error_info.details}[/dim]")
        if error_info.recovery_suggestion:
            console.print(f"\n[info]Hint: {error_info.recovery_suggestion}[/info]")

    raise typer.Exit(2) from None
