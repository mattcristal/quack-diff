"""Schema command for comparing schemas of two tables."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Annotated

import typer

from quack_diff.cli.console import (
    console,
    print_error,
    set_json_output_mode,
    status_context,
)
from quack_diff.cli.errors import get_error_info
from quack_diff.cli.formatters import print_schema_result
from quack_diff.cli.output import (
    format_error_json,
    format_schema_result_json,
    print_json,
)
from quack_diff.config import get_settings
from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer
from quack_diff.core.sql_utils import (
    DatabaseError,
    QueryExecutionError,
    SchemaError,
    SQLInjectionError,
    TableNotFoundError,
)


def schema(
    source: Annotated[
        str,
        typer.Option(
            "--source",
            "-s",
            help="Source table",
        ),
    ],
    target: Annotated[
        str,
        typer.Option(
            "--target",
            "-t",
            help="Target table",
        ),
    ],
    config_file: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Path to configuration file (YAML)",
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be compared without executing",
        ),
    ] = False,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Output results as JSON for CI/CD integration",
        ),
    ] = False,
) -> None:
    """Compare schemas of two tables.

    Shows column names, types, and identifies mismatches between
    source and target table structures.

    Examples:

        quack-diff schema --source prod.users --target dev.users

        # Dry run to see what would be compared

        quack-diff schema --source prod.users --target dev.users --dry-run

        # JSON output for CI/CD pipelines

        quack-diff schema --source prod.users --target dev.users --json
    """
    # Enable JSON output mode if requested
    if json_output:
        set_json_output_mode(True)

    start_time = time.time()

    try:
        # Handle dry-run mode
        if dry_run:
            _print_dry_run_info(source, target, json_output)
            raise typer.Exit(0)

        settings = get_settings(config_file=config_file)

        with DuckDBConnector(settings=settings) as connector:
            differ = DataDiffer(connector=connector)

            with status_context("Comparing schemas..."):
                result = differ.compare_schemas(
                    source_table=source,
                    target_table=target,
                )

            duration = time.time() - start_time

            # Output results based on format
            if json_output:
                json_data = format_schema_result_json(
                    result,
                    source_table=source,
                    target_table=target,
                    duration_seconds=duration,
                )
                print_json(json_data)
            else:
                print_schema_result(result)

            if result.is_identical or result.is_compatible:
                raise typer.Exit(0)
            else:
                raise typer.Exit(1)

    except typer.Exit:
        raise
    except TableNotFoundError as e:
        _handle_error(e, "Table not found", json_output, start_time)
    except SchemaError as e:
        _handle_error(e, "Schema error", json_output, start_time)
    except QueryExecutionError as e:
        _handle_error(e, "Query execution error", json_output, start_time)
    except SQLInjectionError as e:
        _handle_error(e, "Invalid input", json_output, start_time)
    except DatabaseError as e:
        _handle_error(e, "Database error", json_output, start_time)
    except Exception as e:
        _handle_error(e, "Unexpected error", json_output, start_time)


def _print_dry_run_info(source: str, target: str, json_output: bool) -> None:
    """Print dry-run information for schema comparison.

    Args:
        source: Source table name
        target: Target table name
        json_output: Whether to output as JSON
    """
    dry_run_info = {
        "mode": "dry_run",
        "source": {"table": source},
        "target": {"table": target},
        "operations": [
            "Retrieve schema from source table",
            "Retrieve schema from target table",
            "Compare column names and types",
            "Identify matching, missing, and mismatched columns",
        ],
    }

    if json_output:
        print_json(dry_run_info)
    else:
        console.print("\n[header]Dry Run Mode[/header]")
        console.print("The following operations would be performed:\n")

        console.print(f"[key]Source:[/key] {source}")
        console.print(f"[key]Target:[/key] {target}")

        console.print("\n[header]Operations:[/header]")
        for i, op in enumerate(dry_run_info["operations"], 1):
            console.print(f"  {i}. {op}")

        console.print()


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
