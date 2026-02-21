"""Count command for validating row counts across multiple tables."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

from quack_diff.cli.console import (
    console,
    print_error,
    print_success,
    set_json_output_mode,
    status_context,
)
from quack_diff.cli.errors import get_error_info
from quack_diff.cli.formatters import print_count_result
from quack_diff.cli.output import format_count_result_json, format_error_json, print_json
from quack_diff.config import get_settings
from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer
from quack_diff.core.sql_utils import (
    AttachError,
    DatabaseError,
    QueryExecutionError,
    SQLInjectionError,
    TableNotFoundError,
)

if TYPE_CHECKING:
    from quack_diff.config import Settings

logger = logging.getLogger(__name__)


def _parse_table_reference(table: str, known_aliases: set[str] | None = None) -> tuple[str | None, str]:
    """Parse a table reference to extract alias and table name.

    If known_aliases is provided (e.g. from settings.databases), any first
    segment in that set is treated as an alias.
    """
    parts = table.split(".", 1)
    if len(parts) == 2 and parts[0].lower() in ("sf", "snowflake"):
        return parts[0].lower(), parts[1]
    if len(parts) >= 2:
        first_part = parts[0].lower()
        if known_aliases and first_part in known_aliases:
            return first_part, ".".join(parts[1:])
        if len(first_part) <= 4 and first_part.isalpha():
            return first_part, ".".join(parts[1:])
    return None, table


def _is_snowflake_table(table: str, settings: Settings) -> bool:
    """Check if a table reference points to a Snowflake table."""
    known_aliases = set(settings.databases.keys()) if settings.databases else set()
    alias, _ = _parse_table_reference(table, known_aliases=known_aliases)
    if alias in ("sf", "snowflake"):
        return True
    if alias and alias in settings.databases:
        db_config = settings.databases[alias]
        return db_config.get("type", "snowflake").lower() == "snowflake"
    return False


def _auto_attach_databases(
    connector: DuckDBConnector,
    settings: Settings,
    tables: list[str],
    verbose: bool = False,
) -> None:
    """Auto-attach DuckDB databases for any aliases in table references."""
    known_aliases = set(settings.databases.keys()) if settings.databases else set()
    aliases_to_attach: set[str] = set()
    for table in tables:
        alias, _ = _parse_table_reference(table, known_aliases=known_aliases)
        if alias:
            aliases_to_attach.add(alias)

    for alias in aliases_to_attach:
        if alias in connector.attached_databases:
            continue
        if alias in settings.databases:
            db_config = settings.databases[alias]
            db_type = db_config.get("type", "duckdb").lower()
            if db_type == "duckdb":
                path = db_config.get("path")
                if path:
                    if verbose:
                        from quack_diff.cli.console import print_info

                        print_info(f"Attaching DuckDB database: {path} as '{alias}'")
                    connector.attach_duckdb(alias, str(path))


def _resolve_tables_for_count(
    connector: DuckDBConnector,
    settings: Settings,
    tables: list[str],
    verbose: bool = False,
) -> tuple[list[str], dict[str, str]]:
    """Resolve table list: pull Snowflake tables locally, return (resolved_names, display_name_map)."""
    resolved: list[str] = []
    display_name_map: dict[str, str] = {}

    known_aliases = set(settings.databases.keys()) if settings.databases else set()
    for i, table in enumerate(tables):
        alias, table_name = _parse_table_reference(table, known_aliases=known_aliases)
        if alias and _is_snowflake_table(table, settings):
            local_name = f"__count_pulled_{i}"
            if verbose:
                from quack_diff.cli.console import print_info

                print_info(f"Pulling Snowflake table: {table_name}")

            config = None
            database = None
            if alias in settings.databases:
                db_config = settings.databases[alias]
                connection_name = db_config.get("connection_name")
                database = db_config.get("database")
                if connection_name:
                    from quack_diff.config import SnowflakeConfig

                    config = SnowflakeConfig(connection_name=connection_name)
            if config is None:
                config = settings.snowflake

            connector.pull_snowflake_table(
                table_name=table_name,
                local_name=local_name,
                config=config,
                database=database,
            )
            resolved.append(local_name)
            display_name_map[local_name] = table
        else:
            resolved.append(table)
            display_name_map[table] = table

    return resolved, display_name_map


def count(
    tables: Annotated[
        list[str],
        typer.Option(
            "--tables",
            "-t",
            help="Table(s) to compare counts (repeat or comma-separated)",
        ),
    ],
    key: Annotated[
        str | None,
        typer.Option(
            "--key",
            "-k",
            help="Column for COUNT(DISTINCT ...); omit for COUNT(*)",
        ),
    ] = None,
    config_file: Annotated[
        Path | None,
        typer.Option(
            "--config",
            help="Path to configuration file (YAML)",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose",
            help="Show detailed output",
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
    """Check that multiple tables have the same row or distinct-key count.

    Use this to validate bronze/silver/gold (or any pipeline) layers
    have the same number of records without running a full diff.

    Examples:

        # Same row count across layers
        quack-diff count -t bronze.orders -t silver.orders -t gold.orders

        # Same distinct ID count
        quack-diff count -t bronze.orders -t silver.orders -t gold.orders --key order_id

        # Comma-separated tables
        quack-diff count --tables bronze.orders,silver.orders,gold.orders --key id

        # JSON for CI/CD
        quack-diff count -t bronze.orders -t silver.orders -t gold.orders --key id --json
    """
    if json_output:
        set_json_output_mode(True)

    start_time = time.time()

    # Flatten: support both -t a -t b and -t "a,b"
    flat_tables: list[str] = []
    for t in tables:
        flat_tables.extend(s.strip() for s in t.split(",") if s.strip())

    if len(flat_tables) < 2:
        if json_output:
            print_json(
                format_error_json(
                    error_type="ValueError",
                    message="At least two tables are required for count check",
                    exit_code=2,
                )
            )
        else:
            print_error("At least two tables are required for count check")
        raise typer.Exit(2)

    try:
        settings = get_settings(config_file=config_file)
        use_snowflake = any(_is_snowflake_table(t, settings) for t in flat_tables)

        with DuckDBConnector(settings=settings) as connector:
            if use_snowflake:
                with status_context("Pulling data from Snowflake..."):
                    resolved_tables, display_name_map = _resolve_tables_for_count(
                        connector=connector,
                        settings=settings,
                        tables=flat_tables,
                        verbose=verbose,
                    )
            else:
                _auto_attach_databases(connector, settings, flat_tables, verbose)
                resolved_tables = flat_tables
                display_name_map = {t: t for t in flat_tables}

            differ = DataDiffer(
                connector=connector,
                null_sentinel=settings.defaults.null_sentinel,
                column_delimiter=settings.defaults.column_delimiter,
            )

            with status_context("Counting..."):
                result = differ.count_check(
                    tables=resolved_tables,
                    key_column=key,
                )

        duration = time.time() - start_time

        if json_output:
            json_data = format_count_result_json(
                result,
                display_name_map=display_name_map,
                duration_seconds=duration,
            )
            print_json(json_data)
        else:
            print_count_result(result, display_name_map=display_name_map)

        if result.is_match:
            print_success("All table counts match!")
            raise typer.Exit(0)
        else:
            print_error("Table counts do not match")
            raise typer.Exit(1)

    except typer.Exit:
        raise
    except TableNotFoundError as e:
        _handle_error(e, "Table not found", verbose, json_output, start_time)
    except QueryExecutionError as e:
        _handle_error(e, "Query execution error", verbose, json_output, start_time)
    except SQLInjectionError as e:
        _handle_error(e, "Invalid input", verbose, json_output, start_time)
    except AttachError as e:
        _handle_error(e, "Database attach error", verbose, json_output, start_time)
    except DatabaseError as e:
        _handle_error(e, "Database error", verbose, json_output, start_time)
    except ValueError as e:
        _handle_error(e, "Invalid value", verbose, json_output, start_time)
    except Exception as e:
        _handle_error(e, "Unexpected error", verbose, json_output, start_time)


def _handle_error(
    e: Exception,
    prefix: str,
    verbose: bool,
    json_output: bool,
    start_time: float,
) -> None:
    """Handle an error with appropriate output format."""
    import time as time_module

    duration = time_module.time() - start_time
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
        if verbose and not isinstance(e, (ValueError, SQLInjectionError)):
            console.print_exception()

    raise typer.Exit(2) from None
