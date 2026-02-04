"""Compare command for comparing data between two tables."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

from quack_diff.cli.console import (
    console,
    print_error,
    print_info,
    print_success,
    set_json_output_mode,
    status_context,
)
from quack_diff.cli.errors import get_error_info
from quack_diff.cli.formatters import (
    SnowflakeConnectionInfo,
    print_diff_result,
    print_snowflake_connections,
)
from quack_diff.cli.output import (
    format_diff_result_json,
    format_error_json,
    print_json,
)
from quack_diff.config import get_settings
from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer
from quack_diff.core.sql_utils import (
    AttachError,
    DatabaseError,
    KeyColumnError,
    QueryExecutionError,
    SchemaError,
    SQLInjectionError,
    TableNotFoundError,
)

if TYPE_CHECKING:
    from quack_diff.config import Settings

logger = logging.getLogger(__name__)


def _parse_table_reference(table: str) -> tuple[str | None, str]:
    """Parse a table reference to extract alias and table name.

    Args:
        table: Table reference (e.g., "sf.SCHEMA.TABLE" or "SCHEMA.TABLE")

    Returns:
        Tuple of (alias, table_name). Alias is None if not present.
    """
    parts = table.split(".", 1)
    if len(parts) == 2 and parts[0].lower() in ("sf", "snowflake"):
        # Has a recognized alias prefix
        return parts[0].lower(), parts[1]

    # Check if first part is a configured database alias
    # For now, just check common patterns
    if len(parts) >= 2:
        first_part = parts[0].lower()
        # Return as potential alias if it's short (likely an alias)
        if len(first_part) <= 4 and first_part.isalpha():
            return first_part, ".".join(parts[1:])

    return None, table


def _is_snowflake_table(table: str, settings: Settings) -> bool:
    """Check if a table reference points to a Snowflake table.

    Args:
        table: Table reference
        settings: Application settings

    Returns:
        True if this is a Snowflake table
    """
    alias, _ = _parse_table_reference(table)

    # Check explicit sf/snowflake prefix
    if alias in ("sf", "snowflake"):
        return True

    # Check if alias is configured as snowflake in databases config
    if alias and alias in settings.databases:
        db_config = settings.databases[alias]
        return db_config.get("type", "snowflake").lower() == "snowflake"

    return False


def _auto_attach_databases(
    connector: DuckDBConnector,
    settings: Settings,
    source: str,
    target: str,
    verbose: bool = False,
) -> None:
    """Auto-attach DuckDB databases based on config and table references.

    Attaches DuckDB databases from settings.databases config for any aliases
    found in source/target table references.

    Note: Snowflake tables are handled separately via pull_snowflake_table(),
    not through attachment.

    Args:
        connector: DuckDB connector
        settings: Application settings
        source: Source table reference
        target: Target table reference
        verbose: Enable verbose output
    """
    # Collect unique aliases from table references
    aliases_to_attach: set[str] = set()

    for table in (source, target):
        alias, _ = _parse_table_reference(table)
        if alias:
            aliases_to_attach.add(alias)

    # Attach each database
    for alias in aliases_to_attach:
        if alias in connector.attached_databases:
            logger.debug(f"Database '{alias}' already attached")
            continue

        # Check if alias is in databases config
        if alias in settings.databases:
            db_config = settings.databases[alias]
            db_type = db_config.get("type", "duckdb").lower()

            if db_type == "duckdb":
                path = db_config.get("path")
                if path:
                    if verbose:
                        print_info(f"Attaching DuckDB database: {path} as '{alias}'")
                    connector.attach_duckdb(alias, str(path))


def _pull_snowflake_tables(
    connector: DuckDBConnector,
    settings: Settings,
    source: str,
    target: str,
    source_timestamp: str | None = None,
    source_offset: str | None = None,
    target_timestamp: str | None = None,
    target_offset: str | None = None,
    verbose: bool = False,
) -> tuple[str, str, list[SnowflakeConnectionInfo]]:
    """Pull Snowflake tables into local DuckDB tables using native connector.

    This approach uses snowflake-connector-python directly, which provides:
    - Support for time-travel queries via Snowflake's AT syntax
    - Better compatibility (avoids virtual column errors)
    - No dependency on ADBC driver

    Args:
        connector: DuckDB connector
        settings: Application settings
        source: Source table reference
        target: Target table reference
        source_timestamp: Time-travel timestamp for source
        source_offset: Time-travel offset for source
        target_timestamp: Time-travel timestamp for target
        target_offset: Time-travel offset for target
        verbose: Enable verbose output

    Returns:
        Tuple of (source_local_name, target_local_name, connection_info_list)
    """
    source_local = "__source_pulled"
    target_local = "__target_pulled"
    connection_infos: list[SnowflakeConnectionInfo] = []

    # Pull source table
    source_alias, source_table = _parse_table_reference(source)
    if source_alias and _is_snowflake_table(source, settings):
        if verbose:
            time_travel = ""
            if source_timestamp:
                time_travel = f" AT {source_timestamp}"
            elif source_offset:
                time_travel = f" AT {source_offset}"
            print_info(f"Pulling Snowflake table: {source_table}{time_travel}")

        # Get connection config and database override
        config = None
        source_database = None
        source_connection_name = None
        if source_alias in settings.databases:
            db_config = settings.databases[source_alias]
            source_connection_name = db_config.get("connection_name")
            source_database = db_config.get("database")
            if source_connection_name:
                from quack_diff.config import SnowflakeConfig

                config = SnowflakeConfig(connection_name=source_connection_name)
        if config is None:
            config = settings.snowflake

        connector.pull_snowflake_table(
            table_name=source_table,
            local_name=source_local,
            timestamp=source_timestamp,
            offset=source_offset,
            config=config,
            database=source_database,
        )

        # Collect connection info for display
        connection_infos.append(
            SnowflakeConnectionInfo(
                alias="source",
                table_name=source_table,
                account=config.account,
                user=config.user,
                database=source_database or config.database,
                schema=config.schema_name,
                warehouse=config.warehouse,
                role=config.role,
                authenticator=config.authenticator,
                connection_name=source_connection_name or config.connection_name,
            )
        )
    else:
        source_local = source

    # Pull target table
    target_alias, target_table = _parse_table_reference(target)
    if target_alias and _is_snowflake_table(target, settings):
        if verbose:
            time_travel = ""
            if target_timestamp:
                time_travel = f" AT {target_timestamp}"
            elif target_offset:
                time_travel = f" AT {target_offset}"
            print_info(f"Pulling Snowflake table: {target_table}{time_travel}")

        # Get connection config and database override
        config = None
        target_database = None
        target_connection_name = None
        if target_alias in settings.databases:
            db_config = settings.databases[target_alias]
            target_connection_name = db_config.get("connection_name")
            target_database = db_config.get("database")
            if target_connection_name:
                from quack_diff.config import SnowflakeConfig

                config = SnowflakeConfig(connection_name=target_connection_name)
        if config is None:
            config = settings.snowflake

        connector.pull_snowflake_table(
            table_name=target_table,
            local_name=target_local,
            timestamp=target_timestamp,
            offset=target_offset,
            config=config,
            database=target_database,
        )

        # Collect connection info for display
        connection_infos.append(
            SnowflakeConnectionInfo(
                alias="target",
                table_name=target_table,
                account=config.account,
                user=config.user,
                database=target_database or config.database,
                schema=config.schema_name,
                warehouse=config.warehouse,
                role=config.role,
                authenticator=config.authenticator,
                connection_name=target_connection_name or config.connection_name,
            )
        )
    else:
        target_local = target

    return source_local, target_local, connection_infos


def compare(
    source: Annotated[
        str,
        typer.Option(
            "--source",
            "-s",
            help="Source table (e.g., 'db.schema.table' or path to file)",
        ),
    ],
    target: Annotated[
        str,
        typer.Option(
            "--target",
            "-t",
            help="Target table (e.g., 'db.schema.table' or path to file)",
        ),
    ],
    key: Annotated[
        str,
        typer.Option(
            "--key",
            "-k",
            help="Primary key column for row identification",
        ),
    ],
    columns: Annotated[
        str | None,
        typer.Option(
            "--columns",
            "-c",
            help="Comma-separated list of columns to compare (default: all common columns)",
        ),
    ] = None,
    source_at: Annotated[
        str | None,
        typer.Option(
            "--source-at",
            help="Time-travel for source (e.g., '5 minutes ago', timestamp)",
        ),
    ] = None,
    target_at: Annotated[
        str | None,
        typer.Option(
            "--target-at",
            help="Time-travel for target (e.g., '5 minutes ago', timestamp)",
        ),
    ] = None,
    threshold: Annotated[
        float,
        typer.Option(
            "--threshold",
            help="Maximum acceptable difference ratio (0.0 = exact match, 0.01 = 1%)",
        ),
    ] = 0.0,
    limit: Annotated[
        int | None,
        typer.Option(
            "--limit",
            "-l",
            help="Maximum number of differences to show",
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
            help="Show detailed output including schema comparison",
        ),
    ] = False,
    fail_on_modified: Annotated[
        bool,
        typer.Option(
            "--fail-on-modified",
            help="Exit with error code if modified rows are found (default: only fail on added/removed)",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Show what would be compared without executing the comparison",
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
    """Compare data between two tables.

    Examples:

        # Compare two local DuckDB/Parquet files

        quack-diff compare --source data/prod.parquet --target data/dev.parquet --key id

        # Compare tables in attached databases

        quack-diff compare --source sf.schema.users --target pg.public.users --key user_id

        # Time-travel comparison (Snowflake)

        quack-diff compare --source sf.orders --target sf.orders \\
            --source-at "5 minutes ago" --key order_id

        # Dry run to see what would be compared

        quack-diff compare --source prod.users --target dev.users --key id --dry-run

        # JSON output for CI/CD pipelines

        quack-diff compare --source prod.users --target dev.users --key id --json
    """
    # Enable JSON output mode if requested
    if json_output:
        set_json_output_mode(True)

    start_time = time.time()

    try:
        # Load settings
        settings = get_settings(config_file=config_file)

        # Parse columns if provided
        column_list = None
        if columns:
            column_list = [c.strip() for c in columns.split(",")]

        # Parse time-travel options
        source_offset = None
        source_timestamp = None
        if source_at:
            if "ago" in source_at.lower():
                source_offset = source_at
            else:
                source_timestamp = source_at

        target_offset = None
        target_timestamp = None
        if target_at:
            if "ago" in target_at.lower():
                target_offset = target_at
            else:
                target_timestamp = target_at

        # Check if we need to use the Snowflake pull approach
        use_snowflake_pull = _is_snowflake_table(source, settings) or _is_snowflake_table(target, settings)

        # Handle dry-run mode
        if dry_run:
            _print_dry_run_info(
                source=source,
                target=target,
                key=key,
                columns=column_list,
                source_at=source_at,
                target_at=target_at,
                threshold=threshold,
                limit=limit,
                use_snowflake_pull=use_snowflake_pull,
                json_output=json_output,
            )
            raise typer.Exit(0)

        # Create connector and differ
        with DuckDBConnector(settings=settings) as connector:
            differ = DataDiffer(
                connector=connector,
                null_sentinel=settings.defaults.null_sentinel,
                column_delimiter=settings.defaults.column_delimiter,
            )

            if verbose:
                print_info(f"Comparing {source} vs {target}")

            # Determine table names to compare
            snowflake_connections: list[SnowflakeConnectionInfo] = []
            if use_snowflake_pull:
                # Use native Snowflake connector for pulling data (supports time-travel)
                with status_context("Pulling data from Snowflake..."):
                    source_table_name, target_table_name, snowflake_connections = _pull_snowflake_tables(
                        connector=connector,
                        settings=settings,
                        source=source,
                        target=target,
                        source_timestamp=source_timestamp,
                        source_offset=source_offset,
                        target_timestamp=target_timestamp,
                        target_offset=target_offset,
                        verbose=verbose,
                    )
                # Time-travel already applied during pull, so don't pass to diff
                source_timestamp = None
                source_offset = None
                target_timestamp = None
                target_offset = None

                # Display Snowflake connection details when verbose
                if verbose and snowflake_connections:
                    print_snowflake_connections(snowflake_connections)
            else:
                # Auto-attach databases for non-Snowflake tables
                _auto_attach_databases(connector, settings, source, target, verbose)
                source_table_name = source
                target_table_name = target

            # Perform diff with progress indication
            with status_context("Comparing tables..."):
                result = differ.diff(
                    source_table=source_table_name,
                    target_table=target_table_name,
                    key_column=key,
                    columns=column_list,
                    source_timestamp=source_timestamp,
                    source_offset=source_offset,
                    target_timestamp=target_timestamp,
                    target_offset=target_offset,
                    threshold=threshold,
                    limit=limit,
                )

            duration = time.time() - start_time

            # Output results based on format
            if json_output:
                json_data = format_diff_result_json(
                    result,
                    source_display_name=source,
                    target_display_name=target,
                    duration_seconds=duration,
                )
                print_json(json_data)
            else:
                # Print results with original table names for display
                print_diff_result(
                    result,
                    verbose=verbose,
                    source_display_name=source,
                    target_display_name=target,
                )

            # Exit with appropriate code
            if result.is_match:
                print_success("Tables match!")
                raise typer.Exit(0)
            elif threshold > 0 and result.is_within_threshold:
                print_success(f"Differences within threshold ({threshold * 100:.2f}%)")
                raise typer.Exit(0)
            else:
                # Determine if we should fail based on difference types
                has_added_or_removed = result.added_count > 0 or result.removed_count > 0
                has_modified = result.modified_count > 0

                # By default, only fail on added/removed rows
                # With --fail-on-modified, also fail on modified rows
                should_fail = has_added_or_removed or (fail_on_modified and has_modified)

                if should_fail:
                    print_error(f"Found {result.total_differences} differences")
                    raise typer.Exit(1)
                else:
                    print_success(f"Found {result.modified_count} modified rows (no added/removed)")
                    raise typer.Exit(0)

    except typer.Exit:
        raise
    except TableNotFoundError as e:
        _handle_error(e, "Table not found", verbose, json_output, start_time)
    except KeyColumnError as e:
        _handle_error(e, "Key column error", verbose, json_output, start_time)
    except SchemaError as e:
        _handle_error(e, "Schema error", verbose, json_output, start_time)
    except AttachError as e:
        _handle_error(e, "Database attach error", verbose, json_output, start_time)
    except QueryExecutionError as e:
        _handle_error(e, "Query execution error", verbose, json_output, start_time)
    except SQLInjectionError as e:
        _handle_error(e, "Invalid input", verbose, json_output, start_time)
    except DatabaseError as e:
        _handle_error(e, "Database error", verbose, json_output, start_time)
    except ValueError as e:
        _handle_error(e, "Invalid value", verbose, json_output, start_time)
    except Exception as e:
        _handle_error(e, "Unexpected error", verbose, json_output, start_time)


def _print_dry_run_info(
    source: str,
    target: str,
    key: str,
    columns: list[str] | None,
    source_at: str | None,
    target_at: str | None,
    threshold: float,
    limit: int | None,
    use_snowflake_pull: bool,
    json_output: bool,
) -> None:
    """Print dry-run information showing what would be compared.

    Args:
        source: Source table
        target: Target table
        key: Key column
        columns: Columns to compare
        source_at: Source time-travel
        target_at: Target time-travel
        threshold: Difference threshold
        limit: Result limit
        use_snowflake_pull: Whether Snowflake pull would be used
        json_output: Whether to output as JSON
    """
    dry_run_info = {
        "mode": "dry_run",
        "source": {
            "table": source,
            "time_travel": source_at,
            "is_snowflake": use_snowflake_pull and "sf." in source.lower(),
        },
        "target": {
            "table": target,
            "time_travel": target_at,
            "is_snowflake": use_snowflake_pull and "sf." in target.lower(),
        },
        "comparison": {
            "key_column": key,
            "columns": columns or "all common columns",
            "threshold": threshold,
            "limit": limit,
        },
        "operations": [],
    }

    # Describe what would happen
    operations = []
    if use_snowflake_pull:
        operations.append("Pull data from Snowflake using native connector")
    operations.append("Compare table schemas")
    operations.append("Count rows in both tables")
    operations.append("Compute row hashes and identify differences")

    dry_run_info["operations"] = operations

    if json_output:
        print_json(dry_run_info)
    else:
        console.print("\n[header]Dry Run Mode[/header]")
        console.print("The following operations would be performed:\n")

        console.print(f"[key]Source:[/key] {source}")
        if source_at:
            console.print(f"  [muted]Time travel: {source_at}[/muted]")

        console.print(f"[key]Target:[/key] {target}")
        if target_at:
            console.print(f"  [muted]Time travel: {target_at}[/muted]")

        console.print(f"\n[key]Key Column:[/key] {key}")
        console.print(f"[key]Columns:[/key] {columns or 'all common columns'}")

        if threshold > 0:
            console.print(f"[key]Threshold:[/key] {threshold * 100:.2f}%")
        if limit:
            console.print(f"[key]Limit:[/key] {limit}")

        console.print("\n[header]Operations:[/header]")
        for i, op in enumerate(operations, 1):
            console.print(f"  {i}. {op}")

        console.print()


def _handle_error(
    e: Exception,
    prefix: str,
    verbose: bool,
    json_output: bool,
    start_time: float,
) -> None:
    """Handle an error with appropriate output format.

    Args:
        e: The exception
        prefix: Error message prefix
        verbose: Whether verbose mode is enabled
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
        # Add duration to the output
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
