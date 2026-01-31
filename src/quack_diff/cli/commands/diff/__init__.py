"""Diff command for comparing data between two tables."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

from quack_diff.cli.console import console, print_error, print_info, print_success
from quack_diff.cli.formatters import print_diff_result
from quack_diff.config import get_settings
from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer

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
) -> tuple[str, str]:
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
        Tuple of (source_local_name, target_local_name)
    """
    source_local = "__source_pulled"
    target_local = "__target_pulled"

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

        # Get connection config
        config = None
        if source_alias in settings.databases:
            db_config = settings.databases[source_alias]
            connection_name = db_config.get("connection_name")
            if connection_name:
                from quack_diff.config import SnowflakeConfig

                config = SnowflakeConfig(connection_name=connection_name)
        if config is None:
            config = settings.snowflake

        connector.pull_snowflake_table(
            table_name=source_table,
            local_name=source_local,
            timestamp=source_timestamp,
            offset=source_offset,
            config=config,
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

        # Get connection config
        config = None
        if target_alias in settings.databases:
            db_config = settings.databases[target_alias]
            connection_name = db_config.get("connection_name")
            if connection_name:
                from quack_diff.config import SnowflakeConfig

                config = SnowflakeConfig(connection_name=connection_name)
        if config is None:
            config = settings.snowflake

        connector.pull_snowflake_table(
            table_name=target_table,
            local_name=target_local,
            timestamp=target_timestamp,
            offset=target_offset,
            config=config,
        )
    else:
        target_local = target

    return source_local, target_local


def diff(
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
) -> None:
    """Compare data between two tables.

    Examples:

        # Compare two local DuckDB/Parquet files

        quack-diff diff --source data/prod.parquet --target data/dev.parquet --key id

        # Compare tables in attached databases

        quack-diff diff --source sf.schema.users --target pg.public.users --key user_id

        # Time-travel comparison (Snowflake)

        quack-diff diff --source sf.orders --target sf.orders \\
            --source-at "5 minutes ago" --key order_id
    """
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
        use_snowflake_pull = _is_snowflake_table(source, settings) or _is_snowflake_table(
            target, settings
        )

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
            if use_snowflake_pull:
                # Use native Snowflake connector for pulling data (supports time-travel)
                source_table_name, target_table_name = _pull_snowflake_tables(
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
            else:
                # Auto-attach databases for non-Snowflake tables
                _auto_attach_databases(connector, settings, source, target, verbose)
                source_table_name = source
                target_table_name = target

            # Perform diff
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

            # Print results
            print_diff_result(result, verbose=verbose)

            # Exit with appropriate code
            if result.is_match:
                print_success("Tables match!")
                raise typer.Exit(0)
            elif threshold > 0 and result.is_within_threshold:
                print_success(f"Differences within threshold ({threshold * 100:.2f}%)")
                raise typer.Exit(0)
            else:
                print_error(f"Found {result.total_differences} differences")
                raise typer.Exit(1)

    except ValueError as e:
        print_error(str(e))
        raise typer.Exit(2) from None
    except Exception as e:
        print_error(f"Error: {e}")
        if verbose:
            console.print_exception()
        raise typer.Exit(2) from None
