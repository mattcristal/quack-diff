"""Count command for validating row counts across multiple tables."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
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
from quack_diff.cli.formatters import print_count_result
from quack_diff.cli.output import format_count_result_json, format_error_json, print_json
from quack_diff.config import get_settings
from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import CountResult, DataDiffer, TableCount
from quack_diff.core.sql_utils import (
    AttachError,
    DatabaseError,
    QueryExecutionError,
    SQLInjectionError,
    TableNotFoundError,
    sanitize_identifier,
)

if TYPE_CHECKING:
    from quack_diff.config import Settings

logger = logging.getLogger(__name__)

_TABLE_SPEC_RE = re.compile(r"^(?P<ref>[^\[\]]+?)(?:\[(?P<group_by>[^\]]*)\])?$")


@dataclass
class TableSpec:
    """Parsed table specification with optional group-by columns.

    Created from the ``-t`` flag value, e.g.
    ``"sf.DB.SCHEMA.TABLE[col1,col2]"``.
    """

    raw: str
    alias: str | None
    table: str
    group_by: list[str] | None
    is_snowflake: bool


def _parse_table_spec(table: str, settings: Settings) -> TableSpec:
    """Parse a ``-t`` value into a :class:`TableSpec`.

    Supports the inline bracket syntax for per-table GROUP BY::

        sf.DATABASE.SCHEMA.TABLE               -> plain table
        sf.DATABASE.SCHEMA.TABLE[col1,col2]    -> table with group-by

    Args:
        table: Raw ``-t`` value
        settings: Application settings (used for alias resolution)

    Returns:
        Parsed TableSpec

    Raises:
        ValueError: If the syntax is invalid
    """
    m = _TABLE_SPEC_RE.match(table.strip())
    if not m:
        raise ValueError(
            f"Invalid table specification: '{table}'. Expected format: 'alias.TABLE' or 'alias.TABLE[col1,col2,...]'"
        )

    ref = m.group("ref").strip()
    group_by_str = m.group("group_by")
    group_by: list[str] | None = None
    if group_by_str is not None:
        group_by = [c.strip() for c in group_by_str.split(",") if c.strip()]
        if not group_by:
            raise ValueError(f"Empty group-by column list in: '{table}'")

    alias, table_name = _parse_table_reference(ref, settings)
    is_sf = _is_snowflake_ref(alias, settings)

    return TableSpec(
        raw=table,
        alias=alias,
        table=table_name,
        group_by=group_by,
        is_snowflake=is_sf,
    )


def _parse_table_reference(table: str, settings: Settings) -> tuple[str | None, str]:
    """Extract alias and table name from a dotted reference."""
    known_aliases = set(settings.databases.keys()) if settings.databases else set()
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


def _is_snowflake_ref(alias: str | None, settings: Settings) -> bool:
    """Return True when *alias* points to a Snowflake connection."""
    if alias in ("sf", "snowflake"):
        return True
    if alias and alias in settings.databases:
        db_config = settings.databases[alias]
        return db_config.get("type", "snowflake").lower() == "snowflake"
    return False


def _resolve_snowflake_config(alias: str | None, settings: Settings) -> tuple:
    """Return ``(config, database)`` for a Snowflake alias.

    Returns:
        Tuple of (SnowflakeConfig, database_override | None)
    """
    config = None
    database = None
    if alias and alias in settings.databases:
        db_config = settings.databases[alias]
        connection_name = db_config.get("connection_name")
        database = db_config.get("database")
        if connection_name:
            from quack_diff.config import SnowflakeConfig

            config = SnowflakeConfig(connection_name=connection_name)
    if config is None:
        config = settings.snowflake
    return config, database


def _build_count_query(
    spec: TableSpec,
    key_column: str | None = None,
    table_ref: str | None = None,
) -> str:
    """Build a SQL count query for a single :class:`TableSpec`.

    Args:
        spec: Parsed table specification
        key_column: If set, use COUNT(DISTINCT key_column)
        table_ref: Override for the fully-qualified table reference to use
            in the generated SQL.  When *None* the spec's ``table`` field
            is used.

    Returns:
        SQL query string

    Raises:
        ValueError: If both group_by and key_column are specified
    """
    if spec.group_by and key_column:
        raise ValueError(f"Cannot combine --key with per-table [group_by] (table: '{spec.raw}'). Use one or the other.")

    sanitized_table = sanitize_identifier(table_ref or spec.table)

    if spec.group_by:
        sanitized_cols = [sanitize_identifier(c) for c in spec.group_by]
        cols_str = ", ".join(sanitized_cols)
        return f"SELECT COUNT(*) FROM (SELECT 1 FROM {sanitized_table} GROUP BY {cols_str})"

    if key_column:
        sanitized_key = sanitize_identifier(key_column)
        return f"SELECT COUNT(DISTINCT {sanitized_key}) FROM {sanitized_table}"

    return f"SELECT COUNT(*) FROM {sanitized_table}"


def _build_sum_query(
    spec: TableSpec,
    sum_column: str,
    table_ref: str | None = None,
) -> str:
    """Build a SQL SUM query for a single :class:`TableSpec`.

    The SUM is computed over the *entire* table (no implicit GROUP BY),
    even when per-table [group_by] is used for the count. This matches
    patterns like:

        SELECT SUM(QUANTITY) FROM FCT_INVOICE;

    Args:
        spec: Parsed table specification
        sum_column: Column to SUM()
        table_ref: Override for the fully-qualified table reference to use
            in the generated SQL.  When *None* the spec's ``table`` field
            is used.

    Returns:
        SQL query string
    """
    sanitized_table = sanitize_identifier(table_ref or spec.table)
    sanitized_col = sanitize_identifier(sum_column)
    return f"SELECT SUM({sanitized_col}) FROM {sanitized_table}"


def _full_table_ref(spec: TableSpec) -> str:
    """Reconstruct the dotted ``alias.table`` reference for local queries."""
    if spec.alias:
        return f"{spec.alias}.{spec.table}"
    return spec.table


def _execute_direct_metrics(
    connector: DuckDBConnector,
    settings: Settings,
    spec: TableSpec,
    key_column: str | None = None,
    sum_column: str | None = None,
    verbose: bool = False,
) -> tuple[int, int | float | None]:
    """Execute aggregate queries for *spec* on the appropriate backend.

    Always computes a COUNT (or COUNT(DISTINCT key_column)) and, when
    *sum_column* is provided, also computes a SUM(sum_column).

    Snowflake tables are aggregated directly on Snowflake (no data
    transfer). DuckDB / local tables are aggregated via the DuckDB
    connector.

    Returns:
        Tuple of (row_count, sum_value_or_None)
    """
    # COUNT / COUNT(DISTINCT ...)
    count_query = _build_count_query(spec, key_column)

    if spec.is_snowflake:
        # Snowflake: use just the table part (alias is a connection ref, not a DB prefix)
        config, database = _resolve_snowflake_config(spec.alias, settings)
        if verbose:
            print_info(f"Counting on Snowflake: {spec.raw}")
            logger.debug(f"Snowflake count query: {count_query}")
        count_result = connector.execute_snowflake_scalar(query=count_query, config=config, database=database)

        sum_result: int | float | None = None
        if sum_column:
            sum_query = _build_sum_query(spec, sum_column)
            if verbose:
                logger.debug(f"Snowflake sum query: {sum_query}")
            sum_result = connector.execute_snowflake_scalar(query=sum_query, config=config, database=database)
    else:
        # DuckDB: reconstruct alias.table for attached databases
        table_ref = _full_table_ref(spec)
        query = _build_count_query(spec, key_column, table_ref=table_ref)
        if verbose:
            print_info(f"Counting locally: {spec.raw}")
            logger.debug(f"DuckDB count query: {query}")
        row = connector.execute_fetchone(query)
        count_result = row[0] if row else 0

        sum_result = None
        if sum_column:
            sum_query = _build_sum_query(spec, sum_column, table_ref=table_ref)
            if verbose:
                logger.debug(f"DuckDB sum query: {sum_query}")
            sum_row = connector.execute_fetchone(sum_query)
            sum_result = sum_row[0] if sum_row else 0

    return int(count_result), sum_result


def _auto_attach_databases(
    connector: DuckDBConnector,
    settings: Settings,
    specs: list[TableSpec],
    verbose: bool = False,
) -> None:
    """Auto-attach DuckDB databases for non-Snowflake aliases."""
    for spec in specs:
        alias = spec.alias
        if not alias or spec.is_snowflake:
            continue
        if alias in connector.attached_databases:
            continue
        if alias in settings.databases:
            db_config = settings.databases[alias]
            db_type = db_config.get("type", "duckdb").lower()
            if db_type == "duckdb":
                path = db_config.get("path")
                if path:
                    if verbose:
                        print_info(f"Attaching DuckDB database: {path} as '{alias}'")
                    connector.attach_duckdb(alias, str(path))


def count(
    tables: Annotated[
        list[str],
        typer.Option(
            "--tables",
            "-t",
            help=(
                "Table(s) to compare counts (repeat or comma-separated). "
                "Append [col1,col2,...] to GROUP BY before counting, e.g. "
                "'sf.DB.SCHEMA.TABLE[salesid,linenum]'"
            ),
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
    sum_columns: Annotated[
        list[str] | None,
        typer.Option(
            "--sum-column",
            "-s",
            help=(
                "Optional column(s) to SUM() alongside row counts. "
                "Provide one value to use the same column for all tables, "
                "or repeat to specify per-table columns matching the -t order."
            ),
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

    Tables can optionally specify per-table GROUP BY columns using bracket
    syntax. This counts distinct combinations rather than raw rows.

    Examples:

        # Same row count across layers

        quack-diff count -t bronze.orders -t silver.orders -t gold.orders

        # Same distinct ID count

        quack-diff count -t bronze.orders -t silver.orders --key order_id

        # Per-table GROUP BY (count distinct groups in second table)

        quack-diff count \\
            -t sf.GOLD.FCT_INVOICE \\
            -t "sf.RAW.INVOICE_LINES[salesid,linenum,tariffcode,linestartdate]"

        # JSON for CI/CD

        quack-diff count -t bronze.orders -t silver.orders --key id --json
    """
    if json_output:
        set_json_output_mode(True)

    start_time = time.time()

    # Flatten: support both -t a -t b and -t "a,b"
    # Be careful not to split inside [...] brackets
    flat_tables: list[str] = []
    for t in tables:
        flat_tables.extend(_split_table_arg(t))

    # Normalise --sum-column values to a per-table list
    per_table_sum_columns: list[str | None] = [None] * len(flat_tables)
    if sum_columns:
        flat_sums: list[str] = []
        for s in sum_columns:
            # Allow simple comma-separated syntax
            flat_sums.extend([part.strip() for part in s.split(",") if part.strip()])

        if len(flat_sums) == 1:
            # Single column applied to all tables
            per_table_sum_columns = [flat_sums[0]] * len(flat_tables)
        elif len(flat_sums) == len(flat_tables):
            # One column per table, in the same order
            per_table_sum_columns = flat_sums
        else:
            msg = (
                "Number of --sum-column values must be 1 or match the number of --tables "
                f"(got {len(flat_sums)} sum columns for {len(flat_tables)} tables)."
            )
            if json_output:
                print_json(
                    format_error_json(
                        error_type="ValueError",
                        message=msg,
                        exit_code=2,
                    )
                )
            else:
                print_error(msg)
            raise typer.Exit(2)

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

        # Parse all table specs (may raise ValueError for bad syntax)
        specs = [_parse_table_spec(t, settings) for t in flat_tables]

        any_snowflake = any(s.is_snowflake for s in specs)
        any_has_group_by = any(s.group_by for s in specs)

        # When there are group-by specs or Snowflake tables we use the
        # direct-execution path (counts run on the source database).
        # For pure local tables without group-by we can still use the
        # legacy differ.count_check path.
        use_direct = any_snowflake or any_has_group_by

        with DuckDBConnector(settings=settings) as connector:
            if use_direct:
                # Attach any DuckDB databases needed for local tables
                _auto_attach_databases(connector, settings, specs, verbose)

                status_msg = "Counting on Snowflake..." if any_snowflake else "Counting..."
                with status_context(status_msg):
                    table_counts: list[TableCount] = []
                    display_name_map: dict[str, str] = {}
                    for i, spec in enumerate(specs):
                        sum_col = per_table_sum_columns[i] if i < len(per_table_sum_columns) else None
                        count_val, sum_val = _execute_direct_metrics(
                            connector=connector,
                            settings=settings,
                            spec=spec,
                            key_column=key,
                            sum_column=sum_col,
                            verbose=verbose,
                        )
                        label = f"__direct_{i}"
                        table_counts.append(
                            TableCount(
                                table=label,
                                count=count_val,
                                sum_value=sum_val,
                                sum_column=sum_col,
                            )
                        )
                        display_name_map[label] = spec.raw

                reference = table_counts[0].count
                is_match = all(tc.count == reference for tc in table_counts)

                # When SUM metrics are present, require them to match as well
                if any(tc.sum_value is not None for tc in table_counts):
                    sum_ref = table_counts[0].sum_value
                    is_match = is_match and all(tc.sum_value == sum_ref for tc in table_counts)

                result = CountResult(
                    table_counts=table_counts,
                    key_column=key,
                    is_match=is_match,
                )
            else:
                # Legacy path: all local, no group-by
                plain_tables = [_full_table_ref(s) for s in specs]
                _auto_attach_databases(connector, settings, specs, verbose)
                display_name_map = {_full_table_ref(s): s.raw for s in specs}

                differ = DataDiffer(
                    connector=connector,
                    null_sentinel=settings.defaults.null_sentinel,
                    column_delimiter=settings.defaults.column_delimiter,
                )
                with status_context("Counting..."):
                    result = differ.count_check(
                        tables=plain_tables,
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


def _split_table_arg(value: str) -> list[str]:
    """Split a comma-separated ``-t`` value, respecting ``[...]`` brackets.

    ``"sf.A,sf.B[x,y]"`` -> ``["sf.A", "sf.B[x,y]"]``
    """
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in value:
        if ch == "[":
            depth += 1
            current.append(ch)
        elif ch == "]":
            depth = max(depth - 1, 0)
            current.append(ch)
        elif ch == "," and depth == 0:
            token = "".join(current).strip()
            if token:
                parts.append(token)
            current = []
        else:
            current.append(ch)
    token = "".join(current).strip()
    if token:
        parts.append(token)
    return parts


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
