"""Typer CLI entrypoint for quack-diff."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from quack_diff import __version__
from quack_diff.cli.console import console, print_error, print_info, print_success
from quack_diff.cli.formatters import print_diff_result, print_schema_result
from quack_diff.config import get_settings
from quack_diff.core.adapters.base import Dialect
from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer

app = typer.Typer(
    name="quack-diff",
    help="The zero-dependency regression testing tool for modern data warehouses.",
    add_completion=False,
    no_args_is_help=True,
)


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"quack-diff version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            callback=version_callback,
            is_eager=True,
            help="Show version and exit.",
        ),
    ] = False,
) -> None:
    """quack-diff: DuckDB-powered data diffing."""
    pass


@app.command()
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

        # Create connector and differ
        with DuckDBConnector(settings=settings) as connector:
            differ = DataDiffer(
                connector=connector,
                null_sentinel=settings.defaults.null_sentinel,
                column_delimiter=settings.defaults.column_delimiter,
            )

            if verbose:
                print_info(f"Comparing {source} vs {target}")

            # Perform diff
            result = differ.diff(
                source_table=source,
                target_table=target,
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


@app.command()
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
) -> None:
    """Compare schemas of two tables.

    Shows column names, types, and identifies mismatches between
    source and target table structures.

    Example:

        quack-diff schema --source prod.users --target dev.users
    """
    try:
        settings = get_settings(config_file=config_file)

        with DuckDBConnector(settings=settings) as connector:
            differ = DataDiffer(connector=connector)

            result = differ.compare_schemas(
                source_table=source,
                target_table=target,
            )

            print_schema_result(result)

            if result.is_identical or result.is_compatible:
                raise typer.Exit(0)
            else:
                raise typer.Exit(1)

    except Exception as e:
        print_error(f"Error: {e}")
        raise typer.Exit(2) from None


@app.command()
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
            help="Database type (snowflake, postgres, duckdb)",
        ),
    ] = "duckdb",
    path: Annotated[
        str | None,
        typer.Option(
            "--path",
            "-p",
            help="Path to database file (for DuckDB/SQLite)",
        ),
    ] = None,
    connection_string: Annotated[
        str | None,
        typer.Option(
            "--connection-string",
            help="Connection string (for Postgres)",
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

            elif dialect == Dialect.POSTGRES:
                if connection_string:
                    connector.attach_postgres(name, connection_string=connection_string)
                else:
                    connector.attach_postgres(name)

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


if __name__ == "__main__":
    app()
