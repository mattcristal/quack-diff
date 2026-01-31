"""Schema command for comparing schemas of two tables."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from quack_diff.cli.console import print_error
from quack_diff.cli.formatters import print_schema_result
from quack_diff.config import get_settings
from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer


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
