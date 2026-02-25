"""Rich formatters for displaying diff results in the terminal."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from rich import box
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from quack_diff.cli.console import console
from quack_diff.core.differ import CountResult, DiffType

if TYPE_CHECKING:
    from quack_diff.core.differ import DiffResult, SchemaComparisonResult


@dataclass
class SnowflakeConnectionInfo:
    """Information about a Snowflake connection used during diff."""

    alias: str  # e.g., "source" or "target"
    table_name: str
    account: str | None = None
    user: str | None = None
    database: str | None = None
    schema: str | None = None
    warehouse: str | None = None
    role: str | None = None
    authenticator: str | None = None
    connection_name: str | None = None


def format_diff_summary(
    result: DiffResult,
    source_display_name: str | None = None,
    target_display_name: str | None = None,
) -> Panel:
    """Create a summary panel for diff results.

    Args:
        result: DiffResult to format
        source_display_name: Optional display name for source table
        target_display_name: Optional display name for target table

    Returns:
        Rich Panel with summary table
    """
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Label", style="bold")
    table.add_column("Value", justify="right")

    # Row counts - use display names if provided
    source_name = source_display_name or result.source_table
    target_name = target_display_name or result.target_table
    table.add_row("Source Table", source_name)
    table.add_row("Target Table", target_name)
    table.add_row("", "")  # Spacer

    table.add_row("Source Rows", f"{result.source_row_count:,}")
    table.add_row("Target Rows", f"{result.target_row_count:,}")

    # Row difference
    row_diff = result.target_row_count - result.source_row_count
    if row_diff > 0:
        row_diff_text = Text(f"+{row_diff:,}", style="added")
    elif row_diff < 0:
        row_diff_text = Text(f"{row_diff:,}", style="removed")
    else:
        row_diff_text = Text("0", style="muted")
    table.add_row("Row Difference", row_diff_text)
    table.add_row("", "")  # Spacer

    # Schema status
    schema = result.schema_comparison
    if schema.is_identical:
        schema_status = Text("Identical", style="success")
    elif schema.is_compatible:
        schema_status = Text(
            f"{len(schema.matching_columns)}/{len(schema.source_columns)} columns",
            style="warning",
        )
    else:
        schema_status = Text("Incompatible", style="error")

    table.add_row("Schema", schema_status)

    # Diff summary
    if result.is_match:
        diff_status = Text("Match", style="success")
    else:
        diff_status = Text(f"{result.total_differences:,} differences", style="error")

    table.add_row("Data", diff_status)

    if result.total_differences > 0:
        table.add_row("", "")  # Spacer
        table.add_row(
            Text("  Added", style="added"),
            f"{result.added_count:,}",
        )
        table.add_row(
            Text("  Removed", style="removed"),
            f"{result.removed_count:,}",
        )
        table.add_row(
            Text("  Modified", style="modified"),
            f"{result.modified_count:,}",
        )
        table.add_row("", "")  # Spacer
        table.add_row("Diff %", f"{result.diff_percentage:.2f}%")

    # Threshold check
    if result.threshold > 0:
        table.add_row("Threshold", f"{result.threshold * 100:.2f}%")
        if result.is_within_threshold:
            table.add_row("Status", Text("PASS", style="success"))
        else:
            table.add_row("Status", Text("FAIL", style="error"))

    title = "Diff Summary"
    border_style = "green" if result.is_match else "red"

    return Panel(table, title=title, border_style=border_style)


def format_diff_table(result: DiffResult, max_rows: int = 50) -> Table | None:
    """Create a table showing individual row differences.

    Args:
        result: DiffResult to format
        max_rows: Maximum rows to display

    Returns:
        Rich Table or None if no differences
    """
    if result.total_differences == 0:
        return None

    table = Table(title="Row Differences", show_lines=True)
    table.add_column("Key", style="key")
    table.add_column("Type", justify="center")
    table.add_column("Source Hash", style="muted", overflow="fold")
    table.add_column("Target Hash", style="muted", overflow="fold")

    for idx, diff in enumerate(result.differences):
        if idx >= max_rows:
            break

        # Format diff type with color
        if diff.diff_type == DiffType.ADDED:
            type_text = Text("ADDED", style="added")
        elif diff.diff_type == DiffType.REMOVED:
            type_text = Text("REMOVED", style="removed")
        else:
            type_text = Text("MODIFIED", style="modified")

        table.add_row(
            str(diff.key),
            type_text,
            diff.source_hash or "-",
            diff.target_hash or "-",
        )

    if result.total_differences > max_rows:
        table.add_row(
            f"... and {result.total_differences - max_rows} more",
            "",
            "",
            "",
            style="muted",
        )

    return table


def format_schema_comparison(schema: SchemaComparisonResult) -> Panel:
    """Create a panel showing schema comparison results.

    Args:
        schema: SchemaComparisonResult to format

    Returns:
        Rich Panel with schema details
    """
    table = Table(show_header=True, box=None, padding=(0, 2))
    table.add_column("Column", style="bold")
    table.add_column("Source Type")
    table.add_column("Target Type")
    table.add_column("Status", justify="center")

    source_by_name = {c.name.lower(): c for c in schema.source_columns}
    target_by_name = {c.name.lower(): c for c in schema.target_columns}

    # Show matching columns
    for col_name in sorted(schema.matching_columns):
        source_col = source_by_name.get(col_name.lower())
        target_col = target_by_name.get(col_name.lower())

        if col_name in schema.type_mismatches:
            status = Text("Type Mismatch", style="warning")
        else:
            status = Text("OK", style="success")

        table.add_row(
            col_name,
            source_col.data_type if source_col else "-",
            target_col.data_type if target_col else "-",
            status,
        )

    # Show source-only columns
    for col_name in sorted(schema.source_only_columns):
        source_col = source_by_name.get(col_name.lower())
        table.add_row(
            col_name,
            source_col.data_type if source_col else "-",
            Text("-", style="muted"),
            Text("Source Only", style="removed"),
        )

    # Show target-only columns
    for col_name in sorted(schema.target_only_columns):
        target_col = target_by_name.get(col_name.lower())
        table.add_row(
            col_name,
            Text("-", style="muted"),
            target_col.data_type if target_col else "-",
            Text("Target Only", style="added"),
        )

    title = "Schema Comparison"
    border_style = "green" if schema.is_identical else "yellow"

    return Panel(table, title=title, border_style=border_style)


def print_diff_result(
    result: DiffResult,
    verbose: bool = False,
    source_display_name: str | None = None,
    target_display_name: str | None = None,
) -> None:
    """Print a complete diff result to the console.

    Args:
        result: DiffResult to print
        verbose: Show detailed output including schema
        source_display_name: Optional display name for source table
        target_display_name: Optional display name for target table
    """
    # Always show summary
    console.print()
    console.print(format_diff_summary(result, source_display_name, target_display_name))

    # Show schema comparison if verbose or there are issues
    if verbose or not result.schema_comparison.is_identical:
        console.print()
        console.print(format_schema_comparison(result.schema_comparison))

    # Show diff table if there are differences
    if result.total_differences > 0:
        diff_table = format_diff_table(result)
        if diff_table:
            console.print()
            console.print(diff_table)

    console.print()


def print_schema_result(schema: SchemaComparisonResult) -> None:
    """Print schema comparison result to console.

    Args:
        schema: SchemaComparisonResult to print
    """
    console.print()
    console.print(format_schema_comparison(schema))
    console.print()

    # Print summary
    if schema.is_identical:
        console.print("[success]Schemas are identical[/success]")
    elif schema.is_compatible:
        console.print("[warning]Schemas are compatible but not identical[/warning]")
    else:
        console.print("[error]Schemas are not compatible[/error]")

    console.print()


def format_snowflake_connections(
    connections: list[SnowflakeConnectionInfo],
) -> Panel:
    """Create a panel showing Snowflake connection details.

    Args:
        connections: List of SnowflakeConnectionInfo to display

    Returns:
        Rich Panel with connection details
    """
    table = Table(
        show_header=True,
        show_lines=True,  # Show horizontal lines between rows
        box=box.ROUNDED,
        border_style="bright_blue",
        header_style="bold black on bright_cyan",
        padding=(0, 1),
    )
    table.add_column("Property", style="bold black", min_width=12)

    # Add a column for each connection
    for conn in connections:
        table.add_column(conn.alias.title(), justify="left", style="black")

    # Helper to format values (show dash for missing values)
    def format_value(value: str | None) -> Text | str:
        if value is None:
            return Text("-", style="dim black italic")
        return Text(str(value), style="black")

    # Connection name (if using connections.toml)
    values = [format_value(conn.connection_name) for conn in connections]
    table.add_row("Profile", *values)

    # Account
    values = [format_value(conn.account) for conn in connections]
    table.add_row("Account", *values)

    # User
    values = [format_value(conn.user) for conn in connections]
    table.add_row("User", *values)

    # Database
    values = [format_value(conn.database) for conn in connections]
    table.add_row("Database", *values)

    # Schema
    values = [format_value(conn.schema) for conn in connections]
    table.add_row("Schema", *values)

    # Warehouse
    values = [format_value(conn.warehouse) for conn in connections]
    table.add_row("Warehouse", *values)

    # Role
    values = [format_value(conn.role) for conn in connections]
    table.add_row("Role", *values)

    # Authenticator
    values = [format_value(conn.authenticator) for conn in connections]
    table.add_row("Authenticator", *values)

    # Table name
    values = [Text(conn.table_name, style="black") for conn in connections]
    table.add_row("Table", *values)

    return Panel(table, title="[bold blue]Snowflake Connection Details", border_style="blue")


def print_snowflake_connections(connections: list[SnowflakeConnectionInfo]) -> None:
    """Print Snowflake connection details to the console.

    Args:
        connections: List of SnowflakeConnectionInfo to print
    """
    if not connections:
        return

    console.print()
    console.print(format_snowflake_connections(connections))
    console.print()


def format_count_summary(
    result: CountResult,
    display_name_map: dict[str, str] | None = None,
) -> Panel:
    """Create a summary panel for count check results.

    Args:
        result: CountResult to format
        display_name_map: Optional map from resolved table name to display name

    Returns:
        Rich Panel with count table and status
    """
    table = Table(show_header=True, box=None, padding=(0, 2))
    table.add_column("Table", style="bold")
    table.add_column("Count", justify="right")

    # Add SUM column when any table has a sum_value
    has_sum = any(getattr(tc, "sum_value", None) is not None for tc in result.table_counts)
    if has_sum:
        table.add_column("Sum", justify="right")

    display_name_map = display_name_map or {}
    for tc in result.table_counts:
        display_name = display_name_map.get(tc.table, tc.table)
        row = [display_name, f"{tc.count:,}"]
        if has_sum:
            sum_val = getattr(tc, "sum_value", None)
            row.append("-" if sum_val is None else f"{sum_val:,}")
        table.add_row(*row)

    table.add_row("", "")  # Spacer
    status = Text("MATCH", style="success") if result.is_match else Text("MISMATCH", style="error")
    table.add_row("Status", status)

    title = "Count Summary"
    border_style = "green" if result.is_match else "red"
    return Panel(table, title=title, border_style=border_style)


def print_count_result(
    result: CountResult,
    display_name_map: dict[str, str] | None = None,
) -> None:
    """Print count check result to the console.

    Args:
        result: CountResult to print
        display_name_map: Optional map from resolved table name to display name
    """
    console.print()
    console.print(format_count_summary(result, display_name_map))
    console.print()
