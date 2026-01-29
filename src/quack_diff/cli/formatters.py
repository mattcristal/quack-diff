"""Rich formatters for displaying diff results in the terminal."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from quack_diff.cli.console import console
from quack_diff.core.differ import DiffType

if TYPE_CHECKING:
    from quack_diff.core.differ import DiffResult, SchemaComparisonResult


def format_diff_summary(result: DiffResult) -> Panel:
    """Create a summary panel for diff results.

    Args:
        result: DiffResult to format

    Returns:
        Rich Panel with summary table
    """
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Label", style="bold")
    table.add_column("Value", justify="right")

    # Row counts
    table.add_row("Source Table", result.source_table)
    table.add_row("Target Table", result.target_table)
    table.add_row("", "")  # Spacer

    table.add_row("Source Rows", f"{result.source_row_count:,}")
    table.add_row("Target Rows", f"{result.target_row_count:,}")
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


def print_diff_result(result: DiffResult, verbose: bool = False) -> None:
    """Print a complete diff result to the console.

    Args:
        result: DiffResult to print
        verbose: Show detailed output including schema
    """
    # Always show summary
    console.print()
    console.print(format_diff_summary(result))

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
