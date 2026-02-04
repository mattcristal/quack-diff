"""JSON and structured output formatting for CI integration."""

from __future__ import annotations

import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from quack_diff.core.differ import DiffResult, SchemaComparisonResult


class OutputFormat(str, Enum):
    """Supported output formats."""

    RICH = "rich"  # Default Rich console output
    JSON = "json"  # JSON for CI/CD integration


@dataclass
class JSONOutputMeta:
    """Metadata for JSON output."""

    tool: str = "quack-diff"
    version: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    duration_seconds: float | None = None


@dataclass
class JSONDiffOutput:
    """JSON output structure for diff results."""

    status: str  # "match", "mismatch", "error"
    exit_code: int
    meta: JSONOutputMeta
    source: dict[str, Any]
    target: dict[str, Any]
    schema: dict[str, Any]
    diff: dict[str, Any]
    threshold: dict[str, Any] | None = None
    error: dict[str, Any] | None = None


@dataclass
class JSONSchemaOutput:
    """JSON output structure for schema comparison results."""

    status: str  # "identical", "compatible", "incompatible", "error"
    exit_code: int
    meta: JSONOutputMeta
    source: dict[str, Any]
    target: dict[str, Any]
    comparison: dict[str, Any]
    error: dict[str, Any] | None = None


@dataclass
class JSONAttachOutput:
    """JSON output structure for attach command results."""

    status: str  # "success", "error"
    exit_code: int
    meta: JSONOutputMeta
    database: dict[str, Any]
    tables: list[str]
    error: dict[str, Any] | None = None


@dataclass
class JSONErrorOutput:
    """JSON output structure for errors."""

    status: str = "error"
    exit_code: int = 2
    meta: JSONOutputMeta = field(default_factory=JSONOutputMeta)
    error: dict[str, Any] = field(default_factory=dict)


def get_version() -> str:
    """Get quack-diff version."""
    try:
        from quack_diff import __version__

        return __version__
    except ImportError:
        return "unknown"


def format_diff_result_json(
    result: DiffResult,
    source_display_name: str | None = None,
    target_display_name: str | None = None,
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    """Format DiffResult as JSON-serializable dictionary.

    Args:
        result: DiffResult to format
        source_display_name: Optional display name for source
        target_display_name: Optional display name for target
        duration_seconds: Optional execution duration

    Returns:
        Dictionary suitable for JSON serialization
    """
    # Determine status
    if result.is_match or result.threshold > 0 and result.is_within_threshold:
        status = "match"
        exit_code = 0
    else:
        status = "mismatch"
        exit_code = 1

    # Build output structure
    output = JSONDiffOutput(
        status=status,
        exit_code=exit_code,
        meta=JSONOutputMeta(
            version=get_version(),
            duration_seconds=duration_seconds,
        ),
        source={
            "table": source_display_name or result.source_table,
            "row_count": result.source_row_count,
        },
        target={
            "table": target_display_name or result.target_table,
            "row_count": result.target_row_count,
        },
        schema={
            "is_identical": result.schema_comparison.is_identical,
            "is_compatible": result.schema_comparison.is_compatible,
            "matching_columns": result.schema_comparison.matching_columns,
            "source_only_columns": result.schema_comparison.source_only_columns,
            "target_only_columns": result.schema_comparison.target_only_columns,
            "type_mismatches": result.schema_comparison.type_mismatches,
        },
        diff={
            "total_differences": result.total_differences,
            "added_count": result.added_count,
            "removed_count": result.removed_count,
            "modified_count": result.modified_count,
            "diff_percentage": result.diff_percentage,
            "is_match": result.is_match,
            "columns_compared": result.columns_compared,
            "key_column": result.key_column,
            "differences": [
                {
                    "key": str(d.key),
                    "type": d.diff_type.value,
                    "source_hash": d.source_hash,
                    "target_hash": d.target_hash,
                }
                for d in result.differences[:100]  # Limit to first 100 for JSON
            ],
        },
    )

    if result.threshold > 0:
        output.threshold = {
            "value": result.threshold,
            "percentage": result.threshold * 100,
            "is_within_threshold": result.is_within_threshold,
        }

    return asdict(output)


def format_schema_result_json(
    result: SchemaComparisonResult,
    source_table: str,
    target_table: str,
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    """Format SchemaComparisonResult as JSON-serializable dictionary.

    Args:
        result: SchemaComparisonResult to format
        source_table: Source table name
        target_table: Target table name
        duration_seconds: Optional execution duration

    Returns:
        Dictionary suitable for JSON serialization
    """
    # Determine status
    if result.is_identical:
        status = "identical"
        exit_code = 0
    elif result.is_compatible:
        status = "compatible"
        exit_code = 0
    else:
        status = "incompatible"
        exit_code = 1

    output = JSONSchemaOutput(
        status=status,
        exit_code=exit_code,
        meta=JSONOutputMeta(
            version=get_version(),
            duration_seconds=duration_seconds,
        ),
        source={
            "table": source_table,
            "columns": [{"name": c.name, "type": c.data_type, "nullable": c.nullable} for c in result.source_columns],
        },
        target={
            "table": target_table,
            "columns": [{"name": c.name, "type": c.data_type, "nullable": c.nullable} for c in result.target_columns],
        },
        comparison={
            "is_identical": result.is_identical,
            "is_compatible": result.is_compatible,
            "matching_columns": result.matching_columns,
            "source_only_columns": result.source_only_columns,
            "target_only_columns": result.target_only_columns,
            "type_mismatches": result.type_mismatches,
        },
    )

    return asdict(output)


def format_attach_result_json(
    alias: str,
    path: str,
    tables: list[str],
    duration_seconds: float | None = None,
) -> dict[str, Any]:
    """Format attach command result as JSON.

    Args:
        alias: Database alias
        path: Database path
        tables: List of table names
        duration_seconds: Optional execution duration

    Returns:
        Dictionary suitable for JSON serialization
    """
    output = JSONAttachOutput(
        status="success",
        exit_code=0,
        meta=JSONOutputMeta(
            version=get_version(),
            duration_seconds=duration_seconds,
        ),
        database={
            "alias": alias,
            "path": path,
        },
        tables=tables,
    )

    return asdict(output)


def format_error_json(
    error_type: str,
    message: str,
    details: str | None = None,
    recovery_suggestion: str | None = None,
    exit_code: int = 2,
) -> dict[str, Any]:
    """Format an error as JSON.

    Args:
        error_type: Type of error (e.g., "TableNotFoundError")
        message: Error message
        details: Optional detailed error information
        recovery_suggestion: Optional suggestion for fixing the error
        exit_code: Exit code to use

    Returns:
        Dictionary suitable for JSON serialization
    """
    output = JSONErrorOutput(
        exit_code=exit_code,
        meta=JSONOutputMeta(version=get_version()),
        error={
            "type": error_type,
            "message": message,
            "details": details,
            "recovery_suggestion": recovery_suggestion,
        },
    )

    return asdict(output)


def print_json(data: dict[str, Any], file: Any = None) -> None:
    """Print JSON output to stdout or specified file.

    Args:
        data: Dictionary to serialize as JSON
        file: Optional file handle (defaults to stdout)
    """
    output = json.dumps(data, indent=2, default=str)
    print(output, file=file or sys.stdout)
