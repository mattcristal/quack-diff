"""Error handling utilities with recovery suggestions for CLI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


@dataclass
class ErrorInfo:
    """Information about an error with recovery suggestion."""

    error_type: str
    message: str
    details: str | None = None
    recovery_suggestion: str | None = None


# Mapping of error types to recovery suggestions
ERROR_RECOVERY_SUGGESTIONS: dict[str, str] = {
    # Table errors
    "TableNotFoundError": (
        "Check that the table name is correct and fully qualified (database.schema.table). "
        "Verify the table exists using `SHOW TABLES` or your database's catalog."
    ),
    # Key column errors
    "KeyColumnError": (
        "Ensure the key column exists in both tables. "
        "Use `--verbose` to see available columns, or run `quack-diff schema` to compare schemas first."
    ),
    # Schema errors
    "SchemaError": (
        "Tables may have incompatible schemas. Run `quack-diff schema --source <source> --target <target>` "
        "to see detailed schema differences. Consider using `--columns` to compare specific columns."
    ),
    # Attach errors
    "AttachError": (
        "Verify the database file path is correct and accessible. "
        "For DuckDB files, ensure the file exists and is not corrupted. "
        "Check file permissions if access is denied."
    ),
    # Query errors
    "QueryExecutionError": (
        "The SQL query failed to execute. This may be due to syntax errors, missing permissions, "
        "or database-specific issues. Use `--verbose` for more details."
    ),
    # SQL injection
    "SQLInjectionError": (
        "Input contains potentially unsafe characters. "
        "Table and column names should only contain alphanumeric characters, underscores, and dots."
    ),
    # Connection errors
    "ConnectionError": (
        "Unable to connect to the database. Verify connection credentials and network access. "
        "For Snowflake, check that the account identifier, warehouse, and role are correct."
    ),
    # Authentication errors
    "AuthenticationError": (
        "Authentication failed. Check your credentials and ensure they are correctly configured. "
        "For Snowflake, verify your connection profile in ~/.snowflake/connections.toml or environment variables."
    ),
    # Import errors
    "ImportError": (
        "Required optional dependency is not installed. For Snowflake support, run: pip install 'quack-diff[snowflake]'"
    ),
    # Generic database error
    "DatabaseError": (
        "A database error occurred. Check the error details for more information. "
        "Try running with `--verbose` for additional debugging output."
    ),
    # Value errors
    "ValueError": (
        "Invalid input value. Check that all provided arguments are in the correct format. "
        "Use `--help` to see expected argument formats."
    ),
    # File not found
    "FileNotFoundError": (
        "The specified file does not exist. Verify the file path is correct "
        "and the file is accessible from the current directory."
    ),
    # Permission errors
    "PermissionError": (
        "Permission denied when accessing the file or resource. Check file permissions and ensure you have read access."
    ),
    # Timeout errors
    "TimeoutError": (
        "The operation timed out. This may be due to large data volumes or slow network. "
        "Consider using `--limit` to reduce the result set, or check your database connection."
    ),
}


def get_recovery_suggestion(error_type: str) -> str | None:
    """Get a recovery suggestion for an error type.

    Args:
        error_type: The error class name (e.g., "TableNotFoundError")

    Returns:
        Recovery suggestion string or None if not found
    """
    return ERROR_RECOVERY_SUGGESTIONS.get(error_type)


def get_error_info(
    exception: Exception,
    default_message: str | None = None,
) -> ErrorInfo:
    """Extract error information from an exception with recovery suggestion.

    Args:
        exception: The exception to extract info from
        default_message: Optional default message if exception has none

    Returns:
        ErrorInfo with type, message, details, and recovery suggestion
    """
    error_type = type(exception).__name__

    # Extract message
    message = str(exception) or default_message or "An error occurred"

    # Extract details if available
    details = None
    if hasattr(exception, "details"):
        details = exception.details
    elif hasattr(exception, "__cause__") and exception.__cause__:
        details = str(exception.__cause__)

    # Get recovery suggestion
    recovery_suggestion = get_recovery_suggestion(error_type)

    return ErrorInfo(
        error_type=error_type,
        message=message,
        details=details,
        recovery_suggestion=recovery_suggestion,
    )


def format_error_with_suggestion(
    error_info: ErrorInfo,
    show_details: bool = False,
) -> str:
    """Format error information with recovery suggestion for display.

    Args:
        error_info: ErrorInfo instance
        show_details: Whether to include technical details

    Returns:
        Formatted error string for Rich console
    """
    parts = [f"[error]{error_info.message}[/error]"]

    if show_details and error_info.details:
        parts.append(f"[dim]{error_info.details}[/dim]")

    if error_info.recovery_suggestion:
        parts.append(f"\n[info]Hint: {error_info.recovery_suggestion}[/info]")

    return "\n".join(parts)
