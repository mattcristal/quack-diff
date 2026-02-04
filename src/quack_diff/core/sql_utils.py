"""SQL utility functions for sanitization and security.

Provides functions to safely handle SQL identifiers and prevent SQL injection.
"""

from __future__ import annotations

import re
from typing import Any


class SQLInjectionError(ValueError):
    """Raised when a potential SQL injection attempt is detected."""

    pass


class DatabaseError(Exception):
    """Base exception for database-related errors.

    Provides a consistent interface for all database operation failures
    with descriptive error messages.
    """

    def __init__(self, message: str, operation: str | None = None, details: str | None = None) -> None:
        """Initialize the database error.

        Args:
            message: Human-readable error message
            operation: The operation that failed (e.g., "ATTACH", "DESCRIBE")
            details: Additional technical details about the error
        """
        self.message = message
        self.operation = operation
        self.details = details
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the full error message."""
        parts = [self.message]
        if self.operation:
            parts.insert(0, f"[{self.operation}]")
        if self.details:
            parts.append(f"Details: {self.details}")
        return " ".join(parts)


class AttachError(DatabaseError):
    """Raised when attaching a database fails.

    Common causes:
    - File not found
    - Permission denied
    - Invalid/corrupted database file
    - Database already attached with different alias
    """

    def __init__(
        self, message: str, path: str | None = None, alias: str | None = None, details: str | None = None
    ) -> None:
        """Initialize the attach error.

        Args:
            message: Human-readable error message
            path: Path to the database file that failed to attach
            alias: Alias/name used for the attachment
            details: Additional technical details
        """
        self.path = path
        self.alias = alias
        super().__init__(message, operation="ATTACH", details=details)


class TableNotFoundError(DatabaseError):
    """Raised when a table does not exist.

    This error occurs when attempting to:
    - DESCRIBE a non-existent table
    - Query a table that doesn't exist
    - Compare tables where one or both are missing
    """

    def __init__(self, table: str, message: str | None = None, details: str | None = None) -> None:
        """Initialize the table not found error.

        Args:
            table: The table name that was not found
            message: Optional custom message (default: auto-generated)
            details: Additional technical details
        """
        self.table = table
        if message is None:
            message = f"Table '{table}' not found"
        super().__init__(message, operation="TABLE_LOOKUP", details=details)


class SchemaError(DatabaseError):
    """Raised when schema retrieval or validation fails.

    This error occurs when:
    - Unable to retrieve table schema
    - Schema is empty or invalid
    - Schema comparison fails
    """

    def __init__(self, message: str, table: str | None = None, details: str | None = None) -> None:
        """Initialize the schema error.

        Args:
            message: Human-readable error message
            table: The table whose schema caused the error
            details: Additional technical details
        """
        self.table = table
        super().__init__(message, operation="SCHEMA", details=details)


class KeyColumnError(DatabaseError):
    """Raised when the key column validation fails.

    This error occurs when:
    - Key column does not exist in source table
    - Key column does not exist in target table
    - Key column types are incompatible between tables
    """

    def __init__(
        self,
        key_column: str,
        message: str | None = None,
        source_table: str | None = None,
        target_table: str | None = None,
        details: str | None = None,
    ) -> None:
        """Initialize the key column error.

        Args:
            key_column: The key column that failed validation
            message: Optional custom message (default: auto-generated)
            source_table: Source table name
            target_table: Target table name
            details: Additional technical details
        """
        self.key_column = key_column
        self.source_table = source_table
        self.target_table = target_table
        if message is None:
            message = f"Key column '{key_column}' validation failed"
        super().__init__(message, operation="KEY_VALIDATION", details=details)


class QueryExecutionError(DatabaseError):
    """Raised when a SQL query execution fails.

    This error wraps underlying database errors with context about
    what operation was being performed.
    """

    def __init__(self, message: str, query: str | None = None, details: str | None = None) -> None:
        """Initialize the query execution error.

        Args:
            message: Human-readable error message
            query: The SQL query that failed (truncated for display)
            details: Additional technical details
        """
        self.query = query[:200] + "..." if query and len(query) > 200 else query
        super().__init__(message, operation="QUERY", details=details)


def sanitize_identifier(identifier: str, max_length: int = 255) -> str:
    """Sanitize a SQL identifier (table name, column name, database name, etc.).

    This function validates that an identifier contains only safe characters
    and does not contain SQL injection patterns.

    Args:
        identifier: The identifier to sanitize
        max_length: Maximum allowed length for the identifier

    Returns:
        The validated identifier

    Raises:
        SQLInjectionError: If the identifier contains unsafe characters or patterns
        ValueError: If the identifier is empty or too long

    Example:
        >>> sanitize_identifier("users")
        'users'
        >>> sanitize_identifier("my_schema.my_table")
        'my_schema.my_table'
        >>> sanitize_identifier("table'; DROP TABLE users--")
        Traceback (most recent call last):
            ...
        SQLInjectionError: Invalid identifier: contains SQL injection pattern
    """
    if not identifier:
        raise ValueError("Identifier cannot be empty")

    if len(identifier) > max_length:
        raise ValueError(f"Identifier too long: {len(identifier)} > {max_length}")

    # Remove leading/trailing whitespace
    identifier = identifier.strip()

    # Check if empty after stripping
    if not identifier:
        raise ValueError("Identifier cannot be empty")

    # Check for SQL injection patterns
    # Disallow: semicolons, comments (-- and /**/), quotes in suspicious contexts
    dangerous_patterns = [
        r";",  # Statement separator
        r"--",  # SQL comment
        r"/\*",  # Multi-line comment start
        r"\*/",  # Multi-line comment end
        r"\bOR\b.*=.*=",  # OR injection pattern
        r"\bAND\b.*=.*=",  # AND injection pattern
        r"\bUNION\b",  # UNION injection
        r"\bSELECT\b.*\bFROM\b",  # Nested SELECT
        r"\bDROP\b",  # DROP statement
        r"\bDELETE\b",  # DELETE statement
        r"\bINSERT\b",  # INSERT statement
        r"\bUPDATE\b",  # UPDATE statement
        r"\bEXEC\b",  # EXEC statement
        r"\bCREATE\b",  # CREATE statement (except in controlled contexts)
        r"xp_",  # SQL Server extended procedures
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, identifier, re.IGNORECASE):
            raise SQLInjectionError(f"Invalid identifier '{identifier}': contains SQL injection pattern")

    # Valid identifier characters: alphanumeric, underscore, dot (for qualified names),
    # hyphen (for some databases), and backticks/quotes (for quoted identifiers)
    # We allow dots for qualified names like "schema.table" or "db.schema.table"
    if not re.match(r'^[a-zA-Z0-9_.\-`"]+$', identifier):
        raise SQLInjectionError(
            f"Invalid identifier '{identifier}': contains unsafe characters. "
            "Only alphanumeric, underscore, dot, and hyphen are allowed."
        )

    # Validate structure for dotted identifiers (e.g., schema.table)
    # Each part should be a valid identifier
    parts = identifier.split(".")
    if len(parts) > 3:  # db.schema.table is max
        raise SQLInjectionError(f"Invalid identifier '{identifier}': too many parts (max 3: db.schema.table)")

    for part in parts:
        # Remove quotes/backticks for validation
        clean_part = part.strip("`\"'")
        if not clean_part:
            raise SQLInjectionError(f"Invalid identifier '{identifier}': empty part")
        # Each part should start with letter or underscore
        if not re.match(r"^[a-zA-Z_]", clean_part):
            raise SQLInjectionError(f"Invalid identifier part '{part}': must start with letter or underscore")

    return identifier


def quote_identifier(identifier: str) -> str:
    """Quote a SQL identifier for safe use in queries.

    This function first sanitizes the identifier, then wraps it in double quotes
    to ensure it's treated as an identifier even if it contains reserved words.

    Args:
        identifier: The identifier to quote

    Returns:
        The quoted identifier

    Raises:
        SQLInjectionError: If the identifier is unsafe

    Example:
        >>> quote_identifier("table")
        '"table"'
        >>> quote_identifier("my_schema.my_table")
        '"my_schema"."my_table"'
    """
    # Remove existing quotes before sanitizing
    # Handle qualified names (schema.table or db.schema.table)
    parts = identifier.split(".")
    clean_parts = []

    for part in parts:
        # Remove existing quotes if present
        clean_part = part.strip("`\"'")
        clean_parts.append(clean_part)

    # Rejoin and sanitize
    clean_identifier = ".".join(clean_parts)
    identifier = sanitize_identifier(clean_identifier)

    # Now split again for quoting
    parts = identifier.split(".")
    quoted_parts = [f'"{part}"' for part in parts]

    return ".".join(quoted_parts)


def sanitize_path(path: str) -> str:
    """Sanitize a file path for use in SQL ATTACH statements.

    Validates that the path does not contain SQL injection attempts
    while allowing normal file path characters.

    Args:
        path: The file path to sanitize

    Returns:
        The validated path

    Raises:
        SQLInjectionError: If the path contains unsafe patterns
        ValueError: If the path is empty

    Example:
        >>> sanitize_path("/path/to/database.duckdb")
        '/path/to/database.duckdb'
        >>> sanitize_path("./data/mydb.duckdb")
        './data/mydb.duckdb'
    """
    if not path:
        raise ValueError("Path cannot be empty")

    # Check for SQL injection patterns
    dangerous_patterns = [
        r";",  # Statement separator
        r"--",  # SQL comment
        r"/\*",  # Multi-line comment
        r"\*/",
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, path):
            raise SQLInjectionError(f"Invalid path '{path}': contains SQL injection pattern")

    # Paths can contain: alphanumeric, underscore, dot, slash, hyphen, colon (Windows),
    # space, parentheses, and other common file system characters
    if not re.match(r"^[a-zA-Z0-9_.\-/:~()[\] ]+$", path):
        raise SQLInjectionError(
            f"Invalid path '{path}': contains unsafe characters. Only standard file path characters are allowed."
        )

    return path


def build_parameterized_in_clause(values: list[Any]) -> tuple[str, list[Any]]:
    """Build a parameterized IN clause for safe SQL queries.

    Args:
        values: List of values for the IN clause

    Returns:
        Tuple of (placeholders_string, values_list) for use with parameterized queries

    Example:
        >>> placeholders, params = build_parameterized_in_clause([1, 2, 3])
        >>> placeholders
        '?, ?, ?'
        >>> params
        [1, 2, 3]
    """
    if not values:
        raise ValueError("Cannot create IN clause with empty values list")

    placeholders = ", ".join("?" * len(values))
    return placeholders, list(values)


def validate_limit(limit: int | None) -> int | None:
    """Validate a LIMIT value for SQL queries.

    Args:
        limit: The limit value to validate

    Returns:
        The validated limit value

    Raises:
        ValueError: If the limit is invalid

    Example:
        >>> validate_limit(10)
        10
        >>> validate_limit(None)
        >>> validate_limit(-1)
        Traceback (most recent call last):
            ...
        ValueError: LIMIT must be a positive integer
    """
    if limit is None:
        return None

    if not isinstance(limit, int) or limit <= 0:
        raise ValueError("LIMIT must be a positive integer")

    return limit


def escape_like_pattern(pattern: str) -> str:
    """Escape special characters in a LIKE pattern.

    Args:
        pattern: The pattern to escape

    Returns:
        The escaped pattern

    Example:
        >>> escape_like_pattern("test_pattern%")
        'test\\_pattern\\%'
    """
    # Escape special LIKE characters
    pattern = pattern.replace("\\", "\\\\")
    pattern = pattern.replace("%", "\\%")
    pattern = pattern.replace("_", "\\_")
    return pattern
