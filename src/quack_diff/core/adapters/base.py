"""Base adapter class defining the interface for SQL dialect handling.

Each database has slightly different SQL syntax for operations like:
- NULL handling in concatenation
- Type casting
- Time-travel queries
- Hash functions
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


class Dialect(str, Enum):
    """Supported SQL dialects."""

    DUCKDB = "duckdb"
    SNOWFLAKE = "snowflake"


class BaseAdapter(ABC):
    """Abstract base class for SQL dialect adapters.

    Adapters handle the generation of dialect-specific SQL syntax
    for operations like hashing, null handling, and time-travel queries.
    """

    @property
    @abstractmethod
    def dialect(self) -> Dialect:
        """Return the dialect this adapter handles."""
        ...

    @property
    @abstractmethod
    def supports_time_travel(self) -> bool:
        """Whether this dialect supports time-travel queries."""
        ...

    @abstractmethod
    def cast_to_varchar(self, column: str) -> str:
        """Generate SQL to cast a column to VARCHAR.

        Args:
            column: Column name or expression

        Returns:
            SQL expression that casts to VARCHAR
        """
        ...

    @abstractmethod
    def coalesce_null(self, expression: str, sentinel: str = "<NULL>") -> str:
        """Generate SQL to replace NULL with a sentinel value.

        This is critical for hash correctness - NULL values must be
        distinguishable from empty strings in the hash.

        Args:
            expression: SQL expression to coalesce
            sentinel: Value to use in place of NULL

        Returns:
            SQL COALESCE expression
        """
        ...

    @abstractmethod
    def concat_with_separator(self, expressions: list[str], separator: str = "|#|") -> str:
        """Generate SQL to concatenate expressions with a separator.

        Args:
            expressions: List of SQL expressions to concatenate
            separator: String to place between values

        Returns:
            SQL concatenation expression
        """
        ...

    @abstractmethod
    def md5_hash(self, expression: str) -> str:
        """Generate SQL to compute MD5 hash of an expression.

        Args:
            expression: SQL expression to hash

        Returns:
            SQL MD5 hash expression
        """
        ...

    def row_hash_expression(
        self,
        columns: list[str],
        separator: str = "|#|",
        null_sentinel: str = "<NULL>",
    ) -> str:
        """Generate a complete row hash expression.

        This combines casting, null handling, concatenation, and hashing
        into a single expression that can be used to compute a unique
        hash for each row.

        Args:
            columns: List of column names to include in hash
            separator: Delimiter between column values
            null_sentinel: Value to represent NULLs

        Returns:
            SQL expression that computes the row hash
        """
        # Build safe expressions for each column:
        # 1. Cast to VARCHAR for consistent representation
        # 2. Coalesce NULL to sentinel value
        safe_expressions = []
        for col in columns:
            cast_expr = self.cast_to_varchar(col)
            safe_expr = self.coalesce_null(cast_expr, null_sentinel)
            safe_expressions.append(safe_expr)

        # Concatenate all columns with separator
        concat_expr = self.concat_with_separator(safe_expressions, separator)

        # Return final hash
        return self.md5_hash(concat_expr)

    def time_travel_clause(self, timestamp: str | None = None, offset: str | None = None) -> str:
        """Generate SQL clause for time-travel queries.

        Args:
            timestamp: Specific timestamp to query at
            offset: Time offset (e.g., "5 minutes ago")

        Returns:
            SQL time-travel clause or empty string if not supported

        Raises:
            NotImplementedError: If dialect doesn't support time-travel
        """
        if not self.supports_time_travel:
            raise NotImplementedError(f"{self.dialect.value} does not support time-travel queries")
        return ""

    def wrap_table_with_time_travel(
        self,
        table: str,
        timestamp: str | None = None,
        offset: str | None = None,
    ) -> str:
        """Wrap a table reference with time-travel syntax.

        Args:
            table: Table name or reference
            timestamp: Specific timestamp
            offset: Time offset

        Returns:
            Table reference with time-travel clause
        """
        if timestamp is None and offset is None:
            return table

        clause = self.time_travel_clause(timestamp=timestamp, offset=offset)
        return f"{table} {clause}".strip()


def get_adapter(dialect: Dialect | str) -> BaseAdapter:
    """Factory function to get the appropriate adapter for a dialect.

    Args:
        dialect: Dialect enum or string name

    Returns:
        BaseAdapter subclass instance

    Raises:
        ValueError: If dialect is not supported
    """
    # Import here to avoid circular imports
    from quack_diff.core.adapters.duckdb import DuckDBAdapter
    from quack_diff.core.adapters.snowflake import SnowflakeAdapter

    if isinstance(dialect, str):
        dialect = Dialect(dialect.lower())

    adapters: dict[Dialect, type[BaseAdapter]] = {
        Dialect.DUCKDB: DuckDBAdapter,
        Dialect.SNOWFLAKE: SnowflakeAdapter,
    }

    adapter_class = adapters.get(dialect)
    if adapter_class is None:
        raise ValueError(f"Unsupported dialect: {dialect}")

    return adapter_class()
