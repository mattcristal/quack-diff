"""Query builder for generating dialect-safe SQL queries.

Handles the generation of:
- Row hash queries for comparison
- Count queries
- Schema introspection queries
- Time-travel wrapped queries
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from quack_diff.core.adapters.base import BaseAdapter, Dialect, get_adapter
from quack_diff.core.sql_utils import sanitize_identifier

if TYPE_CHECKING:
    pass


@dataclass
class TableReference:
    """Represents a reference to a database table.

    Attributes:
        name: Fully qualified table name (e.g., "db.schema.table")
        dialect: SQL dialect for this table
        timestamp: Optional timestamp for time-travel
        offset: Optional offset for time-travel (e.g., "5 minutes ago")
        columns: Optional list of columns to include (None = all)
    """

    name: str
    dialect: Dialect | str = Dialect.DUCKDB
    timestamp: str | None = None
    offset: str | None = None
    columns: list[str] | None = None
    key_column: str | None = None

    def __post_init__(self) -> None:
        """Normalize dialect to enum."""
        if isinstance(self.dialect, str):
            self.dialect = Dialect(self.dialect.lower())


@dataclass
class QueryBuilder:
    """Builds SQL queries for data comparison operations.

    Handles dialect-specific SQL generation for operations like:
    - Row hashing with NULL safety
    - Count queries
    - Schema queries

    Example:
        >>> builder = QueryBuilder()
        >>> query = builder.build_hash_query(
        ...     table="mydb.users",
        ...     columns=["id", "name", "email"],
        ...     key_column="id",
        ...     dialect="snowflake"
        ... )
    """

    null_sentinel: str = "<NULL>"
    column_delimiter: str = "|#|"
    _adapters: dict[Dialect, BaseAdapter] = field(default_factory=dict)

    def get_adapter(self, dialect: Dialect | str) -> BaseAdapter:
        """Get or create an adapter for the given dialect.

        Args:
            dialect: SQL dialect

        Returns:
            BaseAdapter instance
        """
        if isinstance(dialect, str):
            dialect = Dialect(dialect.lower())

        if dialect not in self._adapters:
            self._adapters[dialect] = get_adapter(dialect)

        return self._adapters[dialect]

    def build_hash_query(
        self,
        table: str,
        columns: list[str],
        key_column: str,
        dialect: Dialect | str = Dialect.DUCKDB,
        timestamp: str | None = None,
        offset: str | None = None,
    ) -> str:
        """Build a query that computes row hashes for comparison.

        The generated query:
        1. Selects the key column for row identification
        2. Casts all columns to VARCHAR for consistent representation
        3. Coalesces NULLs to a sentinel value to prevent hash collisions
        4. Concatenates columns with a delimiter
        5. Computes MD5 hash of the concatenated value

        Args:
            table: Fully qualified table name
            columns: List of columns to include in hash
            key_column: Primary key column for row identification
            dialect: SQL dialect to use
            timestamp: Optional timestamp for time-travel
            offset: Optional offset for time-travel

        Returns:
            SQL query string

        Raises:
            SQLInjectionError: If table or column names contain unsafe characters
        """
        adapter = self.get_adapter(dialect)

        # Sanitize all identifiers
        sanitized_table = sanitize_identifier(table)
        sanitized_key = sanitize_identifier(key_column)
        sanitized_columns = [sanitize_identifier(col) for col in columns]

        # Build the row hash expression
        hash_expr = adapter.row_hash_expression(
            columns=sanitized_columns,
            separator=self.column_delimiter,
            null_sentinel=self.null_sentinel,
        )

        # Handle time-travel if specified
        table_ref = sanitized_table
        if timestamp is not None or offset is not None:
            table_ref = adapter.wrap_table_with_time_travel(
                table=sanitized_table,
                timestamp=timestamp,
                offset=offset,
            )

        return f"""SELECT
    {sanitized_key},
    {hash_expr} AS row_hash
FROM {table_ref}
ORDER BY {sanitized_key}"""

    def build_count_query(
        self,
        table: str,
        dialect: Dialect | str = Dialect.DUCKDB,
        timestamp: str | None = None,
        offset: str | None = None,
    ) -> str:
        """Build a simple count query.

        Args:
            table: Fully qualified table name
            dialect: SQL dialect
            timestamp: Optional timestamp for time-travel
            offset: Optional offset for time-travel

        Returns:
            SQL query string

        Raises:
            SQLInjectionError: If table name contains unsafe characters
        """
        adapter = self.get_adapter(dialect)

        # Sanitize table name
        sanitized_table = sanitize_identifier(table)

        table_ref = sanitized_table
        if timestamp is not None or offset is not None:
            table_ref = adapter.wrap_table_with_time_travel(
                table=sanitized_table,
                timestamp=timestamp,
                offset=offset,
            )

        return f"SELECT COUNT(*) AS row_count FROM {table_ref}"

    def build_distinct_count_query(
        self,
        table: str,
        key_column: str,
        dialect: Dialect | str = Dialect.DUCKDB,
        timestamp: str | None = None,
        offset: str | None = None,
    ) -> str:
        """Build a query that counts distinct values in a key column.

        Args:
            table: Fully qualified table name
            key_column: Column to count distinct values for
            dialect: SQL dialect
            timestamp: Optional timestamp for time-travel
            offset: Optional offset for time-travel

        Returns:
            SQL query string

        Raises:
            SQLInjectionError: If table or column name contains unsafe characters
        """
        adapter = self.get_adapter(dialect)

        sanitized_table = sanitize_identifier(table)
        sanitized_key = sanitize_identifier(key_column)

        table_ref = sanitized_table
        if timestamp is not None or offset is not None:
            table_ref = adapter.wrap_table_with_time_travel(
                table=sanitized_table,
                timestamp=timestamp,
                offset=offset,
            )

        return f"SELECT COUNT(DISTINCT {sanitized_key}) AS row_count FROM {table_ref}"

    def build_schema_query(
        self,
        table: str,
        dialect: Dialect | str = Dialect.DUCKDB,
    ) -> str:
        """Build a query to get table schema.

        For DuckDB (including attached databases), we use DESCRIBE.
        For direct database queries, we'd use information_schema.

        Args:
            table: Fully qualified table name
            dialect: SQL dialect

        Returns:
            SQL query string

        Raises:
            SQLInjectionError: If table name contains unsafe characters
        """
        # Sanitize table name
        sanitized_table = sanitize_identifier(table)

        # DuckDB's DESCRIBE works on attached tables too
        return f"DESCRIBE {sanitized_table}"

    def build_sample_query(
        self,
        table: str,
        columns: list[str],
        key_column: str,
        keys: list[str],
        dialect: Dialect | str = Dialect.DUCKDB,
        timestamp: str | None = None,
        offset: str | None = None,
    ) -> str:
        """Build a query to fetch specific rows by key.

        Used to retrieve the actual data for mismatched rows.

        Args:
            table: Fully qualified table name
            columns: Columns to select
            key_column: Primary key column
            keys: List of key values to fetch
            dialect: SQL dialect
            timestamp: Optional timestamp for time-travel
            offset: Optional offset for time-travel

        Returns:
            SQL query string

        Raises:
            SQLInjectionError: If table or column names contain unsafe characters

        Note:
            This method still uses string interpolation for key values in the IN clause.
            This is a known limitation. For production use, consider using parameterized
            queries or further validation of key values.
        """
        adapter = self.get_adapter(dialect)

        # Sanitize identifiers
        sanitized_table = sanitize_identifier(table)
        sanitized_key = sanitize_identifier(key_column)
        sanitized_columns = [sanitize_identifier(col) for col in columns]

        table_ref = sanitized_table
        if timestamp is not None or offset is not None:
            table_ref = adapter.wrap_table_with_time_travel(
                table=sanitized_table,
                timestamp=timestamp,
                offset=offset,
            )

        # Format keys for IN clause
        # TODO: This still uses string interpolation for values, which could be
        # a security risk if keys contain malicious content. Consider using
        # parameterized queries in the future.
        # For now, we escape single quotes to prevent basic injection
        escaped_keys = [str(k).replace("'", "''") for k in keys]
        formatted_keys = ", ".join(f"'{k}'" for k in escaped_keys)

        columns_str = ", ".join(sanitized_columns)

        return f"""SELECT {columns_str}
FROM {table_ref}
WHERE {sanitized_key} IN ({formatted_keys})
ORDER BY {sanitized_key}"""

    def build_aggregate_hash_query(
        self,
        table: str,
        columns: list[str],
        key_column: str,
        dialect: Dialect | str = Dialect.DUCKDB,
        timestamp: str | None = None,
        offset: str | None = None,
    ) -> str:
        """Build a query that computes an aggregate hash of all rows.

        This is useful for a quick "are these tables identical?" check
        before doing a more expensive row-by-row comparison.

        Args:
            table: Fully qualified table name
            columns: List of columns to include
            key_column: Primary key column for ordering
            dialect: SQL dialect
            timestamp: Optional timestamp for time-travel
            offset: Optional offset for time-travel

        Returns:
            SQL query string

        Raises:
            SQLInjectionError: If table or column names contain unsafe characters
        """
        adapter = self.get_adapter(dialect)

        # Sanitize identifiers
        sanitized_table = sanitize_identifier(table)
        sanitized_key = sanitize_identifier(key_column)
        sanitized_columns = [sanitize_identifier(col) for col in columns]

        # Build row hash expression
        hash_expr = adapter.row_hash_expression(
            columns=sanitized_columns,
            separator=self.column_delimiter,
            null_sentinel=self.null_sentinel,
        )

        table_ref = sanitized_table
        if timestamp is not None or offset is not None:
            table_ref = adapter.wrap_table_with_time_travel(
                table=sanitized_table,
                timestamp=timestamp,
                offset=offset,
            )

        # Create an aggregate hash by XORing or concatenating individual hashes
        # We use MD5 of the concatenated, ordered hashes
        return f"""SELECT MD5(STRING_AGG({hash_expr}, '' ORDER BY {sanitized_key})) AS table_hash
FROM {table_ref}"""

    def build_hash_comparison_query(
        self,
        source_table: str,
        target_table: str,
        columns: list[str],
        key_column: str,
        dialect: Dialect | str = Dialect.DUCKDB,
        source_timestamp: str | None = None,
        source_offset: str | None = None,
        target_timestamp: str | None = None,
        target_offset: str | None = None,
    ) -> str:
        """Build a query that finds rows with different hashes.

        Performs a FULL OUTER JOIN on the key column and compares hashes,
        returning rows that differ.

        Args:
            source_table: Source table reference
            target_table: Target table reference
            columns: Columns to include in hash
            key_column: Primary key column
            dialect: SQL dialect
            source_timestamp: Source time-travel timestamp
            source_offset: Source time-travel offset
            target_timestamp: Target time-travel timestamp
            target_offset: Target time-travel offset

        Returns:
            SQL query string

        Raises:
            SQLInjectionError: If table or column names contain unsafe characters
        """
        adapter = self.get_adapter(dialect)

        # Sanitize all identifiers
        sanitized_source = sanitize_identifier(source_table)
        sanitized_target = sanitize_identifier(target_table)
        sanitized_key = sanitize_identifier(key_column)
        sanitized_columns = [sanitize_identifier(col) for col in columns]

        hash_expr = adapter.row_hash_expression(
            columns=sanitized_columns,
            separator=self.column_delimiter,
            null_sentinel=self.null_sentinel,
        )

        # Handle time-travel for both tables
        source_ref = sanitized_source
        if source_timestamp is not None or source_offset is not None:
            source_ref = adapter.wrap_table_with_time_travel(
                table=sanitized_source,
                timestamp=source_timestamp,
                offset=source_offset,
            )

        target_ref = sanitized_target
        if target_timestamp is not None or target_offset is not None:
            target_ref = adapter.wrap_table_with_time_travel(
                table=sanitized_target,
                timestamp=target_timestamp,
                offset=target_offset,
            )

        return f"""WITH source_hashes AS (
    SELECT
        {sanitized_key},
        {hash_expr} AS row_hash
    FROM {source_ref}
),
target_hashes AS (
    SELECT
        {sanitized_key},
        {hash_expr} AS row_hash
    FROM {target_ref}
)
SELECT
    COALESCE(s.{sanitized_key}, t.{sanitized_key}) AS {sanitized_key},
    CASE
        WHEN s.{sanitized_key} IS NULL THEN 'added'
        WHEN t.{sanitized_key} IS NULL THEN 'removed'
        ELSE 'modified'
    END AS diff_type,
    s.row_hash AS source_hash,
    t.row_hash AS target_hash
FROM source_hashes s
FULL OUTER JOIN target_hashes t ON s.{sanitized_key} = t.{sanitized_key}
WHERE s.row_hash IS DISTINCT FROM t.row_hash
ORDER BY COALESCE(s.{sanitized_key}, t.{sanitized_key})"""
