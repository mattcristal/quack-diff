"""Data comparison engine.

The DataDiffer class orchestrates the comparison of data between
two tables, using the connector for database access and the query
builder for SQL generation.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from quack_diff.core.adapters.base import Dialect
from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.query_builder import QueryBuilder
from quack_diff.core.sql_utils import (
    KeyColumnError,
    QueryExecutionError,
    SchemaError,
    TableNotFoundError,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DiffType(str, Enum):
    """Types of differences found between rows."""

    ADDED = "added"  # Row exists in target but not source
    REMOVED = "removed"  # Row exists in source but not target
    MODIFIED = "modified"  # Row exists in both but values differ


@dataclass
class ColumnInfo:
    """Information about a table column."""

    name: str
    data_type: str
    nullable: bool = True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ColumnInfo):
            return NotImplemented
        # Compare name and type, ignore nullability for basic comparison
        return self.name.lower() == other.name.lower()

    def type_matches(self, other: ColumnInfo) -> bool:
        """Check if column types are compatible."""
        # Normalize common type variations
        self_type = self._normalize_type(self.data_type)
        other_type = self._normalize_type(other.data_type)
        return self_type == other_type

    @staticmethod
    def _normalize_type(dtype: str) -> str:
        """Normalize type names for comparison."""
        dtype = dtype.upper()
        # Map common variations
        mappings = {
            "INT": "INTEGER",
            "INT4": "INTEGER",
            "INT8": "BIGINT",
            "FLOAT8": "DOUBLE",
            "FLOAT4": "FLOAT",
            "BOOL": "BOOLEAN",
            "STRING": "VARCHAR",
            "TEXT": "VARCHAR",
        }
        for pattern, replacement in mappings.items():
            if dtype.startswith(pattern):
                return replacement
        return dtype.split("(")[0]  # Remove precision/scale


@dataclass
class SchemaComparisonResult:
    """Result of comparing two table schemas."""

    source_columns: list[ColumnInfo]
    target_columns: list[ColumnInfo]
    matching_columns: list[str] = field(default_factory=list)
    source_only_columns: list[str] = field(default_factory=list)
    target_only_columns: list[str] = field(default_factory=list)
    type_mismatches: dict[str, tuple[str, str]] = field(default_factory=dict)

    @property
    def is_compatible(self) -> bool:
        """Check if schemas are compatible for comparison."""
        return len(self.matching_columns) > 0

    @property
    def is_identical(self) -> bool:
        """Check if schemas are identical."""
        return (
            len(self.source_only_columns) == 0 and len(self.target_only_columns) == 0 and len(self.type_mismatches) == 0
        )


@dataclass
class RowDiff:
    """Represents a difference in a single row."""

    key: Any
    diff_type: DiffType
    source_hash: str | None = None
    target_hash: str | None = None
    source_values: dict[str, Any] | None = None
    target_values: dict[str, Any] | None = None


@dataclass
class DiffResult:
    """Complete result of a data comparison operation."""

    source_table: str
    target_table: str
    source_row_count: int
    target_row_count: int
    schema_comparison: SchemaComparisonResult
    differences: list[RowDiff] = field(default_factory=list)
    threshold: float = 0.0
    columns_compared: list[str] = field(default_factory=list)
    key_column: str = ""

    @property
    def total_differences(self) -> int:
        """Total number of row differences."""
        return len(self.differences)

    @property
    def added_count(self) -> int:
        """Number of rows added in target."""
        return sum(1 for d in self.differences if d.diff_type == DiffType.ADDED)

    @property
    def removed_count(self) -> int:
        """Number of rows removed from source."""
        return sum(1 for d in self.differences if d.diff_type == DiffType.REMOVED)

    @property
    def modified_count(self) -> int:
        """Number of rows modified."""
        return sum(1 for d in self.differences if d.diff_type == DiffType.MODIFIED)

    @property
    def diff_percentage(self) -> float:
        """Percentage of rows that differ."""
        total = max(self.source_row_count, self.target_row_count)
        if total == 0:
            return 0.0
        return (self.total_differences / total) * 100

    @property
    def is_within_threshold(self) -> bool:
        """Check if differences are within acceptable threshold."""
        return (self.diff_percentage / 100) <= self.threshold

    @property
    def is_match(self) -> bool:
        """Check if tables match (no differences)."""
        return self.total_differences == 0


@dataclass
class TableCount:
    """Row or distinct count for a single table."""

    table: str
    count: int


@dataclass
class CountResult:
    """Result of comparing row counts across multiple tables."""

    table_counts: list[TableCount]
    key_column: str | None = None  # None = COUNT(*), else COUNT(DISTINCT key)
    is_match: bool = False

    @property
    def mode(self) -> str:
        """Count mode: 'rows' for COUNT(*), 'distinct' for COUNT(DISTINCT key)."""
        return "distinct" if self.key_column else "rows"

    @property
    def expected_count(self) -> int | None:
        """Reference count (first table). None if no tables."""
        if not self.table_counts:
            return None
        return self.table_counts[0].count


class DataDiffer:
    """Compares data between two database tables.

    The DataDiffer uses DuckDB as the comparison engine, leveraging
    its ability to attach external databases and perform efficient
    hash-based comparisons.

    Example:
        >>> with DuckDBConnector() as conn:
        ...     differ = DataDiffer(conn)
        ...     result = differ.diff(
        ...         source_table="prod.users",
        ...         target_table="dev.users",
        ...         key_column="id"
        ...     )
        ...     print(f"Found {result.total_differences} differences")
    """

    def __init__(
        self,
        connector: DuckDBConnector,
        null_sentinel: str = "<NULL>",
        column_delimiter: str = "|#|",
    ) -> None:
        """Initialize the DataDiffer.

        Args:
            connector: DuckDB connector instance
            null_sentinel: Value to use for NULL in hashes
            column_delimiter: Delimiter between columns in hash
        """
        self.connector = connector
        self.query_builder = QueryBuilder(
            null_sentinel=null_sentinel,
            column_delimiter=column_delimiter,
        )

    def get_schema(
        self,
        table: str,
        dialect: Dialect | str = Dialect.DUCKDB,
    ) -> list[ColumnInfo]:
        """Get the schema of a table.

        Args:
            table: Fully qualified table name
            dialect: SQL dialect

        Returns:
            List of ColumnInfo objects

        Raises:
            TableNotFoundError: If the table does not exist
            SchemaError: If schema retrieval fails or returns empty
        """
        query = self.query_builder.build_schema_query(table, dialect)
        try:
            result = self.connector.execute_fetchall(query)
        except TableNotFoundError as e:
            raise TableNotFoundError(
                table=table,
                message=f"Cannot retrieve schema: table '{table}' does not exist",
                details="Verify the table name, schema, and database are correct",
            ) from e
        except QueryExecutionError as e:
            raise SchemaError(
                f"Failed to retrieve schema for table '{table}'",
                table=table,
                details=e.details,
            ) from e

        if not result:
            raise SchemaError(
                f"Table '{table}' has no columns or schema is empty",
                table=table,
                details="The table may be corrupted or have an invalid structure",
            )

        columns = []
        for row in result:
            # DESCRIBE returns: column_name, column_type, null, key, default, extra
            col_name = row[0]
            col_type = row[1]
            nullable = row[2] == "YES" if len(row) > 2 else True
            columns.append(ColumnInfo(name=col_name, data_type=col_type, nullable=nullable))

        return columns

    def compare_schemas(
        self,
        source_table: str,
        target_table: str,
        source_dialect: Dialect | str = Dialect.DUCKDB,
        target_dialect: Dialect | str = Dialect.DUCKDB,
    ) -> SchemaComparisonResult:
        """Compare schemas of two tables.

        Args:
            source_table: Source table name
            target_table: Target table name
            source_dialect: Source SQL dialect
            target_dialect: Target SQL dialect

        Returns:
            SchemaComparisonResult with comparison details

        Raises:
            TableNotFoundError: If either table does not exist
            SchemaError: If schema retrieval fails for either table
        """
        # Get source schema with clear error context
        try:
            source_cols = self.get_schema(source_table, source_dialect)
        except TableNotFoundError as e:
            raise TableNotFoundError(
                table=source_table,
                message=f"Source table '{source_table}' does not exist",
                details=e.details,
            ) from e
        except SchemaError as e:
            raise SchemaError(
                f"Failed to retrieve schema for source table '{source_table}'",
                table=source_table,
                details=e.details,
            ) from e

        # Get target schema with clear error context
        try:
            target_cols = self.get_schema(target_table, target_dialect)
        except TableNotFoundError as e:
            raise TableNotFoundError(
                table=target_table,
                message=f"Target table '{target_table}' does not exist",
                details=e.details,
            ) from e
        except SchemaError as e:
            raise SchemaError(
                f"Failed to retrieve schema for target table '{target_table}'",
                table=target_table,
                details=e.details,
            ) from e

        source_names = {c.name.lower(): c for c in source_cols}
        target_names = {c.name.lower(): c for c in target_cols}

        matching = []
        source_only = []
        target_only = []
        type_mismatches = {}

        # Find matching and source-only columns
        for name, col in source_names.items():
            if name in target_names:
                matching.append(col.name)
                target_col = target_names[name]
                if not col.type_matches(target_col):
                    type_mismatches[col.name] = (col.data_type, target_col.data_type)
            else:
                source_only.append(col.name)

        # Find target-only columns
        for name, col in target_names.items():
            if name not in source_names:
                target_only.append(col.name)

        return SchemaComparisonResult(
            source_columns=source_cols,
            target_columns=target_cols,
            matching_columns=matching,
            source_only_columns=source_only,
            target_only_columns=target_only,
            type_mismatches=type_mismatches,
        )

    def get_row_count(
        self,
        table: str,
        dialect: Dialect | str = Dialect.DUCKDB,
        timestamp: str | None = None,
        offset: str | None = None,
    ) -> int:
        """Get the row count of a table.

        Args:
            table: Fully qualified table name
            dialect: SQL dialect
            timestamp: Optional time-travel timestamp
            offset: Optional time-travel offset

        Returns:
            Number of rows

        Raises:
            TableNotFoundError: If the table does not exist
            QueryExecutionError: If the count query fails
        """
        query = self.query_builder.build_count_query(
            table=table,
            dialect=dialect,
            timestamp=timestamp,
            offset=offset,
        )
        try:
            result = self.connector.execute_fetchone(query)
            return result[0] if result else 0
        except TableNotFoundError as e:
            raise TableNotFoundError(
                table=table,
                message=f"Cannot count rows: table '{table}' does not exist",
                details="Verify the table name, schema, and database are correct",
            ) from e

    def count_check(
        self,
        tables: list[str],
        key_column: str | None = None,
        dialect: Dialect | str = Dialect.DUCKDB,
    ) -> CountResult:
        """Check that all tables have the same row or distinct-key count.

        Args:
            tables: List of fully qualified table names
            key_column: If set, use COUNT(DISTINCT key_column); else COUNT(*)
            dialect: SQL dialect for all tables

        Returns:
            CountResult with per-table counts and is_match

        Raises:
            TableNotFoundError: If any table does not exist
            QueryExecutionError: If any count query fails
        """
        if not tables:
            return CountResult(table_counts=[], key_column=key_column, is_match=True)

        table_counts: list[TableCount] = []
        for table in tables:
            if key_column:
                query = self.query_builder.build_distinct_count_query(
                    table=table,
                    key_column=key_column,
                    dialect=dialect,
                )
            else:
                query = self.query_builder.build_count_query(
                    table=table,
                    dialect=dialect,
                )
            try:
                row = self.connector.execute_fetchone(query)
                count = row[0] if row else 0
            except TableNotFoundError as e:
                raise TableNotFoundError(
                    table=table,
                    message=f"Cannot count: table '{table}' does not exist",
                    details="Verify the table name, schema, and database are correct",
                ) from e
            table_counts.append(TableCount(table=table, count=count))

        reference = table_counts[0].count
        is_match = all(tc.count == reference for tc in table_counts)
        return CountResult(
            table_counts=table_counts,
            key_column=key_column,
            is_match=is_match,
        )

    def _validate_key_column(
        self,
        key_column: str,
        schema_result: SchemaComparisonResult,
        source_table: str,
        target_table: str,
    ) -> None:
        """Validate that the key column exists in both tables and has compatible types.

        Args:
            key_column: The key column to validate
            schema_result: Schema comparison result
            source_table: Source table name
            target_table: Target table name

        Raises:
            KeyColumnError: If key column validation fails
        """
        source_cols = {c.name.lower(): c for c in schema_result.source_columns}
        target_cols = {c.name.lower(): c for c in schema_result.target_columns}
        key_lower = key_column.lower()

        # Check if key column exists in source table
        if key_lower not in source_cols:
            available_cols = [c.name for c in schema_result.source_columns]
            raise KeyColumnError(
                key_column=key_column,
                message=f"Key column '{key_column}' does not exist in source table '{source_table}'",
                source_table=source_table,
                target_table=target_table,
                details=f"Available columns: {', '.join(available_cols)}",
            )

        # Check if key column exists in target table
        if key_lower not in target_cols:
            available_cols = [c.name for c in schema_result.target_columns]
            raise KeyColumnError(
                key_column=key_column,
                message=f"Key column '{key_column}' does not exist in target table '{target_table}'",
                source_table=source_table,
                target_table=target_table,
                details=f"Available columns: {', '.join(available_cols)}",
            )

        # Check if key column types are compatible
        source_col = source_cols[key_lower]
        target_col = target_cols[key_lower]
        if not source_col.type_matches(target_col):
            raise KeyColumnError(
                key_column=key_column,
                message=f"Key column '{key_column}' has incompatible types between tables",
                source_table=source_table,
                target_table=target_table,
                details=f"Source type: {source_col.data_type}, Target type: {target_col.data_type}",
            )

    def diff(
        self,
        source_table: str,
        target_table: str,
        key_column: str,
        columns: list[str] | None = None,
        source_dialect: Dialect | str = Dialect.DUCKDB,
        target_dialect: Dialect | str = Dialect.DUCKDB,
        source_timestamp: str | None = None,
        source_offset: str | None = None,
        target_timestamp: str | None = None,
        target_offset: str | None = None,
        threshold: float = 0.0,
        limit: int | None = None,
    ) -> DiffResult:
        """Compare data between two tables.

        Performs a hash-based comparison to identify rows that differ
        between source and target tables.

        Args:
            source_table: Source table name
            target_table: Target table name
            key_column: Primary key column for row identification
            columns: Columns to compare (None = all common columns)
            source_dialect: Source SQL dialect
            target_dialect: Target SQL dialect
            source_timestamp: Source time-travel timestamp
            source_offset: Source time-travel offset
            target_timestamp: Target time-travel timestamp
            target_offset: Target time-travel offset
            threshold: Maximum acceptable difference ratio (0.0 = exact match)
            limit: Maximum number of differences to return

        Returns:
            DiffResult with comparison details

        Raises:
            TableNotFoundError: If either table does not exist
            SchemaError: If schema retrieval fails
            KeyColumnError: If key column validation fails
            QueryExecutionError: If comparison query fails
        """
        logger.info(f"Comparing {source_table} vs {target_table}")

        # Step 1: Compare schemas
        schema_result = self.compare_schemas(
            source_table=source_table,
            target_table=target_table,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
        )

        # Step 2: Validate key column BEFORE proceeding with comparison
        self._validate_key_column(key_column, schema_result, source_table, target_table)

        # Determine columns to compare
        if columns is None:
            columns = schema_result.matching_columns
        else:
            # Validate provided columns exist in both tables
            available = set(c.lower() for c in schema_result.matching_columns)
            columns = [c for c in columns if c.lower() in available]

        if not columns:
            raise SchemaError(
                "No common columns found between tables for comparison",
                details=f"Source columns: {[c.name for c in schema_result.source_columns]}, "
                f"Target columns: {[c.name for c in schema_result.target_columns]}",
            )

        # Ensure key column is in the list
        if key_column not in columns:
            columns = [key_column] + columns

        logger.debug(f"Comparing columns: {columns}")

        # Step 2: Get row counts
        source_count = self.get_row_count(source_table, source_dialect, source_timestamp, source_offset)
        target_count = self.get_row_count(target_table, target_dialect, target_timestamp, target_offset)

        logger.info(f"Source rows: {source_count}, Target rows: {target_count}")

        # Step 3: Find differences using hash comparison
        # For cross-database comparison, we use DuckDB's dialect since
        # attached databases appear as DuckDB tables
        query = self.query_builder.build_hash_comparison_query(
            source_table=source_table,
            target_table=target_table,
            columns=columns,
            key_column=key_column,
            dialect=Dialect.DUCKDB,  # Use DuckDB for the comparison itself
            source_timestamp=source_timestamp,
            source_offset=source_offset,
            target_timestamp=target_timestamp,
            target_offset=target_offset,
        )

        if limit:
            from quack_diff.core.sql_utils import validate_limit

            validated_limit = validate_limit(limit)
            if validated_limit is not None:
                query += f"\nLIMIT {validated_limit}"

        logger.debug("Executing comparison query")
        try:
            diff_rows = self.connector.execute_fetchall(query)
        except QueryExecutionError as e:
            raise QueryExecutionError(
                f"Failed to execute comparison query between '{source_table}' and '{target_table}'",
                query=query,
                details=e.details,
            ) from e

        # Parse differences
        differences = []
        for row in diff_rows:
            key_value, diff_type_str, source_hash, target_hash = row
            diff_type = DiffType(diff_type_str)
            differences.append(
                RowDiff(
                    key=key_value,
                    diff_type=diff_type,
                    source_hash=source_hash,
                    target_hash=target_hash,
                )
            )

        logger.info(f"Found {len(differences)} differences")

        return DiffResult(
            source_table=source_table,
            target_table=target_table,
            source_row_count=source_count,
            target_row_count=target_count,
            schema_comparison=schema_result,
            differences=differences,
            threshold=threshold,
            columns_compared=columns,
            key_column=key_column,
        )

    def quick_check(
        self,
        source_table: str,
        target_table: str,
        key_column: str,
        columns: list[str] | None = None,
        source_dialect: Dialect | str = Dialect.DUCKDB,
        target_dialect: Dialect | str = Dialect.DUCKDB,
    ) -> bool:
        """Quick check if two tables are identical.

        Computes an aggregate hash of both tables and compares them.
        This is much faster than a full diff for large tables that
        are expected to match.

        Args:
            source_table: Source table name
            target_table: Target table name
            key_column: Primary key column for ordering
            columns: Columns to include (None = all common)
            source_dialect: Source SQL dialect
            target_dialect: Target SQL dialect

        Returns:
            True if tables appear identical, False otherwise

        Raises:
            TableNotFoundError: If either table does not exist
            SchemaError: If schema retrieval fails
            KeyColumnError: If key column validation fails
            QueryExecutionError: If hash query execution fails
        """
        # Get common columns and validate key column
        schema_result = self.compare_schemas(source_table, target_table, source_dialect, target_dialect)

        # Validate key column exists in both tables
        self._validate_key_column(key_column, schema_result, source_table, target_table)

        if columns is None:
            columns = schema_result.matching_columns

        if key_column not in columns:
            columns = [key_column] + columns

        # Get aggregate hashes
        source_query = self.query_builder.build_aggregate_hash_query(
            table=source_table,
            columns=columns,
            key_column=key_column,
            dialect=Dialect.DUCKDB,
        )
        target_query = self.query_builder.build_aggregate_hash_query(
            table=target_table,
            columns=columns,
            key_column=key_column,
            dialect=Dialect.DUCKDB,
        )

        try:
            source_hash = self.connector.execute_fetchone(source_query)
            target_hash = self.connector.execute_fetchone(target_query)
        except QueryExecutionError as e:
            raise QueryExecutionError(
                "Failed to compute aggregate hash for comparison",
                details=e.details,
            ) from e

        return source_hash == target_hash
