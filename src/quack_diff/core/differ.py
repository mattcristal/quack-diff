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
            len(self.source_only_columns) == 0
            and len(self.target_only_columns) == 0
            and len(self.type_mismatches) == 0
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
        """
        query = self.query_builder.build_schema_query(table, dialect)
        result = self.connector.execute_fetchall(query)

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
        """
        source_cols = self.get_schema(source_table, source_dialect)
        target_cols = self.get_schema(target_table, target_dialect)

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
        """
        query = self.query_builder.build_count_query(
            table=table,
            dialect=dialect,
            timestamp=timestamp,
            offset=offset,
        )
        result = self.connector.execute_fetchone(query)
        return result[0] if result else 0

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
        """
        logger.info(f"Comparing {source_table} vs {target_table}")

        # Step 1: Compare schemas
        schema_result = self.compare_schemas(
            source_table=source_table,
            target_table=target_table,
            source_dialect=source_dialect,
            target_dialect=target_dialect,
        )

        # Determine columns to compare
        if columns is None:
            columns = schema_result.matching_columns
        else:
            # Validate provided columns exist in both tables
            available = set(c.lower() for c in schema_result.matching_columns)
            columns = [c for c in columns if c.lower() in available]

        if not columns:
            raise ValueError(
                "No common columns found between tables. "
                f"Source columns: {[c.name for c in schema_result.source_columns]}, "
                f"Target columns: {[c.name for c in schema_result.target_columns]}"
            )

        # Ensure key column is in the list
        if key_column not in columns:
            columns = [key_column] + columns

        logger.debug(f"Comparing columns: {columns}")

        # Step 2: Get row counts
        source_count = self.get_row_count(
            source_table, source_dialect, source_timestamp, source_offset
        )
        target_count = self.get_row_count(
            target_table, target_dialect, target_timestamp, target_offset
        )

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
            query += f"\nLIMIT {limit}"

        logger.debug("Executing comparison query")
        diff_rows = self.connector.execute_fetchall(query)

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
        """
        # Get common columns
        if columns is None:
            schema_result = self.compare_schemas(
                source_table, target_table, source_dialect, target_dialect
            )
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

        source_hash = self.connector.execute_fetchone(source_query)
        target_hash = self.connector.execute_fetchone(target_query)

        return source_hash == target_hash
