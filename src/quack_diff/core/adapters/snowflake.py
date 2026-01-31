"""Snowflake SQL dialect adapter."""

from quack_diff.core.adapters.base import BaseAdapter, Dialect


class SnowflakeAdapter(BaseAdapter):
    """Adapter for Snowflake SQL dialect.

    Snowflake has excellent time-travel support and specific
    syntax for certain operations.
    """

    @property
    def dialect(self) -> Dialect:
        """Return the dialect this adapter handles."""
        return Dialect.SNOWFLAKE

    @property
    def supports_time_travel(self) -> bool:
        """Snowflake supports time-travel up to 90 days (Enterprise)."""
        return True

    def cast_to_varchar(self, column: str) -> str:
        """Generate SQL to cast a column to VARCHAR.

        Args:
            column: Column name or expression

        Returns:
            SQL expression that casts to VARCHAR
        """
        return f"CAST({column} AS VARCHAR)"

    def coalesce_null(self, expression: str, sentinel: str = "<NULL>") -> str:
        """Generate SQL to replace NULL with a sentinel value.

        Args:
            expression: SQL expression to coalesce
            sentinel: Value to use in place of NULL

        Returns:
            SQL COALESCE expression
        """
        return f"COALESCE({expression}, '{sentinel}')"

    def concat_with_separator(self, expressions: list[str], separator: str = "|#|") -> str:
        """Generate SQL to concatenate expressions with a separator.

        Snowflake's CONCAT_WS is NULL-safe (skips NULLs), which is
        fine since we've already coalesced them.

        Note: Snowflake also has CONCAT which returns NULL if any
        argument is NULL - we explicitly use CONCAT_WS to avoid this.

        Args:
            expressions: List of SQL expressions to concatenate
            separator: String to place between values

        Returns:
            SQL concatenation expression
        """
        expr_list = ", ".join(expressions)
        return f"CONCAT_WS('{separator}', {expr_list})"

    def md5_hash(self, expression: str) -> str:
        """Generate SQL to compute MD5 hash of an expression.

        Snowflake's MD5 returns a 32-character hex string.

        Args:
            expression: SQL expression to hash

        Returns:
            SQL MD5 hash expression
        """
        return f"MD5({expression})"

    def time_travel_clause(
        self,
        timestamp: str | None = None,
        offset: str | None = None,
    ) -> str:
        """Generate SQL clause for Snowflake time-travel queries.

        Supports both absolute timestamps and relative offsets.

        Examples:
            - timestamp="2024-01-01 12:00:00" -> AT(TIMESTAMP => '2024-01-01 12:00:00'::TIMESTAMP)
            - offset="5 minutes" -> AT(OFFSET => -300)  (300 seconds)
            - offset="1 hour" -> AT(OFFSET => -3600)

        Args:
            timestamp: Specific timestamp to query at
            offset: Time offset like "5 minutes", "1 hour", "30 seconds"

        Returns:
            SQL time-travel clause

        Raises:
            ValueError: If neither timestamp nor offset provided
        """
        if timestamp is not None:
            return f"AT(TIMESTAMP => '{timestamp}'::TIMESTAMP_LTZ)"

        if offset is not None:
            # Parse offset string to seconds
            seconds = self._parse_offset_to_seconds(offset)
            return f"AT(OFFSET => -{seconds})"

        raise ValueError("Either timestamp or offset must be provided")

    def _parse_offset_to_seconds(self, offset: str) -> int:
        """Parse a human-readable offset to seconds.

        Args:
            offset: String like "5 minutes", "1 hour", "30 seconds"

        Returns:
            Number of seconds

        Raises:
            ValueError: If offset format is not recognized
        """
        offset = offset.lower().strip()

        # Handle "X ago" format
        if offset.endswith(" ago"):
            offset = offset[:-4].strip()

        parts = offset.split()
        if len(parts) != 2:
            raise ValueError(
                f"Invalid offset format: '{offset}'. Expected format like '5 minutes' or '1 hour'"
            )

        try:
            value = int(parts[0])
        except ValueError:
            raise ValueError(f"Invalid numeric value in offset: '{parts[0]}'") from None

        unit = parts[1].rstrip("s")  # Remove trailing 's' for plurals

        multipliers = {
            "second": 1,
            "minute": 60,
            "hour": 3600,
            "day": 86400,
        }

        if unit not in multipliers:
            raise ValueError(
                f"Unknown time unit: '{unit}'. Supported units: {list(multipliers.keys())}"
            )

        return value * multipliers[unit]

    def wrap_table_with_time_travel(
        self,
        table: str,
        timestamp: str | None = None,
        offset: str | None = None,
    ) -> str:
        """Wrap a Snowflake table reference with time-travel syntax.

        Args:
            table: Table name or reference
            timestamp: Specific timestamp
            offset: Time offset

        Returns:
            Table reference with AT clause
        """
        if timestamp is None and offset is None:
            return table

        clause = self.time_travel_clause(timestamp=timestamp, offset=offset)
        return f"{table} {clause}"
