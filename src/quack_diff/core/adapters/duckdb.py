"""DuckDB SQL dialect adapter."""

from quack_diff.core.adapters.base import BaseAdapter, Dialect


class DuckDBAdapter(BaseAdapter):
    """Adapter for DuckDB SQL dialect.

    DuckDB has good SQL standard compliance and serves as the
    "default" dialect for local operations.
    """

    @property
    def dialect(self) -> Dialect:
        """Return the dialect this adapter handles."""
        return Dialect.DUCKDB

    @property
    def supports_time_travel(self) -> bool:
        """DuckDB doesn't natively support time-travel."""
        return False

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

        DuckDB supports CONCAT_WS which handles the separator correctly.

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

        Args:
            expression: SQL expression to hash

        Returns:
            SQL MD5 hash expression
        """
        return f"MD5({expression})"
