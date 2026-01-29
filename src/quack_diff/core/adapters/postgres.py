"""PostgreSQL SQL dialect adapter."""

from quack_diff.core.adapters.base import BaseAdapter, Dialect


class PostgresAdapter(BaseAdapter):
    """Adapter for PostgreSQL SQL dialect.

    PostgreSQL has specific syntax for casting (::) and requires
    the pgcrypto extension for MD5 on bytea, though MD5 on text
    works natively.
    """

    @property
    def dialect(self) -> Dialect:
        """Return the dialect this adapter handles."""
        return Dialect.POSTGRES

    @property
    def supports_time_travel(self) -> bool:
        """PostgreSQL doesn't natively support time-travel.

        Note: Some setups with temporal tables or specific extensions
        may support this, but we don't handle those cases.
        """
        return False

    def cast_to_varchar(self, column: str) -> str:
        """Generate SQL to cast a column to VARCHAR.

        PostgreSQL supports both CAST and :: syntax. We use ::
        for brevity but CAST works too.

        Args:
            column: Column name or expression

        Returns:
            SQL expression that casts to VARCHAR (TEXT in PG)
        """
        # Use TEXT which is PostgreSQL's preferred string type
        return f"{column}::TEXT"

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

        PostgreSQL's CONCAT_WS skips NULL values (after our COALESCE,
        there shouldn't be any, but this is the standard behavior).

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

        PostgreSQL's MD5 function works on text and returns
        a 32-character hex string.

        Args:
            expression: SQL expression to hash

        Returns:
            SQL MD5 hash expression
        """
        return f"MD5({expression})"
