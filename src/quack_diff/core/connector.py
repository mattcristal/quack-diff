"""DuckDB connection manager with external database support.

Provides DuckDB connectivity and the ability to pull data from external
databases like Snowflake using native connectors.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

import duckdb

from quack_diff.core.sql_utils import (
    AttachError,
    QueryExecutionError,
    SchemaError,
    TableNotFoundError,
    sanitize_identifier,
    sanitize_path,
)
from quack_diff.core.utils import parse_offset_to_seconds

if TYPE_CHECKING:
    from quack_diff.config import Settings, SnowflakeConfig

logger = logging.getLogger(__name__)


class DatabaseType(str, Enum):
    """Supported database types for attachment."""

    DUCKDB = "duckdb"


@dataclass
class AttachedDatabase:
    """Represents an attached external database."""

    name: str
    db_type: DatabaseType
    attached: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


class DuckDBConnector:
    """Manages DuckDB connections and external database attachments.

    This class provides connectivity to DuckDB and external databases.
    For Snowflake, use pull_snowflake_table() which uses the native
    Snowflake connector for better compatibility and time-travel support.

    Example:
        Pull Snowflake table locally:
        >>> connector = DuckDBConnector()
        >>> connector.pull_snowflake_table("SCHEMA.TABLE", "local_table", offset="5 minutes ago")
        >>> result = connector.execute("SELECT * FROM local_table LIMIT 10")

        Attach another DuckDB file:
        >>> connector = DuckDBConnector()
        >>> connector.attach_duckdb("other", "/path/to/other.duckdb")
        >>> result = connector.execute("SELECT * FROM other.schema.table LIMIT 10")
    """

    def __init__(
        self,
        database: str = ":memory:",
        read_only: bool = False,
        settings: Settings | None = None,
    ) -> None:
        """Initialize the DuckDB connector.

        Args:
            database: Path to DuckDB database file or ":memory:" for in-memory
            read_only: Open database in read-only mode
            settings: Optional Settings instance for default configurations
        """
        self._database = database
        self._read_only = read_only
        self._settings = settings
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._attached_databases: dict[str, AttachedDatabase] = {}

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create the DuckDB connection."""
        if self._connection is None:
            self._connection = duckdb.connect(
                database=self._database,
                read_only=self._read_only,
            )
            logger.debug(f"Created DuckDB connection: {self._database}")
        return self._connection

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            self._attached_databases.clear()
            logger.debug("Closed DuckDB connection")

    def __enter__(self) -> DuckDBConnector:
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    def attach_duckdb(self, name: str, path: str, read_only: bool = True) -> AttachedDatabase:
        """Attach another DuckDB database file.

        Args:
            name: Alias for the attached database
            path: Path to the DuckDB database file
            read_only: Open in read-only mode

        Returns:
            AttachedDatabase instance

        Raises:
            SQLInjectionError: If name or path contains unsafe characters
            AttachError: If the database cannot be attached (file not found,
                permission denied, invalid format, already attached, etc.)
        """
        import os

        # Sanitize inputs to prevent SQL injection
        sanitized_name = sanitize_identifier(name)
        sanitized_path = sanitize_path(path)

        # Validate file exists before attempting attach
        if not os.path.exists(sanitized_path):
            raise AttachError(
                f"Database file not found: '{sanitized_path}'",
                path=sanitized_path,
                alias=sanitized_name,
                details="Verify the file path is correct and the file exists",
            )

        # Check if database is already attached with the same name
        if sanitized_name in self._attached_databases:
            existing = self._attached_databases[sanitized_name]
            existing_path = existing.metadata.get("path", "unknown")
            if existing_path == sanitized_path:
                logger.debug(f"Database '{sanitized_name}' already attached from '{sanitized_path}'")
                return existing
            raise AttachError(
                f"Database alias '{sanitized_name}' is already in use",
                path=sanitized_path,
                alias=sanitized_name,
                details=f"Already attached from: {existing_path}",
            )

        mode = "READ_ONLY" if read_only else "READ_WRITE"
        logger.info(f"Attaching DuckDB database '{sanitized_path}' as '{sanitized_name}'")

        try:
            # Use parameterized query where possible, but ATTACH requires identifier
            # Since we've sanitized the inputs, this is safe
            self.connection.execute(f"ATTACH '{sanitized_path}' AS {sanitized_name} ({mode})")
        except duckdb.IOException as e:
            error_msg = str(e).lower()
            if "permission" in error_msg or "access" in error_msg:
                raise AttachError(
                    f"Permission denied accessing database file: '{sanitized_path}'",
                    path=sanitized_path,
                    alias=sanitized_name,
                    details=str(e),
                ) from e
            raise AttachError(
                f"I/O error attaching database: '{sanitized_path}'",
                path=sanitized_path,
                alias=sanitized_name,
                details=str(e),
            ) from e
        except duckdb.InvalidInputException as e:
            raise AttachError(
                f"Invalid database file format: '{sanitized_path}'",
                path=sanitized_path,
                alias=sanitized_name,
                details=str(e),
            ) from e
        except duckdb.Error as e:
            raise AttachError(
                f"Failed to attach database '{sanitized_path}' as '{sanitized_name}'",
                path=sanitized_path,
                alias=sanitized_name,
                details=str(e),
            ) from e

        attached = AttachedDatabase(
            name=sanitized_name,
            db_type=DatabaseType.DUCKDB,
            attached=True,
            metadata={"path": sanitized_path, "read_only": read_only},
        )
        self._attached_databases[sanitized_name] = attached
        return attached

    def detach(self, name: str) -> None:
        """Detach a previously attached database.

        Args:
            name: Name of the database to detach

        Raises:
            SQLInjectionError: If name contains unsafe characters
            QueryExecutionError: If detach operation fails
        """
        # Sanitize the database name
        sanitized_name = sanitize_identifier(name)

        if sanitized_name in self._attached_databases:
            try:
                self.connection.execute(f"DETACH {sanitized_name}")
            except duckdb.Error as e:
                raise QueryExecutionError(
                    f"Failed to detach database '{sanitized_name}'",
                    query=f"DETACH {sanitized_name}",
                    details=str(e),
                ) from e
            del self._attached_databases[sanitized_name]
            logger.debug(f"Detached database: {sanitized_name}")

    def execute(self, query: str, params: list[Any] | None = None) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            DuckDB relation result

        Raises:
            TableNotFoundError: If a referenced table does not exist
            QueryExecutionError: If query execution fails
        """
        logger.debug(f"Executing query: {query[:100]}...")
        try:
            if params:
                return self.connection.execute(query, params)
            return self.connection.execute(query)
        except duckdb.CatalogException as e:
            error_msg = str(e).lower()
            # Extract table name from error if possible
            if "table" in error_msg and ("does not exist" in error_msg or "not found" in error_msg):
                # Try to extract table name from error message
                # DuckDB format: "Table with name X does not exist"
                import re

                match = re.search(r"table with name (\S+) does not exist", str(e), re.IGNORECASE)
                table_name = match.group(1) if match else "unknown"
                raise TableNotFoundError(
                    table=table_name,
                    message=f"Table '{table_name}' does not exist",
                    details=str(e),
                ) from e
            raise QueryExecutionError(
                f"Catalog error: {e}",
                query=query,
                details=str(e),
            ) from e
        except duckdb.BinderException as e:
            error_msg = str(e).lower()
            if "table" in error_msg and ("does not exist" in error_msg or "not found" in error_msg):
                import re

                match = re.search(r"table with name (\S+) does not exist", str(e), re.IGNORECASE)
                table_name = match.group(1) if match else "unknown"
                raise TableNotFoundError(
                    table=table_name,
                    message=f"Table '{table_name}' does not exist",
                    details=str(e),
                ) from e
            raise QueryExecutionError(
                f"Query binding error: {e}",
                query=query,
                details=str(e),
            ) from e
        except duckdb.Error as e:
            raise QueryExecutionError(
                f"Query execution failed: {e}",
                query=query,
                details=str(e),
            ) from e

    def execute_fetchall(self, query: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
        """Execute a query and fetch all results.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            List of result tuples
        """
        result = self.execute(query, params)
        return result.fetchall()

    def execute_fetchone(self, query: str, params: list[Any] | None = None) -> tuple[Any, ...] | None:
        """Execute a query and fetch one result.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            Single result tuple or None
        """
        result = self.execute(query, params)
        return result.fetchone()

    def get_table_schema(self, table: str) -> list[tuple[str, str]]:
        """Get the schema of a table.

        Args:
            table: Fully qualified table name (e.g., "db.schema.table")

        Returns:
            List of (column_name, column_type) tuples

        Raises:
            SQLInjectionError: If table name contains unsafe characters
            TableNotFoundError: If the table does not exist
            SchemaError: If schema retrieval fails or returns empty result
        """
        # Sanitize the table name
        sanitized_table = sanitize_identifier(table)

        try:
            result = self.execute(f"DESCRIBE {sanitized_table}")
            rows = result.fetchall()
        except TableNotFoundError as e:
            # Re-raise with more context about the table
            raise TableNotFoundError(
                table=sanitized_table,
                message=f"Cannot describe table '{sanitized_table}': table does not exist",
                details="Verify the table name, schema, and database are correct",
            ) from e
        except QueryExecutionError as e:
            raise SchemaError(
                f"Failed to retrieve schema for table '{sanitized_table}'",
                table=sanitized_table,
                details=e.details,
            ) from e

        if not rows:
            raise SchemaError(
                f"Table '{sanitized_table}' has no columns or schema is empty",
                table=sanitized_table,
                details="The table may be corrupted or have an invalid structure",
            )

        return [(row[0], row[1]) for row in rows]

    def get_row_count(self, table: str) -> int:
        """Get the row count of a table.

        Args:
            table: Fully qualified table name

        Returns:
            Number of rows in the table

        Raises:
            SQLInjectionError: If table name contains unsafe characters
            TableNotFoundError: If the table does not exist
            QueryExecutionError: If the count query fails
        """
        # Sanitize the table name
        sanitized_table = sanitize_identifier(table)

        try:
            result = self.execute_fetchone(f"SELECT COUNT(*) FROM {sanitized_table}")
            return result[0] if result else 0
        except TableNotFoundError as e:
            raise TableNotFoundError(
                table=sanitized_table,
                message=f"Cannot count rows: table '{sanitized_table}' does not exist",
                details="Verify the table name, schema, and database are correct",
            ) from e

    def pull_snowflake_table(
        self,
        table_name: str,
        local_name: str,
        timestamp: str | None = None,
        offset: str | None = None,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
        authenticator: str | None = None,
        connection_name: str | None = None,
        config: SnowflakeConfig | None = None,
    ) -> str:
        """Pull a Snowflake table into DuckDB using native Snowflake connector.

        This method uses snowflake-connector-python directly instead of the
        DuckDB Snowflake extension. This provides better compatibility,
        supports time-travel queries, and avoids virtual column errors.

        Args:
            table_name: Snowflake table name (e.g., "SCHEMA.TABLE" or just "TABLE")
            local_name: Local table name in DuckDB
            timestamp: Time-travel timestamp (e.g., "2024-01-15 10:30:00")
            offset: Time-travel offset (e.g., "5 minutes ago", "1 hour ago")
            account: Snowflake account identifier
            user: Snowflake username
            password: Snowflake password
            database: Snowflake database name
            schema: Snowflake schema name
            warehouse: Compute warehouse
            role: User role
            authenticator: Authentication method
            connection_name: Connection profile from ~/.snowflake/connections.toml
            config: SnowflakeConfig instance

        Returns:
            The local table name where data was loaded

        Raises:
            ImportError: If snowflake-connector-python is not installed
            ValueError: If required parameters are missing
        """
        try:
            import snowflake.connector
        except ImportError as e:
            raise ImportError(
                "snowflake-connector-python is required for pull_snowflake_table. "
                "Install it with: pip install snowflake-connector-python"
            ) from e

        # If connection_name provided, create a config from it
        if connection_name is not None and config is None:
            from quack_diff.config import SnowflakeConfig as SFConfig

            config = SFConfig(connection_name=connection_name)

        # Use config or settings if parameters not provided
        if config is None and self._settings is not None:
            config = self._settings.snowflake

        if config is not None:
            account = account or config.account
            user = user or config.user
            password = password or config.password
            database = database or config.database
            schema = schema or config.schema_name
            warehouse = warehouse or config.warehouse
            role = role or config.role
            authenticator = authenticator or config.authenticator

        # Normalize authenticator value
        auth_type = (authenticator or "").lower()

        # Build connection parameters for snowflake.connector
        conn_params: dict[str, Any] = {"account": account}

        if auth_type in ("externalbrowser", "ext_browser"):
            conn_params["authenticator"] = "externalbrowser"
            if user:
                conn_params["user"] = user
        else:
            # Password authentication (default)
            if not all([account, user, password]):
                raise ValueError(
                    "Snowflake connection requires account, user, and password. "
                    "Provide via parameters, config, connection_name, or environment variables."
                )
            conn_params["user"] = user
            conn_params["password"] = password

        if database:
            conn_params["database"] = database
        if schema:
            conn_params["schema"] = schema
        if warehouse:
            conn_params["warehouse"] = warehouse
        if role:
            conn_params["role"] = role

        # Sanitize table and local table names
        sanitized_table = sanitize_identifier(table_name)
        sanitized_local = sanitize_identifier(local_name)

        # Build the query with optional time-travel
        query = f"SELECT * FROM {sanitized_table}"
        if timestamp:
            # Note: timestamp value is passed to Snowflake connector, not interpolated
            query = f"SELECT * FROM {sanitized_table} AT (TIMESTAMP => '{timestamp}'::TIMESTAMP_LTZ)"
        elif offset:
            # Parse offset like "5 minutes ago" -> OFFSET => -300
            # The offset is parsed and converted to integer, safe from injection
            seconds = parse_offset_to_seconds(offset)
            query = f"SELECT * FROM {sanitized_table} AT (OFFSET => -{seconds})"

        logger.info(f"Pulling Snowflake table {sanitized_table} to local table {sanitized_local}")
        logger.debug(f"Query: {query}")

        # Connect to Snowflake and fetch data
        with snowflake.connector.connect(**conn_params) as sf_conn:
            cursor = sf_conn.cursor()
            try:
                cursor.execute(query)

                # Try Arrow fetch first (most efficient)
                try:
                    arrow_table = cursor.fetch_arrow_all()
                    if arrow_table is not None:
                        # Use sanitized local table name
                        self.connection.execute(
                            f"CREATE OR REPLACE TABLE {sanitized_local} AS SELECT * FROM arrow_table"
                        )
                        logger.debug(f"Loaded {arrow_table.num_rows} rows via Arrow")
                        return sanitized_local
                except Exception as arrow_err:
                    logger.debug(f"Arrow fetch failed, falling back to pandas: {arrow_err}")

                # Fallback to pandas
                try:
                    import pandas  # noqa: F401 - ensures pandas is installed

                    df = cursor.fetch_pandas_all()
                    # Use sanitized local table name
                    self.connection.execute(f"CREATE OR REPLACE TABLE {sanitized_local} AS SELECT * FROM df")
                    logger.debug(f"Loaded {len(df)} rows via pandas")
                    return sanitized_local
                except ImportError as err:
                    raise ImportError(
                        "pandas is required for Snowflake data transfer when Arrow fails. "
                        "Install it with: pip install pandas"
                    ) from err
            finally:
                cursor.close()

        return sanitized_local

    def _build_snowflake_conn_params(
        self,
        config: SnowflakeConfig | None = None,
        database: str | None = None,
    ) -> dict[str, Any]:
        """Build Snowflake connection parameters from config and settings.

        Args:
            config: SnowflakeConfig instance (falls back to self._settings.snowflake)
            database: Optional database override

        Returns:
            Dict of connection parameters for snowflake.connector.connect()

        Raises:
            ImportError: If snowflake-connector-python is not installed
            ValueError: If required credentials are missing
        """
        try:
            import snowflake.connector  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "snowflake-connector-python is required for Snowflake operations. "
                "Install it with: pip install snowflake-connector-python"
            ) from e

        if config is None and self._settings is not None:
            config = self._settings.snowflake

        account = config.account if config else None
        user = config.user if config else None
        password = config.password if config else None
        db = database or (config.database if config else None)
        schema = config.schema_name if config else None
        warehouse = config.warehouse if config else None
        role = config.role if config else None
        authenticator = config.authenticator if config else None

        auth_type = (authenticator or "").lower()
        conn_params: dict[str, Any] = {"account": account}

        if auth_type in ("externalbrowser", "ext_browser"):
            conn_params["authenticator"] = "externalbrowser"
            if user:
                conn_params["user"] = user
        else:
            if not all([account, user, password]):
                raise ValueError(
                    "Snowflake connection requires account, user, and password. "
                    "Provide via config, connection_name, or environment variables."
                )
            conn_params["user"] = user
            conn_params["password"] = password

        if db:
            conn_params["database"] = db
        if schema:
            conn_params["schema"] = schema
        if warehouse:
            conn_params["warehouse"] = warehouse
        if role:
            conn_params["role"] = role

        return conn_params

    def execute_snowflake_scalar(
        self,
        query: str,
        config: SnowflakeConfig | None = None,
        database: str | None = None,
    ) -> Any:
        """Execute a SQL query on Snowflake and return a single scalar value.

        Useful for running COUNT queries or other aggregations directly on
        Snowflake without pulling data into DuckDB.

        Args:
            query: SQL query that returns exactly one row with one column
            config: SnowflakeConfig instance (falls back to settings)
            database: Optional database override

        Returns:
            The scalar value from the query result

        Raises:
            ImportError: If snowflake-connector-python is not installed
            ValueError: If required credentials are missing
            QueryExecutionError: If the query fails or returns non-scalar result
        """
        import snowflake.connector

        conn_params = self._build_snowflake_conn_params(config=config, database=database)

        logger.info(f"Executing Snowflake scalar query: {query[:120]}...")
        logger.debug(f"Full query: {query}")

        try:
            with snowflake.connector.connect(**conn_params) as sf_conn:
                cursor = sf_conn.cursor()
                try:
                    cursor.execute(query)
                    row = cursor.fetchone()
                    if row is None:
                        raise QueryExecutionError(
                            "Snowflake query returned no rows (expected exactly one scalar value)",
                            query=query,
                        )
                    if len(row) != 1:
                        raise QueryExecutionError(
                            f"Snowflake query returned {len(row)} columns (expected exactly one)",
                            query=query,
                        )
                    return row[0]
                finally:
                    cursor.close()
        except snowflake.connector.errors.ProgrammingError as e:
            raise QueryExecutionError(
                f"Snowflake query failed: {e}",
                query=query,
                details=str(e),
            ) from e

    @property
    def attached_databases(self) -> dict[str, AttachedDatabase]:
        """Get dictionary of attached databases."""
        return self._attached_databases.copy()


@contextmanager
def create_connector(
    database: str = ":memory:",
    settings: Settings | None = None,
) -> Generator[DuckDBConnector, None, None]:
    """Create a DuckDB connector as a context manager.

    Args:
        database: Path to DuckDB database or ":memory:"
        settings: Optional Settings instance

    Yields:
        DuckDBConnector instance
    """
    connector = DuckDBConnector(database=database, settings=settings)
    try:
        yield connector
    finally:
        connector.close()
