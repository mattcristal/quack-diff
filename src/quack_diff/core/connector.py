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

if TYPE_CHECKING:
    from quack_diff.config import Settings, SnowflakeConfig

logger = logging.getLogger(__name__)


def _parse_offset_to_seconds(offset: str) -> int:
    """Parse a human-readable time offset to seconds.

    Supports formats like:
    - "5 minutes ago"
    - "1 hour ago"
    - "30 seconds ago"

    Args:
        offset: Human-readable offset string

    Returns:
        Number of seconds

    Raises:
        ValueError: If offset format is not recognized
    """
    import re

    offset_lower = offset.lower().strip()

    # Remove "ago" suffix if present
    offset_lower = offset_lower.replace(" ago", "").strip()

    # Parse number and unit
    match = re.match(r"(\d+)\s*(second|minute|hour|day|week)s?", offset_lower)
    if not match:
        raise ValueError(
            f"Could not parse offset: {offset}. "
            "Expected format like '5 minutes ago' or '1 hour ago'"
        )

    value = int(match.group(1))
    unit = match.group(2)

    multipliers = {
        "second": 1,
        "minute": 60,
        "hour": 3600,
        "day": 86400,
        "week": 604800,
    }

    return value * multipliers[unit]


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
        """
        mode = "READ_ONLY" if read_only else "READ_WRITE"
        logger.info(f"Attaching DuckDB database '{path}' as '{name}'")
        self.connection.execute(f"ATTACH '{path}' AS {name} ({mode})")

        attached = AttachedDatabase(
            name=name,
            db_type=DatabaseType.DUCKDB,
            attached=True,
            metadata={"path": path, "read_only": read_only},
        )
        self._attached_databases[name] = attached
        return attached

    def detach(self, name: str) -> None:
        """Detach a previously attached database.

        Args:
            name: Name of the database to detach
        """
        if name in self._attached_databases:
            self.connection.execute(f"DETACH {name}")
            del self._attached_databases[name]
            logger.debug(f"Detached database: {name}")

    def execute(self, query: str, params: list[Any] | None = None) -> duckdb.DuckDBPyRelation:
        """Execute a SQL query.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            DuckDB relation result
        """
        logger.debug(f"Executing query: {query[:100]}...")
        if params:
            return self.connection.execute(query, params)
        return self.connection.execute(query)

    def execute_fetchall(
        self, query: str, params: list[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Execute a query and fetch all results.

        Args:
            query: SQL query to execute
            params: Optional query parameters

        Returns:
            List of result tuples
        """
        result = self.execute(query, params)
        return result.fetchall()

    def execute_fetchone(
        self, query: str, params: list[Any] | None = None
    ) -> tuple[Any, ...] | None:
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
        """
        result = self.execute(f"DESCRIBE {table}")
        rows = result.fetchall()
        return [(row[0], row[1]) for row in rows]

    def get_row_count(self, table: str) -> int:
        """Get the row count of a table.

        Args:
            table: Fully qualified table name

        Returns:
            Number of rows in the table
        """
        result = self.execute_fetchone(f"SELECT COUNT(*) FROM {table}")
        return result[0] if result else 0

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

        # Build the query with optional time-travel
        query = f"SELECT * FROM {table_name}"
        if timestamp:
            query = f"SELECT * FROM {table_name} AT (TIMESTAMP => '{timestamp}'::TIMESTAMP_LTZ)"
        elif offset:
            # Parse offset like "5 minutes ago" -> OFFSET => -300
            query = f"SELECT * FROM {table_name} AT (OFFSET => -{_parse_offset_to_seconds(offset)})"

        logger.info(f"Pulling Snowflake table {table_name} to local table {local_name}")
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
                        self.connection.execute(
                            f"CREATE OR REPLACE TABLE {local_name} AS SELECT * FROM arrow_table"
                        )
                        logger.debug(f"Loaded {arrow_table.num_rows} rows via Arrow")
                        return local_name
                except Exception as arrow_err:
                    logger.debug(f"Arrow fetch failed, falling back to pandas: {arrow_err}")

                # Fallback to pandas
                try:
                    import pandas  # noqa: F401 - ensures pandas is installed

                    df = cursor.fetch_pandas_all()
                    self.connection.execute(
                        f"CREATE OR REPLACE TABLE {local_name} AS SELECT * FROM df"
                    )
                    logger.debug(f"Loaded {len(df)} rows via pandas")
                    return local_name
                except ImportError as err:
                    raise ImportError(
                        "pandas is required for Snowflake data transfer when Arrow fails. "
                        "Install it with: pip install pandas"
                    ) from err
            finally:
                cursor.close()

        return local_name

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
