"""DuckDB connection manager with external database attachment support.

Uses DuckDB's extension system to connect to external databases like
Snowflake and PostgreSQL, treating them as local tables for querying.
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
    from quack_diff.config import PostgresConfig, Settings, SnowflakeConfig

logger = logging.getLogger(__name__)


class DatabaseType(str, Enum):
    """Supported database types."""

    DUCKDB = "duckdb"
    SNOWFLAKE = "snowflake"
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"


@dataclass
class AttachedDatabase:
    """Represents an attached external database."""

    name: str
    db_type: DatabaseType
    attached: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)


class DuckDBConnector:
    """Manages DuckDB connections and external database attachments.

    This class acts as a "universal adapter" by leveraging DuckDB's
    extension system to connect to various databases.

    Example:
        >>> connector = DuckDBConnector()
        >>> connector.attach_snowflake("sf", account="...", user="...", password="...")
        >>> result = connector.execute("SELECT * FROM sf.schema.table LIMIT 10")
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
        self._installed_extensions: set[str] = set()

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

    def _install_extension(self, extension: str) -> None:
        """Install and load a DuckDB extension if not already loaded.

        Args:
            extension: Name of the extension to install
        """
        if extension not in self._installed_extensions:
            logger.debug(f"Installing extension: {extension}")
            self.connection.execute(f"INSTALL {extension}")
            self.connection.execute(f"LOAD {extension}")
            self._installed_extensions.add(extension)

    def attach_snowflake(
        self,
        name: str,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
        config: SnowflakeConfig | None = None,
        connection_name: str | None = None,
    ) -> AttachedDatabase:
        """Attach a Snowflake database.

        Credentials can be provided in multiple ways (in priority order):
        1. Explicit parameters (account, user, password)
        2. config parameter (SnowflakeConfig instance)
        3. connection_name -> reads from ~/.snowflake/connections.toml
        4. Settings from environment variables

        Args:
            name: Alias for the attached database
            account: Snowflake account identifier
            user: Snowflake username
            password: Snowflake password
            database: Snowflake database name
            warehouse: Compute warehouse (optional)
            role: User role (optional)
            config: SnowflakeConfig instance (alternative to individual params)
            connection_name: Connection profile from ~/.snowflake/connections.toml

        Returns:
            AttachedDatabase instance

        Raises:
            ValueError: If required parameters are missing
        """
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
            warehouse = warehouse or config.warehouse
            role = role or config.role

        if not all([account, user, password]):
            raise ValueError(
                "Snowflake connection requires account, user, and password. "
                "Provide via parameters, config, connection_name, or environment variables."
            )

        self._install_extension("snowflake")

        # Build connection string
        conn_parts = [
            f"account={account}",
            f"user={user}",
            f"password={password}",
        ]
        if database:
            conn_parts.append(f"database={database}")
        if warehouse:
            conn_parts.append(f"warehouse={warehouse}")
        if role:
            conn_parts.append(f"role={role}")

        conn_string = ";".join(conn_parts)

        logger.info(f"Attaching Snowflake database as '{name}'")
        self.connection.execute(
            f"ATTACH '{conn_string}' AS {name} (TYPE snowflake)"
        )

        attached = AttachedDatabase(
            name=name,
            db_type=DatabaseType.SNOWFLAKE,
            attached=True,
            metadata={"account": account, "database": database},
        )
        self._attached_databases[name] = attached
        return attached

    def attach_postgres(
        self,
        name: str,
        connection_string: str | None = None,
        host: str | None = None,
        port: int = 5432,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        config: PostgresConfig | None = None,
    ) -> AttachedDatabase:
        """Attach a PostgreSQL database.

        Args:
            name: Alias for the attached database
            connection_string: Full PostgreSQL connection string
            host: PostgreSQL host
            port: PostgreSQL port
            user: PostgreSQL username
            password: PostgreSQL password
            database: PostgreSQL database name
            config: PostgresConfig instance (alternative to individual params)

        Returns:
            AttachedDatabase instance

        Raises:
            ValueError: If required parameters are missing
        """
        # Use config or settings if parameters not provided
        if config is None and self._settings is not None:
            config = self._settings.postgres

        if config is not None:
            connection_string = connection_string or config.get_connection_string()

        # Build connection string from parts if not provided
        if connection_string is None:
            if not all([host, user, database]):
                raise ValueError(
                    "PostgreSQL connection requires connection_string or host/user/database. "
                    "Provide via parameters, config, or environment variables."
                )
            pwd = f":{password}" if password else ""
            connection_string = f"postgresql://{user}{pwd}@{host}:{port}/{database}"

        self._install_extension("postgres")

        logger.info(f"Attaching PostgreSQL database as '{name}'")
        self.connection.execute(
            f"ATTACH '{connection_string}' AS {name} (TYPE postgres)"
        )

        attached = AttachedDatabase(
            name=name,
            db_type=DatabaseType.POSTGRES,
            attached=True,
            metadata={"connection_string": connection_string[:50] + "..."},  # Truncate for safety
        )
        self._attached_databases[name] = attached
        return attached

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
