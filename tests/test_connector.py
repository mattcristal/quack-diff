"""Tests for the DuckDB connector module."""

from quack_diff.core.connector import DatabaseType, DuckDBConnector, create_connector


class TestDuckDBConnector:
    """Tests for DuckDBConnector class."""

    def test_create_connection(self):
        """Test creating an in-memory connection."""
        connector = DuckDBConnector()
        assert connector.connection is not None
        connector.close()

    def test_context_manager(self):
        """Test using connector as context manager."""
        with DuckDBConnector() as connector:
            result = connector.execute_fetchone("SELECT 1")
            assert result == (1,)

    def test_execute_query(self, connector: DuckDBConnector):
        """Test executing a simple query."""
        result = connector.execute_fetchone("SELECT 42 AS answer")
        assert result == (42,)

    def test_execute_fetchall(self, connector: DuckDBConnector):
        """Test fetching all results."""
        connector.execute("CREATE TABLE test (id INT)")
        connector.execute("INSERT INTO test VALUES (1), (2), (3)")

        result = connector.execute_fetchall("SELECT * FROM test ORDER BY id")
        assert result == [(1,), (2,), (3,)]

    def test_get_table_schema(self, connector: DuckDBConnector):
        """Test getting table schema."""
        connector.execute("""
            CREATE TABLE schema_test (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                score DECIMAL(10, 2)
            )
        """)

        schema = connector.get_table_schema("schema_test")

        assert len(schema) == 3
        col_names = [col[0] for col in schema]
        assert "id" in col_names
        assert "name" in col_names
        assert "score" in col_names

    def test_get_row_count(self, connector: DuckDBConnector):
        """Test getting row count."""
        connector.execute("CREATE TABLE count_test (id INT)")
        connector.execute("INSERT INTO count_test VALUES (1), (2), (3), (4), (5)")

        count = connector.get_row_count("count_test")
        assert count == 5

    def test_attach_duckdb_file(self, connector: DuckDBConnector, tmp_path):
        """Test attaching another DuckDB file."""
        # Create a temporary DuckDB file
        db_path = tmp_path / "test.duckdb"
        import duckdb

        temp_conn = duckdb.connect(str(db_path))
        temp_conn.execute("CREATE TABLE remote_table (id INT, name VARCHAR)")
        temp_conn.execute("INSERT INTO remote_table VALUES (1, 'test')")
        temp_conn.close()

        # Attach in our connector
        attached = connector.attach_duckdb("remote", str(db_path))

        assert attached.name == "remote"
        assert attached.db_type == DatabaseType.DUCKDB
        assert attached.attached is True

        # Query the attached table
        result = connector.execute_fetchone("SELECT * FROM remote.remote_table")
        assert result == (1, "test")

    def test_detach_database(self, connector: DuckDBConnector, tmp_path):
        """Test detaching a database."""
        # Create and attach a temporary database
        db_path = tmp_path / "detach_test.duckdb"
        import duckdb

        temp_conn = duckdb.connect(str(db_path))
        temp_conn.execute("CREATE TABLE t (id INT)")
        temp_conn.close()

        connector.attach_duckdb("to_detach", str(db_path))
        assert "to_detach" in connector.attached_databases

        connector.detach("to_detach")
        assert "to_detach" not in connector.attached_databases

    def test_attached_databases_property(self, connector: DuckDBConnector, tmp_path):
        """Test that attached_databases returns a copy."""
        db_path = tmp_path / "copy_test.duckdb"
        import duckdb

        temp_conn = duckdb.connect(str(db_path))
        temp_conn.execute("CREATE TABLE t (id INT)")
        temp_conn.close()

        connector.attach_duckdb("test_db", str(db_path))

        attached = connector.attached_databases
        assert "test_db" in attached

        # Modifying the copy should not affect the original
        del attached["test_db"]
        assert "test_db" in connector.attached_databases


class TestCreateConnectorContextManager:
    """Tests for the create_connector context manager."""

    def test_create_connector(self):
        """Test create_connector function."""
        with create_connector() as connector:
            assert isinstance(connector, DuckDBConnector)
            result = connector.execute_fetchone("SELECT 1")
            assert result == (1,)

    def test_create_connector_cleans_up(self):
        """Test that create_connector closes connection on exit."""
        with create_connector() as connector:
            _ = connector.connection  # Access to ensure connection is created

        # Connection should be closed after context exit
        # DuckDB connections don't have an is_closed property,
        # so we verify by checking the internal state
        assert connector._connection is None
