"""Integration tests for SQL injection prevention.

These tests demonstrate that the security improvements prevent common
SQL injection attacks across all layers of the application.
"""

from __future__ import annotations

import pytest

from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.query_builder import QueryBuilder
from quack_diff.core.sql_utils import SQLInjectionError


class TestConnectorSecurity:
    """Test that DuckDBConnector prevents SQL injection."""

    def test_attach_duckdb_blocks_malicious_name(self):
        """Test that attach_duckdb blocks SQL injection in database name."""
        connector = DuckDBConnector()

        malicious_names = [
            "db; DROP TABLE users",
            "db-- comment",
            "db OR 1=1",
            "db UNION SELECT * FROM passwords",
        ]

        for name in malicious_names:
            with pytest.raises(SQLInjectionError):
                connector.attach_duckdb(name=name, path=":memory:")

    def test_attach_duckdb_blocks_malicious_path(self):
        """Test that attach_duckdb blocks SQL injection in file path."""
        connector = DuckDBConnector()

        malicious_paths = [
            "/path/to/db.duckdb; DROP TABLE users",
            "/path/to/db.duckdb-- comment",
            "/path/to/db.duckdb/* comment */",
        ]

        for path in malicious_paths:
            with pytest.raises(SQLInjectionError):
                connector.attach_duckdb(name="test", path=path)

    def test_attach_duckdb_allows_valid_inputs(self, tmp_path):
        """Test that attach_duckdb allows valid inputs."""
        import duckdb

        connector = DuckDBConnector()

        # Create a valid temporary database file
        db_file = tmp_path / "test.duckdb"
        # Initialize it as a valid DuckDB database
        temp_conn = duckdb.connect(str(db_file))
        temp_conn.execute("CREATE TABLE dummy (id INTEGER)")
        temp_conn.close()

        # These should work without raising exceptions
        # Note: We only use valid, unquoted identifiers (no hyphens)
        valid_cases = [
            ("test_db", str(db_file)),
            ("mydatabase", str(db_file)),
            ("db_1", str(db_file)),
        ]

        for name, path in valid_cases:
            try:
                connector.attach_duckdb(name=name, path=path, read_only=True)
                # Clean up
                connector.detach(name)
            except SQLInjectionError:
                pytest.fail(f"Valid input '{name}' was incorrectly blocked")

    def test_detach_blocks_malicious_name(self):
        """Test that detach blocks SQL injection in database name."""
        connector = DuckDBConnector()

        with pytest.raises(SQLInjectionError):
            connector.detach("db; DROP TABLE users")

    def test_get_table_schema_blocks_malicious_table(self):
        """Test that get_table_schema blocks SQL injection in table name."""
        connector = DuckDBConnector()

        with pytest.raises(SQLInjectionError):
            connector.get_table_schema("users; DROP TABLE students")

    def test_get_row_count_blocks_malicious_table(self):
        """Test that get_row_count blocks SQL injection in table name."""
        connector = DuckDBConnector()

        with pytest.raises(SQLInjectionError):
            connector.get_row_count("users OR 1=1")


class TestQueryBuilderSecurity:
    """Test that QueryBuilder prevents SQL injection."""

    def test_build_hash_query_blocks_malicious_table(self):
        """Test that build_hash_query blocks SQL injection in table name."""
        builder = QueryBuilder()

        with pytest.raises(SQLInjectionError):
            builder.build_hash_query(
                table="users; DROP TABLE students",
                columns=["id", "name"],
                key_column="id",
            )

    def test_build_hash_query_blocks_malicious_column(self):
        """Test that build_hash_query blocks SQL injection in column name."""
        builder = QueryBuilder()

        with pytest.raises(SQLInjectionError):
            builder.build_hash_query(
                table="users",
                columns=["id", "name OR 1=1"],
                key_column="id",
            )

    def test_build_hash_query_blocks_malicious_key_column(self):
        """Test that build_hash_query blocks SQL injection in key column."""
        builder = QueryBuilder()

        with pytest.raises(SQLInjectionError):
            builder.build_hash_query(
                table="users",
                columns=["id", "name"],
                key_column="id; DROP TABLE students",
            )

    def test_build_count_query_blocks_malicious_table(self):
        """Test that build_count_query blocks SQL injection."""
        builder = QueryBuilder()

        with pytest.raises(SQLInjectionError):
            builder.build_count_query(table="users UNION SELECT * FROM passwords")

    def test_build_schema_query_blocks_malicious_table(self):
        """Test that build_schema_query blocks SQL injection."""
        builder = QueryBuilder()

        with pytest.raises(SQLInjectionError):
            builder.build_schema_query(table="users-- comment")

    def test_build_sample_query_blocks_malicious_identifiers(self):
        """Test that build_sample_query blocks SQL injection in identifiers."""
        builder = QueryBuilder()

        # Malicious table name
        with pytest.raises(SQLInjectionError):
            builder.build_sample_query(
                table="users; DROP TABLE",
                columns=["id", "name"],
                key_column="id",
                keys=["1", "2"],
            )

        # Malicious column name
        with pytest.raises(SQLInjectionError):
            builder.build_sample_query(
                table="users",
                columns=["id", "name OR 1=1"],
                key_column="id",
                keys=["1", "2"],
            )

    def test_build_sample_query_escapes_key_values(self):
        """Test that build_sample_query escapes single quotes in key values."""
        builder = QueryBuilder()

        # Key values with single quotes should be escaped
        query = builder.build_sample_query(
            table="users",
            columns=["id", "name"],
            key_column="id",
            keys=["1", "2'OR'1'='1"],  # Attempt to inject via key value
        )

        # The single quotes should be doubled (SQL escape)
        assert "2''OR''1''=''1" in query
        # Should not create an OR condition
        assert "WHERE id IN ('1', '2''OR''1''=''1')" in query

    def test_build_hash_comparison_query_blocks_malicious_tables(self):
        """Test that build_hash_comparison_query blocks SQL injection."""
        builder = QueryBuilder()

        with pytest.raises(SQLInjectionError):
            builder.build_hash_comparison_query(
                source_table="users; DROP TABLE",
                target_table="users",
                columns=["id", "name"],
                key_column="id",
            )

        with pytest.raises(SQLInjectionError):
            builder.build_hash_comparison_query(
                source_table="users",
                target_table="users UNION SELECT * FROM passwords",
                columns=["id", "name"],
                key_column="id",
            )

    def test_query_builder_allows_qualified_names(self):
        """Test that QueryBuilder allows valid qualified names."""
        builder = QueryBuilder()

        # These should work without raising exceptions
        try:
            query = builder.build_hash_query(
                table="schema.table",
                columns=["id", "name"],
                key_column="id",
            )
            assert "schema.table" in query or '"schema"."table"' in query

            query = builder.build_hash_query(
                table="db.schema.table",
                columns=["id", "name"],
                key_column="id",
            )
            assert "db.schema.table" in query or '"db"."schema"."table"' in query
        except SQLInjectionError:
            pytest.fail("Valid qualified names were incorrectly blocked")


class TestEndToEndSecurity:
    """End-to-end integration tests for security."""

    def test_complete_workflow_with_malicious_input(self):
        """Test that malicious input is blocked in a complete workflow."""
        connector = DuckDBConnector()

        # Attempt 1: Malicious table name in attach
        with pytest.raises(SQLInjectionError):
            connector.attach_duckdb(name="evil; DROP TABLE users", path=":memory:")

        # Attempt 2: Malicious path in attach
        with pytest.raises(SQLInjectionError):
            connector.attach_duckdb(name="test", path="/path; DELETE FROM users")

        # Attempt 3: Malicious table name in query
        builder = QueryBuilder()
        with pytest.raises(SQLInjectionError):
            builder.build_hash_query(
                table="users OR 1=1",
                columns=["id"],
                key_column="id",
            )

    def test_complete_workflow_with_valid_input(self):
        """Test that valid input works in a complete workflow."""
        connector = DuckDBConnector()

        # Create a test table
        connector.execute("CREATE TABLE test_users (id INTEGER, name VARCHAR)")
        connector.execute("INSERT INTO test_users VALUES (1, 'Alice'), (2, 'Bob')")

        # Build and execute a query with sanitized inputs
        builder = QueryBuilder()

        try:
            query = builder.build_hash_query(
                table="test_users",
                columns=["id", "name"],
                key_column="id",
            )

            # Execute the query - should work without issues
            result = connector.execute(query)
            rows = result.fetchall()

            # Should return 2 rows
            assert len(rows) == 2

        except SQLInjectionError:
            pytest.fail("Valid workflow was incorrectly blocked")

        finally:
            connector.execute("DROP TABLE IF EXISTS test_users")


class TestCommonAttackVectors:
    """Test protection against common SQL injection attack vectors."""

    def test_prevents_stacked_queries(self):
        """Test prevention of stacked query injection."""
        builder = QueryBuilder()

        attacks = [
            "users; DROP TABLE students;",
            "users; DELETE FROM passwords;",
            "users; INSERT INTO admins VALUES ('hacker');",
        ]

        for attack in attacks:
            with pytest.raises(SQLInjectionError):
                builder.build_hash_query(table=attack, columns=["id"], key_column="id")

    def test_prevents_union_injection(self):
        """Test prevention of UNION-based injection."""
        builder = QueryBuilder()

        attacks = [
            "users UNION SELECT * FROM passwords",
            "users' UNION SELECT NULL, username, password FROM admins--",
        ]

        for attack in attacks:
            with pytest.raises(SQLInjectionError):
                builder.build_hash_query(table=attack, columns=["id"], key_column="id")

    def test_prevents_comment_injection(self):
        """Test prevention of comment-based injection."""
        builder = QueryBuilder()

        attacks = [
            "users-- comment",
            "users/* comment */",
            "users'--",
        ]

        for attack in attacks:
            with pytest.raises(SQLInjectionError):
                builder.build_hash_query(table=attack, columns=["id"], key_column="id")

    def test_prevents_boolean_injection(self):
        """Test prevention of boolean-based injection."""
        builder = QueryBuilder()

        attacks = [
            "users WHERE 1=1",
            "users' OR '1'='1",
        ]

        for attack in attacks:
            with pytest.raises(SQLInjectionError):
                builder.build_hash_query(table=attack, columns=["id"], key_column="id")
