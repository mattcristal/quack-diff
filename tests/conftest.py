"""Pytest fixtures for quack-diff tests."""

from __future__ import annotations

import tempfile
from pathlib import Path

import duckdb
import pytest

from quack_diff.cli.console import set_json_output_mode
from quack_diff.config import reset_settings
from quack_diff.core.connector import DuckDBConnector


@pytest.fixture(autouse=True)
def reset_global_settings():
    """Reset global settings and console state before each test."""
    reset_settings()
    set_json_output_mode(False)
    yield
    reset_settings()
    set_json_output_mode(False)


@pytest.fixture
def duckdb_connection():
    """Create a fresh in-memory DuckDB connection."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def connector():
    """Create a DuckDBConnector instance."""
    connector = DuckDBConnector(database=":memory:")
    yield connector
    connector.close()


@pytest.fixture
def sample_tables(connector: DuckDBConnector):
    """Create sample tables for testing.

    Creates two tables:
    - source_table: Original data
    - target_table: Modified data (with added, removed, modified rows)
    """
    # Create source table
    connector.execute("""
        CREATE TABLE source_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            score DECIMAL(10, 2)
        )
    """)

    connector.execute("""
        INSERT INTO source_table VALUES
            (1, 'Alice', 'alice@example.com', 30, 95.50),
            (2, 'Bob', 'bob@example.com', 25, 88.00),
            (3, 'Charlie', 'charlie@example.com', 35, 92.75),
            (4, 'Diana', 'diana@example.com', 28, 91.00),
            (5, 'Eve', 'eve@example.com', 32, 87.25)
    """)

    # Create target table with modifications
    connector.execute("""
        CREATE TABLE target_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            email VARCHAR,
            age INTEGER,
            score DECIMAL(10, 2)
        )
    """)

    connector.execute("""
        INSERT INTO target_table VALUES
            (1, 'Alice', 'alice@example.com', 30, 95.50),  -- unchanged
            (2, 'Bob', 'bob.new@example.com', 25, 88.00),  -- modified email
            (3, 'Charlie', 'charlie@example.com', 36, 92.75),  -- modified age
            (5, 'Eve', 'eve@example.com', 32, 87.25),  -- unchanged
            (6, 'Frank', 'frank@example.com', 29, 90.00)  -- added
            -- Diana (4) removed
    """)

    return connector


@pytest.fixture
def identical_tables(connector: DuckDBConnector):
    """Create two identical tables for testing match scenarios."""
    connector.execute("""
        CREATE TABLE identical_source (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            value DECIMAL(10, 2)
        )
    """)

    connector.execute("""
        INSERT INTO identical_source VALUES
            (1, 'Item A', 100.00),
            (2, 'Item B', 200.00),
            (3, 'Item C', 300.00)
    """)

    connector.execute("""
        CREATE TABLE identical_target AS
        SELECT * FROM identical_source
    """)

    return connector


@pytest.fixture
def null_handling_tables(connector: DuckDBConnector):
    """Create tables with NULL values for testing NULL handling."""
    connector.execute("""
        CREATE TABLE null_source (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            optional_field VARCHAR
        )
    """)

    connector.execute("""
        INSERT INTO null_source VALUES
            (1, 'With Value', 'value'),
            (2, 'With NULL', NULL),
            (3, 'With Empty', '')
    """)

    connector.execute("""
        CREATE TABLE null_target (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            optional_field VARCHAR
        )
    """)

    # Same data - should match exactly
    connector.execute("""
        INSERT INTO null_target VALUES
            (1, 'With Value', 'value'),
            (2, 'With NULL', NULL),
            (3, 'With Empty', '')
    """)

    return connector


@pytest.fixture
def schema_mismatch_tables(connector: DuckDBConnector):
    """Create tables with different schemas for testing schema comparison."""
    connector.execute("""
        CREATE TABLE schema_source (
            id INTEGER PRIMARY KEY,
            name VARCHAR,
            old_column VARCHAR,
            shared_column INTEGER
        )
    """)

    connector.execute("""
        CREATE TABLE schema_target (
            id INTEGER PRIMARY KEY,
            name TEXT,  -- Different type (VARCHAR vs TEXT)
            new_column VARCHAR,  -- Different column
            shared_column INTEGER
        )
    """)

    return connector


@pytest.fixture
def temp_parquet_files():
    """Create temporary parquet files for CLI testing (e.g. count command)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        source_path = Path(tmpdir) / "source.parquet"
        target_path = Path(tmpdir) / "target.parquet"

        conn = duckdb.connect()
        conn.execute("CREATE TABLE source (id INT, name VARCHAR, value INT)")
        conn.execute("INSERT INTO source VALUES (1, 'a', 100), (2, 'b', 200)")
        conn.execute(f"COPY source TO '{source_path}'")

        conn.execute("CREATE TABLE target (id INT, name VARCHAR, value INT)")
        conn.execute("INSERT INTO target VALUES (1, 'a', 100), (2, 'b', 200)")
        conn.execute(f"COPY target TO '{target_path}'")
        conn.close()

        yield str(source_path), str(target_path)


@pytest.fixture
def temp_duckdb_count_tables():
    """Create a temporary DuckDB file with two tables (same row count) and config for count CLI tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        db_path = tmpdir_path / "count_test.duckdb"
        config_path = tmpdir_path / "quack-diff.yaml"

        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE TABLE t1 (id INT, name VARCHAR)")
        conn.execute("INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        conn.execute("CREATE TABLE t2 (id INT, name VARCHAR)")
        conn.execute("INSERT INTO t2 VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        conn.close()

        config_path.write_text(
            f"databases:\n  countdb:\n    type: duckdb\n    path: {db_path}\n",
            encoding="utf-8",
        )

        yield str(config_path), ["countdb.t1", "countdb.t2"]
