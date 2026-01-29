"""Core diffing engine and database connectivity."""

from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer
from quack_diff.core.query_builder import QueryBuilder

__all__ = ["DuckDBConnector", "DataDiffer", "QueryBuilder"]
