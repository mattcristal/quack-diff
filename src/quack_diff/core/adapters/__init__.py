"""SQL dialect adapters for different database backends."""

from quack_diff.core.adapters.base import BaseAdapter, Dialect
from quack_diff.core.adapters.duckdb import DuckDBAdapter
from quack_diff.core.adapters.snowflake import SnowflakeAdapter

__all__ = [
    "BaseAdapter",
    "Dialect",
    "DuckDBAdapter",
    "SnowflakeAdapter",
]
