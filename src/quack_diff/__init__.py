__version__ = "0.0.10"
__author__ = "Matteo Renoldi"

from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer
from quack_diff.core.sql_utils import (
    AttachError,
    DatabaseError,
    KeyColumnError,
    QueryExecutionError,
    SchemaError,
    SQLInjectionError,
    TableNotFoundError,
)

__all__ = [
    "AttachError",
    "DatabaseError",
    "DataDiffer",
    "DuckDBConnector",
    "KeyColumnError",
    "QueryExecutionError",
    "SchemaError",
    "SQLInjectionError",
    "TableNotFoundError",
    "__version__",
]
