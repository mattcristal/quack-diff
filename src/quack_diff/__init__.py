__version__ = "0.0.9"
__author__ = "Matteo Renoldi"

from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer
from quack_diff.core.sql_utils import SQLInjectionError

__all__ = ["DataDiffer", "DuckDBConnector", "SQLInjectionError", "__version__"]
