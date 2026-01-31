__version__ = "0.0.5"
__author__ = "Matteo Renoldi"

from quack_diff.core.connector import DuckDBConnector
from quack_diff.core.differ import DataDiffer

__all__ = ["DataDiffer", "DuckDBConnector", "__version__"]
