"""Rich console singleton for consistent terminal output."""

from rich.console import Console
from rich.theme import Theme

# Custom theme for quack-diff
QUACK_THEME = Theme(
    {
        "info": "cyan",
        "success": "green",
        "warning": "yellow",
        "error": "red bold",
        "highlight": "magenta",
        "key": "blue bold",
        "added": "green",
        "removed": "red",
        "modified": "yellow",
        "header": "bold cyan",
        "muted": "dim",
    }
)

# Global console instance
console = Console(theme=QUACK_THEME)
error_console = Console(stderr=True, theme=QUACK_THEME)


def print_info(message: str) -> None:
    """Print an info message."""
    console.print(f"[info]{message}[/info]")


def print_success(message: str) -> None:
    """Print a success message."""
    console.print(f"[success]{message}[/success]")


def print_warning(message: str) -> None:
    """Print a warning message."""
    console.print(f"[warning]{message}[/warning]")


def print_error(message: str) -> None:
    """Print an error message to stderr."""
    error_console.print(f"[error]{message}[/error]")
