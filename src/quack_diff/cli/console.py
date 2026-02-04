"""Rich console singleton for consistent terminal output."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.theme import Theme

if TYPE_CHECKING:
    from collections.abc import Generator

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
        "progress.description": "cyan",
        "progress.percentage": "green",
    }
)

# Global console instance
console = Console(theme=QUACK_THEME)
error_console = Console(stderr=True, theme=QUACK_THEME)

# Global flag for JSON output mode (suppresses Rich output)
_json_output_mode = False


def set_json_output_mode(enabled: bool) -> None:
    """Enable or disable JSON output mode.

    When enabled, Rich console output is suppressed in favor of JSON.
    """
    global _json_output_mode
    _json_output_mode = enabled


def is_json_output_mode() -> bool:
    """Check if JSON output mode is enabled."""
    return _json_output_mode


def print_info(message: str) -> None:
    """Print an info message."""
    if not _json_output_mode:
        console.print(f"[info]{message}[/info]")


def print_success(message: str) -> None:
    """Print a success message."""
    if not _json_output_mode:
        console.print(f"[success]{message}[/success]")


def print_warning(message: str) -> None:
    """Print a warning message."""
    if not _json_output_mode:
        console.print(f"[warning]{message}[/warning]")


def print_error(message: str) -> None:
    """Print an error message to stderr."""
    if not _json_output_mode:
        error_console.print(f"[error]{message}[/error]")


def create_progress(
    description: str = "Processing",
    transient: bool = True,
) -> Progress:
    """Create a Rich progress bar with consistent styling.

    Args:
        description: Description text for the progress bar
        transient: If True, progress bar disappears when complete

    Returns:
        Configured Progress instance
    """
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=transient,
        disable=_json_output_mode,
    )


def create_spinner(description: str = "Working...") -> Progress:
    """Create a simple spinner for indeterminate operations.

    Args:
        description: Description text to show with spinner

    Returns:
        Configured Progress instance with spinner only
    """
    return Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
        disable=_json_output_mode,
    )


@contextmanager
def progress_context(
    description: str = "Processing",
    total: float | None = None,
) -> Generator[tuple[Progress, Any], None, None]:
    """Context manager for progress tracking.

    Args:
        description: Description for the progress task
        total: Total units of work (None for indeterminate)

    Yields:
        Tuple of (Progress instance, task_id)

    Example:
        with progress_context("Loading data", total=100) as (progress, task):
            for i in range(100):
                do_work()
                progress.advance(task)
    """
    progress = create_spinner(description) if total is None else create_progress(description)

    with progress:
        task = progress.add_task(description, total=total)
        yield progress, task


@contextmanager
def status_context(message: str) -> Generator[None, None, None]:
    """Context manager for showing a status spinner.

    Args:
        message: Status message to display

    Yields:
        None

    Example:
        with status_context("Connecting to Snowflake..."):
            connect_to_snowflake()
    """
    if _json_output_mode:
        yield
        return

    with console.status(f"[info]{message}[/info]", spinner="dots"):
        yield
