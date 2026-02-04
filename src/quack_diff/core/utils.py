"""Core utility functions.

Provides shared utility functions used across the quack_diff.core module.
"""

from __future__ import annotations

import re


def parse_offset_to_seconds(offset: str) -> int:
    """Parse a human-readable time offset to seconds.

    Supports formats like:
    - "5 minutes ago"
    - "1 hour ago"
    - "30 seconds ago"
    - "5 minutes"
    - "1 hour"

    Args:
        offset: Human-readable offset string

    Returns:
        Number of seconds

    Raises:
        ValueError: If offset format is not recognized

    Example:
        >>> parse_offset_to_seconds("5 minutes ago")
        300
        >>> parse_offset_to_seconds("1 hour")
        3600
        >>> parse_offset_to_seconds("30 seconds")
        30
    """
    offset_lower = offset.lower().strip()

    # Remove "ago" suffix if present
    if offset_lower.endswith(" ago"):
        offset_lower = offset_lower[:-4].strip()

    # Parse number and unit using regex
    match = re.match(r"(\d+)\s*(second|minute|hour|day|week)s?$", offset_lower)
    if not match:
        raise ValueError(f"Invalid offset format: '{offset}'. Expected format like '5 minutes ago' or '1 hour'")

    value = int(match.group(1))
    unit = match.group(2)

    multipliers = {
        "second": 1,
        "minute": 60,
        "hour": 3600,
        "day": 86400,
        "week": 604800,
    }

    return value * multipliers[unit]
