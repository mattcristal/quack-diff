"""Tests for quack_diff.core.utils."""

import pytest

from quack_diff.core.utils import parse_offset_to_seconds


class TestParseOffsetToSeconds:
    """Tests for parse_offset_to_seconds function."""

    def test_seconds(self):
        """Parse seconds offset."""
        assert parse_offset_to_seconds("30 seconds") == 30

    def test_seconds_singular(self):
        """Parse singular second."""
        assert parse_offset_to_seconds("1 second") == 1

    def test_seconds_plural_with_value_one(self):
        """Parse plural form with value 1."""
        assert parse_offset_to_seconds("1 seconds") == 1

    def test_minutes(self):
        """Parse minutes offset."""
        assert parse_offset_to_seconds("5 minutes") == 300

    def test_minute_singular(self):
        """Parse singular minute."""
        assert parse_offset_to_seconds("1 minute") == 60

    def test_hours(self):
        """Parse hours offset."""
        assert parse_offset_to_seconds("1 hour") == 3600

    def test_hours_plural(self):
        """Parse plural hours."""
        assert parse_offset_to_seconds("2 hours") == 7200

    def test_days(self):
        """Parse days offset."""
        assert parse_offset_to_seconds("2 days") == 172800

    def test_day_singular(self):
        """Parse singular day."""
        assert parse_offset_to_seconds("1 day") == 86400

    def test_weeks(self):
        """Parse weeks offset."""
        assert parse_offset_to_seconds("1 week") == 604800

    def test_weeks_plural(self):
        """Parse plural weeks."""
        assert parse_offset_to_seconds("2 weeks") == 1209600

    def test_ago_format(self):
        """Parse 'X ago' format has ' ago' stripped before parsing."""
        assert parse_offset_to_seconds("5 minutes ago") == 300

    def test_ago_format_with_hours(self):
        """Parse 'X hours ago' format."""
        assert parse_offset_to_seconds("2 hours ago") == 7200

    def test_case_insensitive(self):
        """Parsing is case insensitive."""
        assert parse_offset_to_seconds("5 MINUTES") == 300
        assert parse_offset_to_seconds("5 Minutes Ago") == 300

    def test_whitespace_handling(self):
        """Extra whitespace is handled."""
        assert parse_offset_to_seconds("  5 minutes  ") == 300
        assert parse_offset_to_seconds("5  minutes") == 300

    def test_invalid_format_no_number(self):
        """String without number raises ValueError."""
        with pytest.raises(ValueError, match="Invalid offset format"):
            parse_offset_to_seconds("minutes")

    def test_invalid_format_extra_parts(self):
        """Too many parts raises ValueError."""
        with pytest.raises(ValueError, match="Invalid offset format"):
            parse_offset_to_seconds("5 minutes extra words")

    def test_invalid_numeric_value(self):
        """Non-numeric first part raises ValueError."""
        with pytest.raises(ValueError, match="Invalid offset format"):
            parse_offset_to_seconds("five minutes")

    def test_unknown_time_unit(self):
        """Unknown unit raises ValueError."""
        with pytest.raises(ValueError, match="Invalid offset format"):
            parse_offset_to_seconds("5 fortnights")

    def test_empty_string(self):
        """Empty string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid offset format"):
            parse_offset_to_seconds("")

    def test_only_whitespace(self):
        """Whitespace-only string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid offset format"):
            parse_offset_to_seconds("   ")
