"""Tests for input validators."""
import pytest
from datetime import date, datetime, timedelta

from src.schemas.validators import (
    validate_symbol,
    validate_date,
    validate_optional_date,
    validate_interval,
    validate_limit,
    validate_offset,
    validate_exchange,
    validate_optional_exchange,
    validate_segment,
    validate_chart_type,
    validate_drawing_tool,
    validate_time_range,
)


class TestSymbolValidation:
    """Tests for validate_symbol."""

    def test_valid_symbol(self):
        assert validate_symbol("VNM") == "VNM"
        assert validate_symbol("vic") == "VIC"  # uppercase
        assert validate_symbol("FPT") == "FPT"

    def test_symbol_stripped_and_uppercase(self):
        assert validate_symbol("  abc  ") == "ABC"

    def test_symbol_too_short(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_symbol("")

    def test_symbol_too_long(self):
        with pytest.raises(ValueError, match="Invalid symbol format"):
            validate_symbol("ABCDE")  # 5 letters

    def test_symbol_with_numbers(self):
        with pytest.raises(ValueError, match="Invalid symbol format"):
            validate_symbol("VNM1")

    def test_symbol_lowercase_only(self):
        """Should convert lowercase to uppercase and accept."""
        assert validate_symbol("vnm") == "VNM"  # lowercase is converted to uppercase

    def test_symbol_reserved_code(self):
        """Should reject reserved segment/exchange codes."""
        with pytest.raises(ValueError, match="reserved"):
            validate_symbol("ALL")
        with pytest.raises(ValueError, match="reserved"):
            validate_symbol("HOSE")
        # VN30 fails pattern check (contains digits), not reserved check
        with pytest.raises(ValueError, match="Invalid symbol format"):
            validate_symbol("VN30")


class TestDateValidation:
    """Tests for validate_date."""

    def test_valid_date(self):
        today = date.today().isoformat()
        assert validate_date(today) == today

    def test_valid_date_format(self):
        assert validate_date("2026-04-30") == "2026-04-30"

    def test_date_in_future(self):
        future = (date.today() + timedelta(days=1)).isoformat()
        with pytest.raises(ValueError, match="cannot be in the future"):
            validate_date(future)

    def test_date_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid date format"):
            validate_date("04/30/2026")
        with pytest.raises(ValueError, match="Invalid date format"):
            validate_date("2026-04-30 12:00:00")


class TestOptionalDateValidation:
    """Tests for validate_optional_date."""

    def test_none_returns_none(self):
        assert validate_optional_date(None) is None

    def test_valid_date_returns_date(self):
        assert validate_optional_date("2026-04-30") == "2026-04-30"


class TestIntervalValidation:
    """Tests for validate_interval."""

    def test_valid_intervals(self):
        assert validate_interval("5m") == "5m"
        assert validate_interval("1h") == "1h"
        assert validate_interval("1d") == "1d"

    def test_invalid_interval(self):
        with pytest.raises(ValueError, match="Invalid interval"):
            validate_interval("10m")
        with pytest.raises(ValueError, match="Invalid interval"):
            validate_interval("2h")


class TestLimitValidation:
    """Tests for validate_limit."""

    def test_valid_limit(self):
        assert validate_limit(1) == 1
        assert validate_limit(100) == 100
        assert validate_limit(1000) == 1000

    def test_limit_too_small(self):
        with pytest.raises(ValueError, match="between 1 and 1000"):
            validate_limit(0)

    def test_limit_too_large(self):
        with pytest.raises(ValueError, match="between 1 and 1000"):
            validate_limit(1001)

    def test_limit_not_integer(self):
        with pytest.raises(ValueError, match="must be an integer"):
            validate_limit(100.5)


class TestOffsetValidation:
    """Tests for validate_offset."""

    def test_valid_offset(self):
        assert validate_offset(0) == 0
        assert validate_offset(100) == 100

    def test_negative_offset(self):
        with pytest.raises(ValueError, match="cannot be negative"):
            validate_offset(-1)


class TestExchangeValidation:
    """Tests for validate_exchange."""

    def test_valid_exchanges(self):
        assert validate_exchange("HOSE") == "HOSE"
        assert validate_exchange("HNX") == "HNX"
        assert validate_exchange("UPCO") == "UPCO"

    def test_invalid_exchange(self):
        with pytest.raises(ValueError, match="Invalid exchange"):
            validate_exchange("ABC")


class TestSegmentValidation:
    """Tests for validate_segment."""

    def test_valid_segments(self):
        assert validate_segment("ALL") == "ALL"
        assert validate_segment("HOSE") == "HOSE"
        assert validate_segment("VN30") == "VN30"
        assert validate_segment("HNX30") == "HNX30"

    def test_invalid_segment(self):
        with pytest.raises(ValueError, match="Invalid segment"):
            validate_segment("WARRANT")


class TestChartTypeValidation:
    """Tests for validate_chart_type."""

    def test_valid_chart_types(self):
        assert validate_chart_type("candlestick") == "candlestick"
        assert validate_chart_type("line") == "line"

    def test_invalid_chart_type(self):
        with pytest.raises(ValueError, match="Invalid chart type"):
            validate_chart_type("pie")


class TestDrawingToolValidation:
    """Tests for validate_drawing_tool."""

    def test_valid_tools(self):
        assert validate_drawing_tool("trend_line") == "trend_line"
        assert validate_drawing_tool("rectangle") == "rectangle"

    def test_invalid_tool(self):
        with pytest.raises(ValueError, match="Invalid drawing tool"):
            validate_drawing_tool("unknown_tool")


class TestTimeRangeValidation:
    """Tests for validate_time_range."""

    def test_valid_range(self):
        from_t, to_t = validate_time_range("09:30:00", "15:30:00")
        assert from_t == "09:30:00"
        assert to_t == "15:30:00"

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid from_time format"):
            validate_time_range("9:30", "15:30")
        with pytest.raises(ValueError, match="Invalid to_time format"):
            validate_time_range("09:30:00", "3:30pm")

    def test_from_after_to(self):
        with pytest.raises(ValueError, match="from_time must be earlier"):
            validate_time_range("15:30:00", "09:30:00")

    def test_none_values(self):
        assert validate_time_range(None, "15:30:00") == (None, "15:30:00")
        assert validate_time_range("09:30:00", None) == ("09:30:00", None)
        assert validate_time_range(None, None) == (None, None)
