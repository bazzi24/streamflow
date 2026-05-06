"""
Input Validation Module
──────────────────────
Centralized validation functions for API inputs.
All validators raise ValueError on invalid input.
"""

import re
from datetime import date, datetime
from typing import Optional


# ── Symbol Validation ────────────────────────────────────────────────────────

VIETNAMESE_SYMBOL_PATTERN = re.compile(r"^[A-Z]{3,4}$")
EXCLUDED_SYMBOLS = {"ALL", "HOSE", "HNX", "UPCO", "VN30", "HNX30"}

def validate_symbol(symbol: str) -> str:
    """
    Validate a Vietnamese stock symbol.
    - Must be 3-4 uppercase letters
    - Cannot be reserved segment/exchange codes
    Raises ValueError if invalid.
    """
    if not isinstance(symbol, str):
        raise ValueError("Symbol must be a string")
    symbol = symbol.strip().upper()
    if not symbol:
        raise ValueError("Symbol cannot be empty")
    if not VIETNAMESE_SYMBOL_PATTERN.match(symbol):
        raise ValueError(
            f"Invalid symbol format '{symbol}'. Expected 3-4 uppercase letters (e.g., VNM, VIC, FPT)."
        )
    if symbol in EXCLUDED_SYMBOLS:
        raise ValueError(f"'{symbol}' is a reserved segment/exchange code, not a valid symbol")
    return symbol


# ── Date Validation ──────────────────────────────────────────────────────────

DATE_FORMAT = "%Y-%m-%d"

def validate_date(date_str: str) -> str:
    """
    Validate date string in YYYY-MM-DD format.
    - Must be parseable as date
    - Cannot be in the future
    Raises ValueError if invalid.
    """
    if not isinstance(date_str, str):
        raise ValueError("Date must be a string")
    try:
        parsed = datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError:
        raise ValueError(f"Invalid date format '{date_str}'. Expected YYYY-MM-DD.")
    today = date.today()
    if parsed > today:
        raise ValueError(f"Date '{date_str}' cannot be in the future")
    return date_str


def validate_optional_date(date_str: Optional[str]) -> Optional[str]:
    """Validate optional date string. Returns None if input is None."""
    if date_str is None:
        return None
    return validate_date(date_str)


# ── Time Interval Validation ──────────────────────────────────────────────────

VALID_INTERVALS = {"5m", "15m", "30m", "1h", "1d", "1w", "1M"}

def validate_interval(interval: str) -> str:
    """
    Validate time interval.
    Must be one of: 5m, 15m, 30m, 1h, 1d, 1w, 1M
    """
    if interval not in VALID_INTERVALS:
        raise ValueError(
            f"Invalid interval '{interval}'. Must be one of: {', '.join(sorted(VALID_INTERVALS))}"
        )
    return interval


# ── Limit/Offset Validation ───────────────────────────────────────────────────

def validate_limit(limit: int) -> int:
    """
    Validate pagination limit.
    - Must be between 1 and 1000
    """
    if not isinstance(limit, int):
        raise ValueError("Limit must be an integer")
    if limit < 1 or limit > 1000:
        raise ValueError("Limit must be between 1 and 1000")
    return limit


def validate_offset(offset: int) -> int:
    """
    Validate pagination offset.
    - Must be non-negative
    """
    if not isinstance(offset, int):
        raise ValueError("Offset must be an integer")
    if offset < 0:
        raise ValueError("Offset cannot be negative")
    return offset


# ── Exchange Validation ───────────────────────────────────────────────────────

VALID_EXCHANGES = {"HOSE", "HNX", "UPCO"}

def validate_exchange(exchange: str) -> str:
    """
    Validate exchange code.
    Must be one of: HOSE, HNX, UPCO
    """
    if exchange not in VALID_EXCHANGES:
        raise ValueError(
            f"Invalid exchange '{exchange}'. Must be one of: {', '.join(sorted(VALID_EXCHANGES))}"
        )
    return exchange


def validate_optional_exchange(exchange: Optional[str]) -> Optional[str]:
    """Validate optional exchange. Returns None if input is None."""
    if exchange is None:
        return None
    return validate_exchange(exchange)


# ── Market Segment Validation ────────────────────────────────────────────────

VALID_SEGMENTS = {"ALL", "HOSE", "HNX", "UPCO", "VN30", "HNX30"}

def validate_segment(segment: str) -> str:
    """
    Validate market segment.
    Must be one of: ALL, HOSE, HNX, UPCO, VN30, HNX30
    """
    if segment not in VALID_SEGMENTS:
        raise ValueError(
            f"Invalid segment '{segment}'. Must be one of: {', '.join(sorted(VALID_SEGMENTS))}"
        )
    return segment


# ── Chart Type Validation ─────────────────────────────────────────────────────

VALID_CHART_TYPES = {"candlestick", "line", "area", "bar"}

def validate_chart_type(chart_type: str) -> str:
    """
    Validate chart type.
    Must be one of: candlestick, line, area, bar
    """
    if chart_type not in VALID_CHART_TYPES:
        raise ValueError(
            f"Invalid chart type '{chart_type}'. Must be one of: {', '.join(sorted(VALID_CHART_TYPES))}"
        )
    return chart_type


# ── Drawing Tool Validation ───────────────────────────────────────────────────

VALID_DRAWING_TOOLS = {
    "trend_line", "horizontal_line", "vertical_line", "fibonacci", "rectangle",
    "ellipse", "text", "arrow", "price_label", "time_label"
}

def validate_drawing_tool(tool: str) -> str:
    """
    Validate drawing tool identifier.
    """
    if tool not in VALID_DRAWING_TOOLS:
        raise ValueError(f"Invalid drawing tool '{tool}'")
    return tool


# ── Time Range Validation ─────────────────────────────────────────────────────

TIME_PATTERN = re.compile(r"^\d{2}:\d{2}:\d{2}$")

def validate_time_range(
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    Validate a time range (HH:MM:SS format).
    Returns (from_time, to_time) validated.
    If either is None, returns (None, None).
    """
    if from_time is None or to_time is None:
        return from_time, to_time

    if not TIME_PATTERN.match(from_time):
        raise ValueError(f"Invalid from_time format '{from_time}'. Expected HH:MM:SS")
    if not TIME_PATTERN.match(to_time):
        raise ValueError(f"Invalid to_time format '{to_time}'. Expected HH:MM:SS")

    try:
        from_dt = datetime.strptime(from_time, "%H:%M:%S").time()
        to_dt = datetime.strptime(to_time, "%H:%M:%S").time()
    except ValueError:
        raise ValueError("Invalid time format, expected HH:MM:SS (24-hour)")

    if from_dt >= to_dt:
        raise ValueError("from_time must be earlier than to_time")

    return from_time, to_time

