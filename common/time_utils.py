import datetime
import logging
import re
import time

def current_timestamp():
    return int(time.time() * 1000)

def timestamp_to_date(timestamp, format_string="%Y-%m-%d %H:%M:%S"):
    """
    Convert a unix timestamp (seconds or milliseconds) to a formatted date string.
    Automatically detects whether the timestamp is in seconds (< 10^10) or milliseconds (>= 10^10).
    """
    if not timestamp:
        timestamp = time.time()
    ts = float(timestamp)
    # Normalise milliseconds → seconds
    if ts > 10**10:
        ts = ts / 1000.0
    time_array = time.localtime(int(ts))
    return time.strftime(format_string, time_array)

def date_string_to_timestamp(time_str, format_string="%Y-%m-%d %H:%M:%S"):
    time_array = time.strptime(time_str, format_string)
    time_stamp = int(time.mktime(time_array) * 1000)
    return time_stamp

def datetime_format(date_time: datetime.datetime) -> datetime.datetime:
    """Return a datetime with microseconds stripped (tz-aware tz-naive handling)."""
    return date_time.replace(microsecond=0)

def get_format_time() -> datetime.datetime:
    return datetime_format(datetime.datetime.now())

def delta_seconds(date_string: str):
    """
    Return seconds between a naive date string and now.

    Handles ISO 8601 strings with a +HH:MM / -HH:MM timezone suffix by stripping
    the offset before parsing (converts local-time string to a naive datetime for comparison).
    """
    # Strip trailing Z or +HH:MM / -HH:MM suffix
    cleaned = re.sub(r"[+-]\d{2}:\d{2}$", "", date_string.strip()).rstrip("Z")
    dt = datetime.datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
    return (datetime.datetime.now() - dt).total_seconds()

