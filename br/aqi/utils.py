"""Utility functions for brasilia_air_quality.

Helper functions centralise date parsing, timezone handling and other common
operations used throughout the pipeline.  By concentrating logic here we
encourage reuse and ease testing.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Union

from dateutil import tz

LOCAL_TZ = tz.gettz("America/Sao_Paulo")


def parse_date(value: str) -> date:
    """Parse a string into a date.

    Accepts ISO 8601 dates (YYYY-MM-DD) and the special string ``today``.
    Raises ``ValueError`` if the input cannot be parsed.
    """
    if value.lower() == "today":
        return datetime.now(tz=LOCAL_TZ).date()
    return datetime.strptime(value, "%Y-%m-%d").date()


def ensure_datetime(value: Union[str, datetime]) -> datetime:
    """Ensure that ``value`` is a ``datetime`` with timezone information.

    Strings are parsed using pandas if necessary.  Naive datetimes are assumed
    to be in local time and converted to timezone-aware objects.
    """
    import pandas as pd

    if isinstance(value, datetime):
        dt = value
    else:
        dt = pd.to_datetime(value).to_pydatetime()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LOCAL_TZ)
    return dt


def to_utc(dt: datetime) -> datetime:
    """Convert a datetime to UTC."""
    return dt.astimezone(timezone.utc)