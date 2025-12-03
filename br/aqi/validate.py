"""Validation routines for the normalized air quality data.

Validation checks ensure that the canonical datasets contain plausible and
internally consistent information.  If any records violate the rules the
validator returns a list of human‑readable issues.  The pipeline will fail
during CI if any issues are reported.
"""
from __future__ import annotations

from datetime import datetime
from typing import List

import pandas as pd


RANGES = {
    "pm25": (0, 1000),
    "pm10": (0, 1000),
    "o3": (0, 200),
    "no2": (0, 400),
    "so2": (0, 400),
    "co": (0, 10000),
}


def validate_dataframe(df: pd.DataFrame) -> List[str]:
    """Validate a normalized dataframe and return a list of issues.

    Each issue is a string describing the problem.  An empty list indicates
    that the dataframe passed all checks.
    """
    issues: List[str] = []
    # Check pollutant values
    for idx, row in df.iterrows():
        pollutant = row.get("pollutant")
        value = row.get("value")
        if pd.isna(value):
            continue
        try:
            val = float(value)
        except (TypeError, ValueError):
            issues.append(f"Row {idx}: value '{value}' is not a number")
            continue
        if pollutant in RANGES:
            lo, hi = RANGES[pollutant]
            if not (lo <= val <= hi):
                issues.append(
                    f"Row {idx}: {pollutant} concentration {val} outside plausible range [{lo}, {hi}]"
                )
    # Check timestamps monotonic
    try:
        dt_series = pd.to_datetime(df["datetime_utc"])
        if not dt_series.is_monotonic_increasing:
            issues.append("Timestamps are not strictly non‑decreasing")
    except Exception:
        issues.append("Invalid datetime_utc values")
    # Check coordinates within Brazil bounding box
    for idx, row in df.iterrows():
        lat, lon = row.get("latitude"), row.get("longitude")
        try:
            if pd.notna(lat) and pd.notna(lon):
                if not (-33 <= float(lat) <= 5 and -74 <= float(lon) <= -34):
                    issues.append(
                        f"Row {idx}: coordinates ({lat}, {lon}) outside Brazil bounds"
                    )
        except Exception:
            issues.append(f"Row {idx}: invalid coordinate values ({lat}, {lon})")
    # Check required columns exist
    required = {
        "datetime_utc",
        "datetime_local",
        "station_id",
        "station_name",
        "pollutant",
        "value",
        "unit",
        "avg_period_minutes",
        "source_url",
        "source_agency",
        "ingested_at_utc",
        "quality_flag",
    }
    missing = required - set(df.columns)
    if missing:
        issues.append(f"Missing required columns: {', '.join(sorted(missing))}")
    return issues