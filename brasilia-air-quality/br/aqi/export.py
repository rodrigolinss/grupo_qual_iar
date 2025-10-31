"""Export normalized data to target formats.

This module provides functions to write normalized pandas dataframes to CSV
files organised by year and month.  Partitioning the output helps to keep
individual files manageable and facilitates incremental updates.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def export_to_csv(df: pd.DataFrame, root_dir: Path) -> None:
    """Write a dataframe to CSV partitioned by year and month.

    Parameters
    ----------
    df : pandas.DataFrame
        Normalized data with at least a ``datetime_local`` column.
    root_dir : pathlib.Path
        Directory into which the partitioned files will be written.
    """
    if df.empty:
        return
    # Ensure timestamp is datetime
    dates = pd.to_datetime(df["datetime_local"])
    df = df.copy()
    df["year"] = dates.dt.year
    df["month"] = dates.dt.month
    for (year, month), group in df.groupby(["year", "month"]):
        part_dir = root_dir / f"year={year:04d}" / f"month={month:02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{year:04d}-{month:02d}.csv"
        group.drop(columns=["year", "month"]).to_csv(part_dir / filename, index=False)