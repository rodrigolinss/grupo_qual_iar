"""Command line interface for brasilia_air_quality.

This module uses the Typer library to expose a set of commands that drive the
data pipeline: discovery of sources, extraction of raw data, normalization to
a canonical schema, validation of the resulting datasets and export to CSV or
Parquet.  Each command is designed to be idempotent so that the pipeline can
be executed incrementally over time.

Usage examples::

    # discover potential sources and save the index
    python -m br.aqi.cli discover

    # extract all available data since January 2020
    python -m br.aqi.cli extract --since 2020-01-01 --until today

    # normalize and validate the data
    python -m br.aqi.cli normalize && python -m br.aqi.cli validate

    # export processed data to CSV (default)
    python -m br.aqi.cli export

"""
from __future__ import annotations

import asyncio
import json
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import typer

from .rag import crawl_candidates, rank_sources, plan_per_source
from .sources import get_sources
from .normalize import normalize_dataframe
from .validate import validate_dataframe
from .export import export_to_csv
from .utils import parse_date


app = typer.Typer(add_completion=False, help="Brasília air quality data pipeline")


@app.command()
def discover() -> None:
    """Discover candidate data sources and save the ranked index to artifacts.

    This command uses the RAG (Retrieval Augmented Generation) component to
    crawl and score candidate sources.  The resulting index is stored in
    ``artifacts/sources_index.json``.
    """

    async def _run() -> None:
        candidates = await crawl_candidates()
        ranked = rank_sources(candidates)
        # Write index to artifacts
        artifacts_dir = Path("artifacts")
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        index_path = artifacts_dir / "sources_index.json"
        with index_path.open("w", encoding="utf-8") as fh:
            json.dump(ranked, fh, indent=2, ensure_ascii=False)
        typer.echo(f"Saved {len(ranked)} sources to {index_path}")

    asyncio.run(_run())


@app.command()
def extract(
    since: str = typer.Option(
        "2020-01-01",
        help="Earliest date (ISO format) from which to retrieve data",
        show_default=True,
    ),
    until: str = typer.Option(
        "today",
        help="Latest date (ISO format) until which to retrieve data; 'today' for now",
        show_default=True,
    ),
) -> None:
    """Download raw data from all configured sources.

    The extraction step is idempotent and supports incremental updates.  Data
    downloaded from each source is cached under ``artifacts/cache`` to avoid
    re-fetching unchanged periods.  The raw datasets are stored in
    ``data/bronze``.
    """
    start = parse_date(since)
    end = parse_date(until)
    if end < start:
        raise typer.BadParameter("until date must be on or after since date")

    # Ensure data directories exist
    data_dir = Path("data/bronze")
    data_dir.mkdir(parents=True, exist_ok=True)
    cache_dir = Path("artifacts/cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    async def _run() -> None:
        sources = get_sources()
        tasks = []
        for source in sources:
            tasks.append(source.extract(start, end, cache_dir, data_dir))
        await asyncio.gather(*tasks)
        typer.echo(f"Extraction complete for {len(tasks)} sources")

    asyncio.run(_run())


@app.command()
def normalize() -> None:
    """Normalize raw datasets to the canonical schema and write to silver layer."""
    import pandas as pd

    bronze_dir = Path("data/bronze")
    silver_dir = Path("data/silver")
    silver_dir.mkdir(parents=True, exist_ok=True)
    for raw_file in bronze_dir.glob("*.csv"):
        df = pd.read_csv(raw_file)
        norm = normalize_dataframe(df)
        out_path = silver_dir / raw_file.name
        norm.to_csv(out_path, index=False)
        typer.echo(f"Normalized {raw_file} -> {out_path}")


@app.command()
def validate() -> None:
    """Validate the normalized datasets and report any issues."""
    import pandas as pd
    from rich import print as rprint

    silver_dir = Path("data/silver")
    success = True
    for file in silver_dir.glob("*.csv"):
        df = pd.read_csv(file)
        report = validate_dataframe(df)
        if report:
            success = False
            rprint(f"[bold red]Validation issues in {file}:[/bold red]")
            for issue in report:
                rprint(f" - {issue}")
    if not success:
        raise typer.Exit(code=1)
    typer.echo("All files passed validation")


@app.command()
def export(format: str = typer.Option("csv", help="Output format: csv or parquet")) -> None:
    """Export normalized datasets to the target format.

    The canonical format is CSV, but Parquet is also supported if the
    ``pyarrow`` dependency is installed.
    """
    import pandas as pd

    silver_dir = Path("data/silver")
    export_dir = Path("data/export")
    export_dir.mkdir(parents=True, exist_ok=True)
    for file in silver_dir.glob("*.csv"):
        df = pd.read_csv(file)
        export_to_csv(df, export_dir)  # always write CSV
        if format.lower() == "parquet":
            try:
                import pyarrow  # noqa: F401
                pq_path = export_dir / (file.stem + ".parquet")
                df.to_parquet(pq_path)
                typer.echo(f"Wrote {pq_path}")
            except ImportError:
                typer.echo("pyarrow is not installed; skipping Parquet export", err=True)
    typer.echo("Export completed")


if __name__ == "__main__":  # pragma: no cover
    app()