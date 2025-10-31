# Quickstart

This quickstart guide walks you through setting up the development
environment, discovering data sources and running the extraction pipeline.  It
assumes you are familiar with Python and have Docker installed if you intend
to use containerisation.

## Prerequisites

* **Python 3.11 or later.**  The pipeline uses recent features such as
  `zoneinfo` and structural pattern matching.
* **Poetry or virtualenv** (optional) to create an isolated environment.

## Setup

Clone this repository and create a virtual environment::

    git clone https://github.com/your‑username/brasilia-air-quality.git
    cd brasilia-air-quality
    python -m venv .venv
    source .venv/bin/activate

Install the dependencies using `pip`::

    pip install -r requirements.txt

Alternatively, run the provided Makefile target::

    make setup

which will create a virtual environment under `.venv` and install the
dependencies using `pip-tools` to lock the versions.  The locked
dependencies are written to `requirements.txt`.

## Discover sources

To discover candidate data sources run::

    python -m br.aqi.cli discover

This command invokes the RAG module to collect and rank official sources.  The
result is saved to `artifacts/sources_index.json`.  You can inspect this file
to see how each source was scored and what extraction plan will be applied.

## Extract raw data

Use the `extract` command to download raw data from all configured sources::

    python -m br.aqi.cli extract --since 2020-01-01 --until today

The raw datasets are saved into `data/bronze`.  If a source provides
incremental downloads you can run the command repeatedly and only new data
will be fetched.

## Normalize and validate

Normalize the raw data into a canonical schema::

    python -m br.aqi.cli normalize

This writes CSV files into `data/silver` with unified pollutant codes and
units.  To verify that the normalized data are plausible run::

    python -m br.aqi.cli validate

If any issues are detected the command will exit with a non‑zero status and
print a list of problems.

## Export

To partition and export the normalized data by year and month run::

    python -m br.aqi.cli export

This writes CSV files into `data/export/year=YYYY/month=MM/`.  If you have
installed `pyarrow` you can request Parquet output with the `--format parquet`
option.

## Makefile targets

For convenience the most common operations are wrapped into Makefile targets::

    make setup        # Install dependencies in a virtualenv
    make discover     # Run source discovery
    make extract      # Download raw data
    make normalize    # Normalize data
    make validate     # Validate normalized data
    make export       # Export to CSV
    make all          # Run the full pipeline (discover → extract → normalize → validate → export)

Additional targets exist to build and run the Docker image and to clean
generated files.  See the Makefile for details.