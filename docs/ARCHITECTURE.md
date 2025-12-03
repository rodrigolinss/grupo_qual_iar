# Architecture

The **Brasília Air Quality** project is organised around a simple but
extensible data pipeline.  The design emphasises modularity, reproducibility
and observability.  Each stage is implemented as a Python module with a
clear responsibility and tested in isolation.

## Pipeline stages

1. **Discovery (RAG)** — The `br/aqi/rag.py` module implements a
   Retrieval Augmented Generation (RAG) pattern to discover official data
   sources.  In practice this involves crawling open data catalogues,
   environmental portals and ArcGIS endpoints and producing a ranked list of
   candidates.  The ranking heuristic prioritises official government sources,
   open formats and broad temporal coverage.  The result is persisted to
   `artifacts/sources_index.json`.

2. **Extraction** — Each candidate source corresponds to a connector
   implemented in `br/aqi/sources.py`.  Connectors inherit from the `Source`
   abstract base class and implement an asynchronous `extract()` method.  The
   extraction step retrieves raw data for a user‑specified time window,
   respecting caching (`artifacts/cache`), rate limiting and retry policies
   using `httpx` and `tenacity`.  Raw CSV files are written into the
   `data/bronze` layer.

3. **Normalization** — Raw data often use heterogeneous column names,
   timestamps and units.  The `br/aqi/normalize.py` module converts these
   datasets into a canonical schema.  It maps pollutant names to standard
   codes, converts units to µg/m³ and produces both UTC and local timestamps
   using Python’s `zoneinfo` module.  The normalized CSVs are stored in the
   `data/silver` layer.

4. **Validation** — Before further analysis or publication the normalized
   data are validated.  The `br/aqi/validate.py` module checks that values lie
   within plausible ranges (e.g. PM2.5 between 0–1000 µg/m³), timestamps are
   monotonic, coordinates fall within Brazil’s geographic boundaries and all
   required columns are present.  Any issues cause the CI pipeline to fail.

5. **Export** — Finally the `br/aqi/export.py` module partitions the silver
   datasets by year and month and writes them to `data/export` in CSV (and
   optionally Parquet) format.  Partitioning facilitates incremental
   updates and downstream processing.

## Observability

Structured logging is provided via the `structlog` library.  Each stage
emits messages with contextual information such as the number of records
processed or the URL being fetched.  When running in development the logs
are human‑readable; in production they can be configured to emit JSON for
aggregation.

## Scheduling and deployment

The pipeline is intended to be run periodically to keep the datasets up to
date.  A simple cron schedule can be configured via Docker Compose or an
orchestration tool like Airflow.  For example, the provided
`docker-compose.yml` (optional) mounts the project directory and runs `make
all` on a daily schedule.  CI/CD is handled by GitHub Actions via
`.github/workflows/ci.yml`, which lints, type‑checks, tests and produces
artifacts for sample runs.

## Extensibility

New sources can be integrated by adding a connector class under
`br/aqi/sources.py` and registering it in `get_sources()`.  The normalization
and validation logic are centralised and automatically apply to new
datasets provided they emit the expected columns.  Adjust the heuristics in
`rag.py` to influence how sources are ranked and selected.