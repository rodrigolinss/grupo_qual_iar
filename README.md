# Brasília Air Quality Project

This repository provides a reproducible pipeline for discovering, downloading,
processing and validating air quality data for the **Federal District of
Brazil** (Brasília/DF).  The goal is to centralise disparate official sources
into a uniform set of historical and near‑real‑time datasets that can be
analysed locally or integrated into other systems.

## Motivation

Air pollution can have significant health impacts, especially in densely
populated urban areas.  Monitoring pollutant concentrations over time
empowers citizens and decision makers to take preventative actions and to
design effective public policies.  The **Programa de Monitoramento da
Qualidade do Ar do Distrito Federal** has been operating since 2005 to
evaluate concentrations of priority pollutants and inform environmental
licensing【488355614682576†L152-L156】.  Automatic stations located in the
industrial suburb of Fercal report data in real time through the
**MonitorAr** system managed by Brazil’s Ministry of the Environment
【488355614682576†L132-L137】.  The pipeline implemented here automates the
collection and transformation of those official datasets.

## Features

* **Source discovery** – A lightweight Retrieval Augmented Generation (RAG)
  module searches data catalogues and public portals for relevant datasets.  In
  this initial version the sources are seeded with official resources
  discovered during exploratory research.
* **Connectors for official sources** – The pipeline includes a connector for
  the IBRAM ArcGIS feature layer listing the monitoring stations and a
  placeholder connector for the MonitorAr real‑time service.  Additional
  connectors can be added by subclassing the `Source` base class.
* **Normalization** – Raw data are transformed into a canonical schema with
  unified pollutant codes, units (µg/m³) and timezone handling.  CO
  concentrations reported in mg/m³ are converted to µg/m³.
* **Validation** – The processed data are checked for plausibility
  (reasonable value ranges, monotonic timestamps, valid coordinates and
  complete columns).  The CI pipeline fails if any issues are detected.
* **Export** – Normalized data can be exported to CSV files partitioned by
  year and month.  Parquet export is optional when `pyarrow` is available.
* **Reproducibility** – A Makefile and Dockerfile enable running the
  pipeline locally or inside a container with pinned dependencies.

## Data sources

Two official sources underpin the current pipeline:

1. **ArcGIS feature layer (IBRAM)** – The Brasília Environmental Institute
   (IBRAM) publishes an ArcGIS feature layer titled *Estações de
   monitoramento da qualidade do ar estabelecidas por licenciamento
   ambiental*.  The layer defines coded station names (e.g. `cras_fercal`,
   `rodoviaria`, `zoo`) and attributes such as whether each station measures
   PM10, PM2.5 or gases【276791825465717†L86-L121】.  Export formats include CSV
   and GeoJSON【276791825465717†L24-L25】 but the service may require
   authentication for direct queries.  It provides metadata about nine
   monitoring points.
2. **MonitorAr real‑time data (MMA)** – The *MonitorAr* platform displays
   real‑time air quality data from certified automatic stations.  According
   to IBRAM, all certified automatic stations are located in Fercal and their
   data are “divulgados em tempo real” via MonitorAr【488355614682576†L132-L137】.
   Manual stations located throughout the Federal District monitor PM10 and
   PM2.5 on a periodic basis; their results feed into monthly and annual
   reports【572340908419758†L127-L142】.  The CRAS Fercal station is complete and
   certified and monitors PM2.5, PM10, SO₂, CO, O₃ and NO₂, with results
   published in real time【572340908419758†L227-L232】.

Details of all sources, including access URLs and terms of use, are compiled
in [SOURCES.md](SOURCES.md).

## Repository structure

The repository follows a clear hierarchy:

```text
brasilia-air-quality/
├─ br/aqi/               # Python package implementing the pipeline
│  ├─ cli.py             # Typer CLI with commands discover/extract/normalize/validate/export
│  ├─ rag.py             # RAG logic for discovering and ranking sources
│  ├─ sources.py         # Connectors for each data source
│  ├─ normalize.py       # Normalization routines for canonical schema
│  ├─ validate.py        # Data validation checks
│  ├─ export.py          # Export utilities
│  └─ utils.py           # Miscellaneous helpers
├─ data/bronze/          # Raw downloaded data (one CSV per source)
├─ data/silver/          # Normalized datasets (canonical schema)
├─ artifacts/cache/      # Cached downloads and intermediate files
├─ artifacts/sources_index.json  # Ranked list of candidate sources
├─ docs/                 # Project documentation (README, architecture, sources)
├─ tests/                # Pytest suite with fixtures
├─ Dockerfile            # Container definition for reproducible execution
├─ Makefile              # Convenience targets for running the pipeline
├─ requirements.in / requirements.txt  # Dependency specifications
├─ pyproject.toml        # Project metadata and tool configuration
└─ LICENSE               # MIT license
```

## Getting started

See [QUICKSTART.md](QUICKSTART.md) for instructions on setting up your
environment, running the pipeline and contributing to the project.

python -m br.aqi.cli extract --since 2020-01-01 --until today
python -m br.aqi.cli normalize
python -m br.aqi.cli validate
python -m br.aqi.cli export
