"""Top-level package for brasilia_air_quality.

This package provides tools to discover, extract, normalize, validate and export
air quality data for the Federal District of Brazil (Brasília/DF).  It is
designed to be reproducible and extensible so that new data sources can be
integrated as they become available.

The canonical public API is exposed via the command line interface defined in
``br/aqi/cli.py``.
"""

from importlib.metadata import version as _get_version


def __getattr__(name: str):  # pragma: no cover
    if name == "__version__":
        return _get_version("brasilia-air-quality")
    raise AttributeError(name)


__all__ = ["cli", "rag", "sources", "normalize", "validate", "export", "utils"]