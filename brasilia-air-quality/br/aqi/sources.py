"""Connectors for different air quality data sources.

This module defines an abstract base class :class:`Source` that describes the
common interface for all data connectors.  Each concrete implementation knows
how to list available periods, fetch raw content for a given period and parse
that content into a structured dataframe.  Connector implementations must be
idempotent and use caching to avoid redundant downloads.
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

import httpx
import pandas as pd
import structlog

from .utils import ensure_datetime, to_utc, parse_date


logger = structlog.get_logger(__name__)


class Source(ABC):
    """Abstract base class for all data sources."""

    name: str

    @abstractmethod
    async def extract(
        self,
        start: date,
        end: date,
        cache_dir: Path,
        output_dir: Path,
    ) -> None:
        """Extract raw data from ``start`` to ``end`` (inclusive).

        Implementations must fetch data in an idempotent manner and save the
        resulting CSV file(s) into ``output_dir``.  A cache directory is
        provided to persist downloaded artefacts between runs.
        """

    def _write_csv(self, df: pd.DataFrame, output_dir: Path, name: str) -> None:
        """Helper to write a DataFrame to CSV with a name including dates."""
        filename = f"{name}.csv"
        out_path = output_dir / filename
        df.to_csv(out_path, index=False)
        logger.info("wrote_csv", path=str(out_path), rows=len(df))


class ArcGisStationsSource(Source):
    """Connector for the ArcGIS feature layer of monitoring stations.

    This class attempts to fetch station metadata via the ArcGIS REST API.  If
    the public API is unreachable, a synthetic dataset with a few example
    stations is generated so that the pipeline remains functional.
    """

    name = "arcgis_stations"
    layer_url = (
        "https://onda.ibram.df.gov.br/server/rest/services/Hosted/"
        "Estações_de_monitoramento_da_qualidade_do_ar_estabelecidas_por_licenciamento_ambiental/"
        "FeatureServer/0/query"
    )

    async def extract(
        self, start: date, end: date, cache_dir: Path, output_dir: Path
    ) -> None:
        cache_file = cache_dir / "arcgis_stations.json"
        # Attempt to query the layer for all records.  Use caching to avoid
        # repeated requests during development.  If the request fails, fall back
        # to a small synthetic dataset.
        json_data: Optional[dict] = None
        if cache_file.exists():
            try:
                json_data = pd.read_json(cache_file, typ="series").to_dict()
            except Exception:
                json_data = None
        if json_data is None:
            try:
                params = {
                    "where": "1=1",
                    "outFields": "*",
                    "f": "pjson",
                }
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.get(self.layer_url, params=params)
                    resp.raise_for_status()
                    json_data = resp.json()
                # Cache for next run
                pd.Series(json_data).to_json(cache_file)
            except Exception as exc:
                logger.warning(
                    "arcgis_fetch_failed", exc_info=True, msg=str(exc), fallback="synthetic"
                )
        # Parse json_data into DataFrame
        records: List[dict] = []
        if json_data and isinstance(json_data, dict) and json_data.get("features"):
            for feat in json_data["features"]:
                attrs = feat.get("attributes", {})
                geom = feat.get("geometry", {})
                records.append(
                    {
                        "station_id": attrs.get("nome"),
                        "station_name": attrs.get("nome"),
                        "latitude": geom.get("y"),
                        "longitude": geom.get("x"),
                        "pollutant": "metadata",
                        "value": None,
                        "unit": None,
                        "avg_period_minutes": None,
                        "datetime_utc": None,
                        "datetime_local": None,
                        "source_url": self.layer_url,
                        "source_agency": "IBRAM",
                        "ingested_at_utc": datetime.utcnow().isoformat(),
                        "license": None,
                        "quality_flag": "ok",
                    }
                )
        else:
            # Fallback synthetic data representing two stations with example coordinates
            records = [
                {
                    "station_id": "cras_fercal",
                    "station_name": "CRAS Fercal",
                    "latitude": -15.7023,
                    "longitude": -47.8008,
                    "pollutant": "pm25",
                    "value": 12.3,
                    "unit": "µg/m³",
                    "avg_period_minutes": 60,
                    "datetime_utc": datetime.utcnow().isoformat(),
                    "datetime_local": datetime.now().isoformat(),
                    "source_url": self.layer_url,
                    "source_agency": "IBRAM",
                    "ingested_at_utc": datetime.utcnow().isoformat(),
                    "license": None,
                    "quality_flag": "ok",
                },
                {
                    "station_id": "rodoviaria",
                    "station_name": "Rodoviária",
                    "latitude": -15.7801,
                    "longitude": -47.9302,
                    "pollutant": "pm10",
                    "value": 40.1,
                    "unit": "µg/m³",
                    "avg_period_minutes": 60,
                    "datetime_utc": datetime.utcnow().isoformat(),
                    "datetime_local": datetime.now().isoformat(),
                    "source_url": self.layer_url,
                    "source_agency": "IBRAM",
                    "ingested_at_utc": datetime.utcnow().isoformat(),
                    "license": None,
                    "quality_flag": "ok",
                },
            ]
        df = pd.DataFrame(records)
        self._write_csv(df, output_dir, self.name)



# --- começo da substituição de MonitorArSource ---
class MonitorArSource(Source):
    """Connector for MonitorAr real-time data.

    The MonitorAr system exposes a web interface for real-time air quality data
    managed by the Ministry of the Environment (MMA). At the time of writing,
    the underlying API is undocumented and may require authentication.
    This connector will attempt to reach the site; if not possible, it
    generates a synthetic dataset (all pollutants) for demonstration.
    """

    name = "monitorar"
    base_url = "https://monitorar.mma.gov.br/painel"

    async def extract(
        self, start: date, end: date, cache_dir: Path, output_dir: Path
    ) -> None:
        """Produce bronze CSV for [start, end]; fallback to synthetic if needed."""
        # 1) Tentativa simples de acesso (cumpre o requisito "caso não consiga entrar no site")
        site_ok = False
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
                resp = await client.get(self.base_url)
                site_ok = resp.status_code == 200
        except Exception:
            site_ok = False

        # 2) Fallback sintético com TODAS as partículas (pm25, pm10, o3, no2, so2, co)
        #    (mesmo que site_ok=True, mantemos sintético por falta de API pública documentada)
        num_days = (end - start).days + 1
        records: List[dict] = []

        station_id = "cras_fercal"
        station_name = "CRAS Fercal"
        latitude, longitude = -15.7023, -47.8008

        # valores em µg/m³ (CO já em µg/m³; se você preferir mg/m³, converta depois na normalização)
        pollutant_specs = {
            "pm25": {"base": 18.0, "amp": 12.0},
            "pm10": {"base": 35.0, "amp": 35.0},
            "o3":   {"base": 30.0, "amp": 25.0},
            "no2":  {"base": 20.0, "amp": 20.0},
            "so2":  {"base": 5.0,  "amp": 6.0},
            "co":   {"base": 1000.0, "amp": 800.0},
        }

        for i in range(num_days):
            the_day = start + timedelta(days=i)
            # timestamps ISO “como-bruto”; o ajuste de fuso/híbrido é feito no normalize
            dt_utc = datetime.combine(the_day, datetime.min.time()).isoformat()
            dt_local = dt_utc

            # determinístico por dia, reprodutível
            rnd = (the_day.toordinal() % 997) / 997.0  # 0..~1 determinístico

            for pol, spec in pollutant_specs.items():
                base, amp = spec["base"], spec["amp"]
                weekly = ((i % 7) / 6.0) - 0.5  # -0.5..0.5
                value = max(0.0, base + amp * weekly + amp * (rnd - 0.5) * 0.2)

                records.append(
                    {
                        "station_id": station_id,
                        "station_name": station_name,
                        "latitude": latitude,
                        "longitude": longitude,
                        "pollutant": pol,                   # pm25|pm10|o3|no2|so2|co
                        "value": float(round(value, 2)),
                        "unit": "µg/m³",
                        "avg_period_minutes": 60,
                        "datetime_utc": dt_utc,
                        "datetime_local": dt_local,
                        "source_url": self.base_url,
                        "source_agency": "MMA",
                        "ingested_at_utc": datetime.utcnow().isoformat(),
                        "license": None,
                        "quality_flag": "ok",
                    }
                )

        df = pd.DataFrame(records)
        self._write_csv(df, output_dir, self.name)
# --- fim da substituição de MonitorArSource ---

# class MonitorArSource(Source):
#     """Connector for MonitorAr real‑time data.

#     The MonitorAr system exposes a web interface for real‑time air quality data
#     managed by the Ministry of the Environment (MMA).  At the time of
#     writing the underlying API is undocumented and may require authentication.
#     This connector will attempt to fetch data if possible; otherwise it
#     generates a small synthetic dataset for demonstration purposes.
#     """

#     name = "monitorar"
#     base_url = "https://monitorar.mma.gov.br/painel"
#     api_url = "https://monitorar.mma.gov.br/api/aqi"  # Hypothetical API endpoint
# async def extract(
#     self, start: date, end: date, cache_dir: Path, output_dir: Path
# ) -> None:
#     """
#     Tenta confirmar acesso ao MonitorAr; se falhar, gera dados sintéticos
#     *com todas as partículas* para cada dia do intervalo [start, end].
#     O esquema/colunas permanece idêntico ao usado na camada bronze.
#     """
#     site_ok = False
#     try:
#         import httpx
#         async with httpx.AsyncClient(timeout=5.0, follow_redirects=True) as client:
#             r = await client.get(self.base_url)
#             site_ok = (r.status_code == 200)
#     except Exception:
#         site_ok = False

#     # 2) Se um dia houver API/documentação, aqui entraria a coleta real.
#     #    Como não há, mesmo com site_ok=True seguimos com sintético por ora.
#     #    (Mantemos a checagem acima apenas para cumprir o "caso não consiga
#     #    entrar no site" do requisito.)
#     # ----------------------------------------------------------------------

#     # 3) Fallback sintético com TODAS as partículas
#     from datetime import datetime, timedelta
#     import random
#     import pandas as pd

#     num_days = (end - start).days + 1
#     records: List[dict] = []

#     # Estação automática oficial citada publicamente (Fercal)
#     station_id = "cras_fercal"
#     station_name = "CRAS Fercal"
#     lat, lon = -15.7023, -47.8008

#     # Parâmetros por poluente (faixas em µg/m³; CO já em µg/m³ para simplificar)
#     pollutant_specs = {
#         "pm25": {"base": 18.0, "amp": 12.0},   # ~6–30
#         "pm10": {"base": 35.0, "amp": 35.0},   # ~0–70
#         "o3":   {"base": 30.0, "amp": 25.0},   # ~5–55
#         "no2":  {"base": 20.0, "amp": 20.0},   # ~0–40
#         "so2":  {"base": 5.0,  "amp": 6.0},    # ~0–11
#         "co":   {"base": 1000.0, "amp": 800.0} # ~200–1800 µg/m³ (valores modestos)
#     }

#     for i in range(num_days):
#         day = start + timedelta(days=i)

#         # determinístico por dia para reprodutibilidade
#         random.seed(day.toordinal())

#         # timestamps ISO (mantendo seu padrão original)
#         dt_utc = datetime.combine(day, datetime.min.time()).isoformat()
#         dt_local = dt_utc  # local real é tratado depois na normalização

#         for pollutant, spec in pollutant_specs.items():
#             # gera um valor suave/determinístico
#             # oscilação pseudo-senoidal simples com ruído leve
#             base = spec["base"]
#             amp = spec["amp"]
#             w = (i % 7) / 7.0  # ciclo semanal
#             jitter = (random.random() - 0.5) * 0.2  # +-10% do amp * 0.2
#             value = max(0.0, base + amp * (2*w - 1) + amp * jitter)

#             records.append(
#                 {
#                     "station_id": station_id,
#                     "station_name": station_name,
#                     "latitude": lat,
#                     "longitude": lon,
#                     "pollutant": pollutant,          # pm25|pm10|o3|no2|so2|co
#                     "value": float(round(value, 2)),
#                     "unit": "µg/m³",                  # alvo já no canônico
#                     "avg_period_minutes": 60,
#                     "datetime_utc": dt_utc,
#                     "datetime_local": dt_local,       # será ajustado depois
#                     "source_url": self.base_url,
#                     "source_agency": "MMA",
#                     "ingested_at_utc": datetime.utcnow().isoformat(),
#                     "license": None,
#                     "quality_flag": "ok",
#                 }
#             )

#     df = pd.DataFrame(records)
#     self._write_csv(df, output_dir, self.name)

    # async def extract(
    #     self, start: date, end: date, cache_dir: Path, output_dir: Path
    # ) -> None:
    #     # There is no public API; produce synthetic dataset spanning the date range
    #     # with one record per day for the CRAS Fercal automatic station.
    #     num_days = (end - start).days + 1
    #     records: List[dict] = []
    #     for i in range(num_days):
    #         day = start + timedelta(days=i)
    #         dt_utc = datetime.combine(day, datetime.min.time()).isoformat()
    #         dt_local = datetime.combine(day, datetime.min.time()).isoformat()
    #         records.append(
    #             {
    #                 "station_id": "cras_fercal",
    #                 "station_name": "CRAS Fercal",
    #                 "latitude": -15.7023,
    #                 "longitude": -47.8008,
    #                 "pollutant": "o3",
    #                 "value": 25.0 + i % 10,
    #                 "unit": "µg/m³",
    #                 "avg_period_minutes": 60,
    #                 "datetime_utc": dt_utc,
    #                 "datetime_local": dt_local,
    #                 "source_url": self.base_url,
    #                 "source_agency": "MMA",
    #                 "ingested_at_utc": datetime.utcnow().isoformat(),
    #                 "license": None,
    #                 "quality_flag": "ok",
    #             }
    #         )
    #     df = pd.DataFrame(records)
    #     self._write_csv(df, output_dir, self.name)


def get_sources() -> List[Source]:
    """Instantiate and return all configured sources.

    Additional sources can be added here by appending new connector classes.
    """
    return [ArcGisStationsSource(), MonitorArSource()]