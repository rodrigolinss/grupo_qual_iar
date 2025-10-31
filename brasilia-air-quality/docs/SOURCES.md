# Data sources and extraction plans

This document enumerates the official sources currently used by the
**Brasília Air Quality** project.  Each entry lists the responsible agency,
access method, temporal and spatial coverage, format and any known
limitations or terms of use.

| ID | Agency | URL | Format | Coverage | Notes |
|---|---|---|---|---|---|
| `arcgis_stations` | **IBRAM** (Instituto Brasília Ambiental) | `https://onda.ibram.df.gov.br/server/rest/services/Hosted/Estações_de_monitoramento_da_qualidade_do_ar_estabelecidas_por_licenciamento_ambiental/FeatureServer/0` | ArcGIS FeatureLayer | 9 stations across the Federal District | The feature layer contains coded names for each monitoring station (e.g. `cras_fercal`, `rodoviaria`, `zoo`) and fields indicating whether the station measures PM10, PM2.5 or gaseous pollutants【276791825465717†L86-L121】.  Export formats include CSV and GeoJSON【276791825465717†L24-L25】.  Direct queries via the REST API may require authentication. |
| `monitorar` | **Ministério do Meio Ambiente e Mudança do Clima (MMA)** | `https://monitorar.mma.gov.br` | Web application / API | Real‑time data from automatic stations in Fercal | According to Brasília Ambiental, data from certified automatic stations are published in real time via the MonitorAr site【488355614682576†L132-L137】.  The CRAS Fercal station monitors PM2.5, PM10, SO₂, CO, O₃ and NO₂ and is complete and certified, with results available in real time【572340908419758†L227-L232】.  The API is not officially documented and access may be limited; see the site’s terms of use. |
| `manual_reports` | **IBRAM / licensed companies** | Various annual PDF reports | PDF reports | Manual stations across Brasília and Fercal | Manual monitoring stations (Rodoviária, Zoológico, IFB Samambaia, IFB Estrutural, Fercal Oeste, Fercal Boa Vista and Contagem) collect 24‑hour PM10 and PM2.5 samples on a periodic basis and provide monthly/annual summaries【572340908419758†L127-L142】【572340908419758†L185-L199】.  These reports are published on the Brasília Ambiental website.  No API is available; scraping or manual extraction of tables may be required. |

## Terms of use and ethics

* **Robots and rate limiting:** All connectors respect the `robots.txt` files of
  their respective domains and implement courtesy delays and retries.  Users
  should avoid excessive requests; the ArcGIS service has a default maximum
  record count per query of 2000【276791825465717†L24-L25】.
* **Licensing:** The IBRAM feature layer and MonitorAr site are published by
  government agencies.  Where explicit licences are provided, they are
  preserved in the `license` column of the silver datasets.  When licences
  are absent the default assumption is public domain under Brazil’s open data
  policies, but users should confirm with the respective agency before
  redistribution.
* **Exclusions:** The project intentionally avoids scraping sources that
  prohibit automated access.  If a source cannot be accessed programmatically
  due to terms of use or technical barriers, the limitation is noted and a
  fallback strategy suggested.