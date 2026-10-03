# NZ Traffic Crash Data Pipeline

An end-to-end data engineering pipeline that ingests, loads, transforms, and tests public traffic crash data from the **Waka Kotahi NZ Transport Agency Open Data Portal**, enriched with daily climate data from the **Open-Meteo Archive API**.

Built to demonstrate production-grade data engineering patterns: containerised infrastructure, programmatic data ingestion, schema-aware database design, layered dbt transformation modelling, custom data quality testing, incremental backfill with checkpoint/resume, Airflow orchestration, and GitHub Actions CI — all running locally via Docker Compose.

---

## Architecture & Data Flow

```mermaid
graph TD
    A[Waka Kotahi NZTA Open Data API] -->|Python / Pandas| B[Raw DataFrame]
    W[Open-Meteo Archive API] -->|Python / Pandas + checkpoint| H[Weather DataFrame]
    B -->|SQLAlchemy / psycopg2| C[(PostgreSQL — raw schema)]
    H -->|SQLAlchemy / psycopg2| C
    C -->|dbt Staging| D[View: stg_nz_traffic_crash]
    C -->|dbt Staging| I[View: stg_nz_weather]
    D -->|dbt Mart| E[Table: fct_nz_traffic_crash]
    I -->|dbt Mart| J[Table: dim_regional_climate]
    E -->|dbt Tests| F[Data Quality Validation]
    G[Apache Airflow] -->|Orchestrates| B
    G -->|Orchestrates| D
    G -->|Orchestrates| F
    K[GitHub Actions CI] -->|lint + dbt parse + gitleaks| B
    K -->|Guards| D

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style W fill:#9df,stroke:#333,stroke-width:2px
    style C fill:#96f,stroke:#333,stroke-width:2px
    style E fill:#6f9,stroke:#333,stroke-width:2px
    style J fill:#6f9,stroke:#333,stroke-width:2px
    style G fill:#f96,stroke:#333,stroke-width:2px
    style K fill:#ff6,stroke:#333,stroke-width:2px
```

| Stage | Tool | Description |
|---|---|---|
| **Extract** | Python, Pandas | Pulls crash data from the NZTA ArcGIS CSV endpoint and adds an ingestion timestamp; pulls daily weather (`rain_sum`, `temperature_2m_mean`) per region from Open-Meteo with exponential-backoff retries |
| **Load** | SQLAlchemy, psycopg2 | Loads DataFrames into PostgreSQL preserving original column casing; chunked (`chunksize=5000`, `method='multi'`); weather load is incremental with checkpoint/resume |
| **Transform** | dbt Core | Staging views (clean types, snake_case) + mart fact table + regional climate dimension (see below) |
| **Test** | dbt singular tests | Validates business logic across the mart layer |
| **Orchestrate** | Apache Airflow | Schedules the crash pipeline daily with task-level dependency management |
| **CI** | GitHub Actions, Ruff, Gitleaks | Lint + `dbt parse` + secret scan on every push/PR to `main` — no warehouse connection needed |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10 |
| Transformation | dbt Core 1.12+ / dbt-postgres 1.11 |
| Database | PostgreSQL 18 |
| Ingestion | Python 3.10+, Pandas, SQLAlchemy, Open-Meteo Archive API |
| Containerisation | Docker, Docker Compose |
| CI / Quality | GitHub Actions, Ruff (lint + format), Gitleaks (secret scan) |

---

## Project Structure

```
├── .github/
│   └── workflows/
│       └── ci.yml                 # CI: lint (ruff) → dbt parse → secret scan (gitleaks)
├── dags/
│   └── NZCrash_pipeline_dag.py    # Airflow DAG — extract/load → dbt run → dbt test (crash only)
├── extract/
│   ├── NZTrafficCrash_extract.py  # ExtractData (NZTA CSV) + ExtractWeatherData (Open-Meteo) + backoff
│   └── main.py                    # Extraction smoke-test entrypoint
├── load/
│   ├── postgres_load.py           # load_to_postgres (chunked) + delete_table_if_exists
│   ├── main.py                    # Crash + weather backfill entrypoint (checkpoint/resume, --full-reload)
│   └── Static/
│       └── MainCities.csv         # One representative lat/lon per NZTA region (16 regions)
├── dbt/NZCrashData/
│   ├── models/
│   │   ├── sources.yml            # Source definitions for raw.NZ_Traffic_Crash + raw.NZ_Weather
│   │   ├── staging/
│   │   │   ├── stg_nz_traffic_crash.sql
│   │   │   └── stg_nz_weather.sql       # City → NZTA region mapping, date/coord cleanup
│   │   └── marts/
│   │       ├── fct_nz_traffic_crash.sql # Fact table with derived metrics
│   │       └── dim_regional_climate.sql # Yearly region climate (avg temp, rainfall, wet days)
│   ├── tests/                     # Custom singular dbt data quality tests
│   ├── dbt_project.yml
│   └── profiles.yml               # Connection config (reads from environment variables)
├── docker-compose.yml             # PostgreSQL + Airflow (init, webserver, scheduler)
├── requirements.txt               # Full local deps (includes apache-airflow)
├── requirements-ci.txt            # Lean CI deps (no airflow) + ruff
├── pyproject.toml                 # Ruff config (target py310, rules E/F/I)
└── .env                           # Not committed — see Environment Variables below
```

> **Note:** `requirements-ci.txt` intentionally excludes `apache-airflow` — Airflow is only needed inside Docker / full local runs and makes CI slow. CI installs the lean file instead.

---

## dbt Data Modelling

### Staging Layer — `stg_nz_traffic_crash` (View)

Cleans and standardises the raw crash table:

- Renames all columns from camelCase / UPPERCASE to `snake_case`
- Wraps mixed-case source columns in double quotes to handle Postgres case sensitivity
- Safe-casts all numeric columns using regex guards, defaulting to `0` or `NULL` rather than erroring
- Handles coordinates (`X`, `Y`) as decimals with NULL for invalid values
- Adds `ingested_at` via `{{ current_timestamp() }}`

### Staging Layer — `stg_nz_weather` (View)

Cleans the raw Open-Meteo table (`raw.NZ_Weather`, grain = one row per city per day):

- Maps representative city label (`"Region"` e.g. `Auckland`) to exact NZTA region spelling (`Auckland Region`) so it joins to the crash mart
- Attaches representative `source_lat` / `source_lon` per region for traceability
- Parses text `"Date"` (`YYYY-MM-DD`) to `weather_date` + `weather_year`, drops unmapped/undated rows

### Mart Layer — `fct_nz_traffic_crash` (Table)

Builds the analytical fact table with derived business metrics:

| Column | Description |
|---|---|
| `road_type` | Categorises speed limit into `Urban` (≤50), `Rural` (≤80), `Highway` (>80), or `Unknown` |
| `total_casualties` | Sum of fatal, serious, and minor injury counts |
| `has_fatality` | Boolean — true when `fatal_count > 0` |
| `involves_vulnerable_road_user` | Boolean — true when any bicycle, moped, or motorcycle count > 0 |
| `is_serious_or_fatal` | Boolean — true when serious or fatal injuries occurred |
| `holiday_name` | Null-safe — defaults to `'No Holiday'` when null |

### Mart Layer — `dim_regional_climate` (Table)

Yearly regional climate dimension from daily weather:

- Grain is **one row per NZTA region per year** — deliberate: the crash open data carries only `crashYear` + `region` (no date), so a per-crash weather join is impossible. Join on `region + crash_year (= year)` or `region_year_key`.
- Columns: `avg_temp_c`, `total_rainfall_mm`, `wet_days` (days with `rainfall_mm > 1.0`), `max_daily_rainfall_mm`, `days_observed`, `source_lat/lon`, `fetched_at`.
- Only `rain_sum` / `temperature_2m_mean` are extracted, so there is intentionally no wind column.

---

## Weather Enrichment & Backfill

`python -m load.main` loads crashes, then backfills weather per region from `load/Static/MainCities.csv` (one lat/lon per NZTA region):

- **Incremental by default:** reads `MAX("Date")` per region from `raw.NZ_Weather` and fetches only missing dates; `--full-reload` truncates and refetches everything.
- **Checkpoint/resume:** progress tracked in `Data/weather_progress.json` (`{region: end_date}`); re-runs skip completed regions. `--reset-progress` ignores/overwrites it; `--progress-file` overrides the path.
- **Rate-limit safe:** shared session + exponential backoff (`fetch_with_backoff`); `DailyRateLimitExceeded` stops gracefully with progress saved so the next run resumes; `--request-delay` (default `6.0`s) throttles Open-Meteo calls.

```bash
python -m load.main                                # incremental crash + weather
python -m load.main --full-reload                 # truncate raw.NZ_Weather and refetch all
python -m load.main --reset-progress              # ignore checkpoint file
python -m load.main --request-delay 10 --progress-file ./my_progress.json
```

---

## Data Quality Tests

Five custom singular tests validate business logic against the mart:

| Test | What it checks |
|---|---|
| `assert_fct_nz_traffic_crash_fatalityCheck` | `has_fatality` is `true` whenever `fatal_count > 0` |
| `assert_fct_nz_traffic_crash_checkSeriousOrFatel` | `is_serious_or_fatal` is not `true` when both `serious_injury_count` and `fatal_count` are 0 |
| `assert_fct_nz_traffic_crash_validTotalcasualties` | `total_casualties` is never negative |
| `assert_fct_nz_traffic_crash_roadType` | `road_type` is always one of the four expected values |
| `assert_fct_nz_traffic_crash_yearNotInFuture` | `crash_year` does not exceed the current year |

---

## Airflow DAG

The `NZTrafficCrash_pipeline` DAG runs daily (`@daily`, no catchup) with three sequential tasks (crash pipeline only — weather backfill runs via `python -m load.main` above):

```
extract_and_load  →  dbt_run  →  dbt_test
```

- **`extract_and_load`** — PythonOperator that calls the extract and load functions
- **`dbt_run`** — BashOperator running `dbt run` inside the Airflow container
- **`dbt_test`** — BashOperator running `dbt test` after a successful build

---

## CI (GitHub Actions)

Workflow `.github/workflows/ci.yml` (`crash-pipeline-ci`) runs on `push` to `main` and on `pull_request` — three parallel jobs, no live warehouse connection:

| Job (check name) | What it does |
|---|---|
| `lint (ruff)` | `pip install ruff`, `ruff check extract load dags`, `ruff format --check extract load dags` (Python 3.11) |
| `dbt parse (no warehouse)` | `pip install -r requirements-ci.txt`, `dbt deps`, `dbt parse --profiles-dir .` with dummy `POSTGRES_PASSWORD` / `DBT_HOST=localhost` — validates SQL compilation only |
| `secret scan (gitleaks)` | `gitleaks/gitleaks-action@v2` over full history — blocks committed secrets |

Branch protection (GitHub → Settings → Rules → branch ruleset on `main`, enforcement `Active`) requires all three checks plus `Block force pushes` before merge. Required check names are the `name:` values above — not the file path.

Run the same checks locally:

```bash
pip install -r requirements-ci.txt
ruff check extract load dags
ruff format --check extract load dags
cd dbt/NZCrashData && dbt parse --profiles-dir .
```

Ruff rules live in `pyproject.toml` (`target py310`, `E/F/I`, `E501` ignored).

---

## Environment Variables

Create a `.env` file in the project root (not committed, covered by Gitleaks):

```env
POSTGRES_PASSWORD=your_password
POSTGRES_DATABASE_NAME=NZTrafficCrashData
databaseURL=postgresql://postgres:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DATABASE_NAME}
DBT_HOST=localhost

AIR_FLOW_USER=admin
AIR_FLOW_PASSWORD=admin
AIR_FLOW_FIRSTNAME=First
AIR_FLOW_LASTNAME=Last
AIR_FLOW_EMAIL=admin@example.com
```

> **Note:** PostgreSQL 18 requires the volume path to include a version suffix: `/var/lib/postgresql/data/18`. This is already configured in `docker-compose.yml`. `dbt/NZCrashData/profiles.yml` reads `POSTGRES_DATABASE_NAME` / `POSTGRES_PASSWORD` / `DBT_HOST` from the environment (defaults suit CI).

---

## How to Run

### Prerequisites

- Docker and Docker Compose
- Python 3.10+

### 1. Configure environment

```bash
cp .env.example .env  # then fill in your values
```

### 2. Start infrastructure

```bash
docker compose up -d
```

This starts PostgreSQL and the full Airflow stack (init, webserver, scheduler). Airflow initialises the metadata database before the webserver and scheduler start.

### 3. Run the pipeline manually (without Airflow)

```bash
python -m venv venv
source venv/bin/activate       # macOS/Linux
# venv\Scripts\activate        # Windows

pip install -r requirements.txt
python -m load.main            # crash + incremental weather backfill
```

### 4. Run dbt transformations and tests

```bash
cd dbt/NZCrashData
dbt build --profiles-dir .
```

### 5. Trigger via Airflow

Navigate to `http://localhost:8080`, log in with your configured credentials, enable the `NZTrafficCrash_pipeline` DAG, and trigger a run.

---

## Data Sources

**Waka Kotahi NZ Transport Agency — Crash Analysis System (CAS)**

- Portal: [NZTA Open Data Portal](https://opendata-nzta.opendata.arcgis.com/)
- Licence: Creative Commons Attribution 4.0 International
- Coverage: All recorded road crashes in New Zealand

**Open-Meteo Archive API — Daily Weather**

- Endpoint: `archive-api.open-meteo.com/v1/archive?daily=rain_sum,temperature_2m_mean`
- Coverage: One representative city per NZTA region (see `load/Static/MainCities.csv`), from earliest crash year to today
