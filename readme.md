# NZ Traffic Crash Data Pipeline

A containerised data pipeline that loads public New Zealand road-crash data, enriches it with regional climate data, models it with dbt, tests the business logic, and runs on an Airflow schedule. Everything runs locally with Docker Compose.

**Patterns demonstrated:** layered dbt modelling (raw → staging → marts), incremental API backfill with checkpoint/resume, rate-limit-aware extraction, custom data quality tests, task orchestration, and CI that needs no warehouse connection.

---

## The Problem

Road-safety analysts and local authorities want to understand where and when serious crashes happen, and whether regional conditions such as rainfall play a part. The raw data makes that hard:

- The NZTA export is one wide table of 70+ columns with mixed-case names, inconsistent nulls, and counts stored as floats.
- It contains no weather measurements. Weather appears only as a category in the police crash report (`weatherA` / `weatherB`).
- It is a downloadable snapshot. There is nothing to refresh it, test it, or join it to other sources.

This project turns that snapshot into a repeatable, tested pipeline with analytics-ready tables, and adds measured climate data so regional crash patterns can be compared with regional weather.

## What It Delivers

| Output | Grain | Purpose |
|---|---|---|
| `fct_nz_traffic_crash` | One row per crash | Crash facts with derived metrics: road type, total casualties, fatality and vulnerable-road-user flags |
| `dim_regional_climate` | One row per NZTA region per year | Average temperature, total rainfall, wet days, heaviest daily rainfall |

Questions the model supports:

- How do crash severity and fatality rates vary by road type, region, and year?
- How often do crashes involve vulnerable road users (bicycles, mopeds, motorcycles)?
- Do regions with more wet days also show more crashes, at region-year level?
- Do officer-reported weather conditions line up with measured regional rainfall?

---

## Architecture & Data Flow

```mermaid
graph TD
    A[Waka Kotahi NZTA Open Data API] -->|Python / Pandas| B[Raw crash DataFrame]
    W[Open-Meteo Archive API] -->|Python / Pandas + checkpoint| H[Weather DataFrame]
    B -->|SQLAlchemy / psycopg2| C[(PostgreSQL — raw schema)]
    H -->|SQLAlchemy / psycopg2| C
    C -->|dbt staging| D[View: stg_nz_traffic_crash]
    C -->|dbt staging| I[View: stg_nz_weather]
    D -->|dbt mart| E[Table: fct_nz_traffic_crash]
    I -->|dbt mart| J[Table: dim_regional_climate]
    E -->|dbt tests| F[Data quality validation]
    E -->|join on region + year| L[Analysis / BI layer]
    J -->|join on region + year| L
    G[Apache Airflow] -.->|orchestrates| B
    G -.->|orchestrates| D
    G -.->|orchestrates| F
    K[GitHub Actions CI] -.->|lint + dbt parse + gitleaks| D

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
| **Transform** | dbt Core | Staging views (clean types, snake_case), a crash fact table, and a regional climate dimension |
| **Test** | dbt singular tests | Validates business logic across the mart layer |
| **Orchestrate** | Apache Airflow | Runs the crash pipeline daily with task-level dependencies |
| **CI** | GitHub Actions, Ruff, Gitleaks | Lint, `dbt parse`, and secret scan on every push and pull request to `main`; no warehouse connection needed |

---

## Design Decisions: Why We Do It This Way

| Decision | Why | Trade-off |
|---|---|---|
| **Layered models: raw → staging → marts** | Raw stays an untouched copy of the source. Staging standardises names and types in one place. Marts hold business logic, so changing a rule never means re-downloading data. | More models to maintain than a single query |
| **Staging as views, marts as tables** | Views cost no storage and always reflect raw. Marts are materialised so reporting queries are fast. | Marts must be rebuilt to pick up new data |
| **Raw keeps the source's mixed-case columns; staging renames to snake_case** | Raw mirrors the source exactly. Renaming happens in one layer, and Postgres needs the original names double-quoted. | Quoting is required whenever raw is queried directly |
| **Safe-casting with regex guards and defaults** | The source has inconsistent nulls and non-numeric values. A bad value should not fail the whole run, and derived flags should not evaluate to NULL. | Defaults can hide missing data, so the mart tests check the derived logic |
| **Weather joined at region + year, not per crash** | The open crash data carries only `crashYear` and `region`, with no date or time. A per-crash weather join is impossible, and region + year is the finest grain the data supports honestly. | Results are regional indicators, not conditions at the moment of a crash |
| **One representative city per region** | Open-Meteo is point-based. One location per NZTA region keeps API call volume small. | One city cannot fully represent a large or varied region |
| **Incremental weather load with checkpoint/resume and backoff** | Open-Meteo enforces rate limits. A throttled or failed run resumes where it stopped instead of restarting. | Adds state (`weather_progress.json`) to manage |
| **Region `NULL` becomes `'Unknown'` in the mart** | Some crash rows have coordinates but no region, and there is no lookup source. Keeping them preserves crash totals instead of silently dropping rows. | These rows cannot join to the climate dimension |
| **CI runs lint, `dbt parse`, and a secret scan only** | Fast, needs no credentials, and blocks committed secrets. | `dbt parse` proves the SQL compiles, not that it returns correct data; `dbt test` covers that in the Docker/Airflow run |
| **Custom singular tests on derived columns** | Derived flags and categories are where logic errors creep in. | Tests cover the crash mart only (see Known Limitations) |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10 |
| Transformation | dbt Core / dbt-postgres (versions pinned in `requirements.txt`) |
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
│   │   ├── sources.yml            # Sources: raw.NZ_Traffic_Crash + raw.NZ_Weather
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
├── .env.example                   # Template for local environment variables
└── .env                           # Not committed — see Environment Variables below
```

> **Note:** `requirements-ci.txt` intentionally excludes `apache-airflow`. Airflow is only needed inside Docker or full local runs and makes CI slow.

---

## dbt Data Modelling

### Staging: `stg_nz_traffic_crash` (View)

Cleans and standardises the raw crash table:

- Renames all columns from camelCase / UPPERCASE to `snake_case`
- Wraps mixed-case source columns in double quotes to handle Postgres case sensitivity
- Safe-casts numeric columns using regex guards, defaulting to `0` or `NULL` rather than erroring
- Handles coordinates (`X`, `Y`) as decimals, with `NULL` for invalid values
- Adds `ingested_at` via `{{ current_timestamp() }}`

### Staging: `stg_nz_weather` (View)

Cleans the raw Open-Meteo table (`raw.NZ_Weather`, one row per city per day):

- Maps the representative city label (for example `Auckland`) to the exact NZTA region spelling (`Auckland Region`) so it joins to the crash mart
- Attaches representative `source_lat` / `source_lon` per region for traceability
- Parses the text `"Date"` (`YYYY-MM-DD`) into `weather_date` and `weather_year`, and drops unmapped or undated rows

### Mart: `fct_nz_traffic_crash` (Table)

Builds the analytical fact table with derived business metrics:

| Column | Description |
|---|---|
| `road_type` | Categorises speed limit into `Urban` (≤50), `Rural` (≤80), `Highway` (>80), or `Unknown` |
| `total_casualties` | Sum of fatal, serious, and minor injury counts |
| `has_fatality` | `true` when `fatal_count > 0` |
| `involves_vulnerable_road_user` | `true` when any bicycle, moped, or motorcycle count is above 0 |
| `is_serious_or_fatal` | `true` when serious or fatal injuries occurred |
| `holiday_name` | Defaults to `'No Holiday'` when null |
| `region` | Source `NULL` regions are set to `'Unknown'` (see below) |

**Missing regions.** A number of crash rows (around 3,000 at the time of writing) have coordinates but no `region` in the source. There is no lookup source for them, so the mart keeps the rows and labels the region `'Unknown'`. Crash totals therefore stay complete, but these rows cannot be matched to `dim_regional_climate`.

### Mart: `dim_regional_climate` (Table)

Yearly regional climate dimension built from daily weather:

- **Grain is one row per NZTA region per year.** This is deliberate: the open crash data carries only `crashYear` and `region` (no date or time), so a per-crash weather join is impossible. Join on `region` + `crash_year` (= `year`), or on `region_year_key`.
- Columns: `avg_temp_c`, `total_rainfall_mm`, `wet_days` (days with `rainfall_mm > 1.0`), `max_daily_rainfall_mm`, `days_observed`, `source_lat/lon`, `fetched_at`.
- Only `rain_sum` and `temperature_2m_mean` are extracted, so there is no wind column.

---

## Weather Enrichment & Backfill

`python -m load.main` loads crashes, then backfills weather per region from `load/Static/MainCities.csv` (one lat/lon per NZTA region):

- **Incremental by default:** reads `MAX("Date")` per region from `raw.NZ_Weather` and fetches only missing dates. `--full-reload` truncates and refetches everything.
- **Checkpoint/resume:** progress is tracked in `Data/weather_progress.json` (`{region: end_date}`), and re-runs skip completed regions. `--reset-progress` ignores and overwrites it; `--progress-file` overrides the path.
- **Rate-limit safe:** shared session with exponential backoff (`fetch_with_backoff`). `DailyRateLimitExceeded` stops gracefully with progress saved so the next run resumes. `--request-delay` (default `6.0` seconds) throttles Open-Meteo calls.

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

The `NZTrafficCrash_pipeline` DAG runs daily (`@daily`, no catchup) with three sequential tasks. It covers the crash pipeline only; the weather backfill runs on demand via `python -m load.main`.

```
extract_and_load  →  dbt_run  →  dbt_test
```

- **`extract_and_load`**: PythonOperator that calls the extract and load functions
- **`dbt_run`**: BashOperator running `dbt run` inside the Airflow container
- **`dbt_test`**: BashOperator running `dbt test` after a successful build

---

## CI (GitHub Actions)

Workflow `.github/workflows/ci.yml` (`crash-pipeline-ci`) runs on `push` to `main` and on `pull_request`. It has three parallel jobs and needs no live warehouse connection:

| Job (check name) | What it does |
|---|---|
| `lint (ruff)` | `pip install ruff`, `ruff check extract load dags`, `ruff format --check extract load dags` (Python 3.11) |
| `dbt parse (no warehouse)` | `pip install -r requirements-ci.txt`, `dbt deps`, `dbt parse --profiles-dir .` with dummy `POSTGRES_PASSWORD` / `DBT_HOST=localhost`; validates SQL compilation only |
| `secret scan (gitleaks)` | `gitleaks/gitleaks-action@v2` over full history; blocks committed secrets |

Branch protection on `main` (GitHub → Settings → Rules → branch ruleset, enforcement `Active`) requires all three checks and blocks force pushes before merge. Required check names are the `name:` values above, not the file path.

Run the same checks locally:

```bash
pip install -r requirements-ci.txt
ruff check extract load dags
ruff format --check extract load dags
cd dbt/NZCrashData && dbt parse --profiles-dir .
```

---

## Known Limitations

- **No per-crash weather.** The open crash data has no date or time, so weather can only be compared at region + year level. The results show regional patterns, not the conditions of an individual crash.
- **One city per region.** A single representative location stands in for each NZTA region, which is least accurate for large or geographically varied regions.
- **Unknown regions.** Crashes with no source region are kept as `'Unknown'` and are excluded from climate comparisons.
- **Weather variables.** Only rainfall and mean temperature are extracted.
- **Refresh frequency.** The NZTA crash dataset is published monthly, so the daily schedule mostly demonstrates orchestration rather than a freshness requirement.
- **Test coverage.** The five singular tests cover the crash mart. `dim_regional_climate` does not have tests yet (for example, one row per region-year, and `wet_days` never above `days_observed`).
- **Weather is not orchestrated.** The backfill runs outside the Airflow DAG.

---

## Environment Variables

Create a `.env` file in the project root (not committed, and covered by Gitleaks). Use `.env.example` as the template:

```env
POSTGRES_PASSWORD=your_password
POSTGRES_DATABASE_NAME=NZTrafficCrashData
databaseURL=postgresql://postgres:${POSTGRES_PASSWORD}@localhost:5432/${POSTGRES_DATABASE_NAME}
DBT_HOST=localhost

AIR_FLOW_USER=admin
AIR_FLOW_PASSWORD=change_me
AIR_FLOW_FIRSTNAME=First
AIR_FLOW_LASTNAME=Last
AIR_FLOW_EMAIL=admin@example.com
```

> **Note:** PostgreSQL 18 requires the volume path to include a version suffix: `/var/lib/postgresql/data/18`. This is already configured in `docker-compose.yml`. `dbt/NZCrashData/profiles.yml` reads `POSTGRES_DATABASE_NAME`, `POSTGRES_PASSWORD`, and `DBT_HOST` from the environment (defaults suit CI).

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

This starts PostgreSQL and the full Airflow stack (init, webserver, scheduler). Airflow initialises its metadata database before the webserver and scheduler start.

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

Open `http://localhost:8080`, log in with your configured credentials, enable the `NZTrafficCrash_pipeline` DAG, and trigger a run.

---

## Data Sources

**Waka Kotahi NZ Transport Agency: Crash Analysis System (CAS)**

- Portal: [NZTA Open Data Portal](https://opendata-nzta.opendata.arcgis.com/)
- Licence: Creative Commons Attribution 4.0 International
- Coverage: Road crashes reported to NZ Police, from 2000 onwards. The open dataset is a confidentialised version of the internal system and is updated monthly.

**Open-Meteo Archive API: daily weather**

- Endpoint: `archive-api.open-meteo.com/v1/archive?daily=rain_sum,temperature_2m_mean`
- Coverage: One representative city per NZTA region (see `load/Static/MainCities.csv`), from the earliest crash year to today
- Attribution: Weather data by [Open-Meteo.com](https://open-meteo.com/), licensed CC BY 4.0. The free API is for non-commercial use.