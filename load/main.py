import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from extract.NZTrafficCrash_extract import DailyRateLimitExceeded, ExtractData, ExtractWeatherData
from load.postgres_load import delete_table_if_exists, load_to_postgres

load_dotenv(".env")
dburl = os.getenv("databaseURL")
# print(f"Database URL: {dburl}")

DEFAULT_PROGRESS_FILE = Path(__file__).resolve().parents[1] / "Data" / "weather_progress.json"


def load_progress(progress_file: Path) -> dict:
    try:
        if progress_file.exists():
            with open(progress_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"Could not read progress file {progress_file}: {e}")
    return {}


def save_progress(progress_file: Path, progress: dict) -> None:
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = progress_file.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(progress, f, indent=2)
    tmp.replace(progress_file)


def get_weather_max_date_per_region(db_url: str) -> dict:
    """Return {Region: max Date string} from raw.NZ_Weather, or {} if table/DB unavailable."""
    try:
        import sqlalchemy as sa

        engine = sa.create_engine(db_url)
        with engine.connect() as conn:
            insp = sa.inspect(conn)
            if not insp.has_table("NZ_Weather", schema="raw"):
                return {}
            rows = conn.execute(
                sa.text('SELECT "Region", MAX("Date") FROM raw."NZ_Weather" GROUP BY "Region"')
            ).fetchall()
            out = {}
            for region, max_date in rows:
                if region is not None and max_date is not None:
                    out[str(region)] = str(max_date)[:10]
            return out
    except Exception as e:
        print(f"Could not read existing weather max dates (continuing without DB incremental check): {e}")
        return {}


def parse_args():
    p = argparse.ArgumentParser(description="NZ crash + weather backfill with checkpoint/resume.")
    p.add_argument(
        "--full-reload",
        action="store_true",
        help="Truncate raw.NZ_Weather and refetch everything (default is incremental/resume).",
    )
    p.add_argument("--reset-progress", action="store_true", help="Ignore and overwrite the checkpoint file.")
    p.add_argument(
        "--request-delay",
        type=float,
        default=6.0,
        help="Seconds to sleep between Open-Meteo archive calls (default 6).",
    )
    p.add_argument(
        "--progress-file",
        type=str,
        default=str(DEFAULT_PROGRESS_FILE),
        help="Checkpoint file tracking completed regions.",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    progress_file = Path(args.progress_file)

    if args.reset_progress and progress_file.exists():
        progress_file.unlink()
        print(f"Removed progress file {progress_file}")

    progress = {} if args.reset_progress else load_progress(progress_file)
    print(f"Progress file: {progress_file} ({len(progress)} regions tracked)")

    # Example usage
    df = ExtractData()

    if df is not None:
        delete_table_if_exists("NZ_Traffic_Crash", "raw", dburl)
        load_to_postgres(df, "NZ_Traffic_Crash", "raw", dburl)

        # Weather Data Extraction
        csv_file = Path(__file__).parent / "Static" / "MainCities.csv"
        # print(f"Extracted DataFrame: {df.head()}")
        if args.full_reload:
            delete_table_if_exists("NZ_Weather", "raw", dburl)
            progress = {}
            save_progress(progress_file, progress)
        db_max_dates = {} if args.full_reload else get_weather_max_date_per_region(dburl)
        if db_max_dates:
            print(f"Found existing weather data for {len(db_max_dates)} regions in DB; will fetch only missing dates.")
        distinct_regions = pd.DataFrame({"region": df["region"].str.strip().dropna().unique()})
        mainCitices = pd.read_csv(csv_file)
        mainCitices["region"] = mainCitices["region"].str.strip()
        MainCitydf = distinct_regions.merge(mainCitices, on="region", how="left")
        # Keep lat/lon alongside the city name; drop rows with no mapping or coords
        MainCitydf = MainCitydf.dropna(subset=["main city"])
        if "latitude" in MainCitydf.columns:
            MainCitydf = MainCitydf.dropna(subset=["latitude", "longitude"])
        # print(f"Distinct regions extracted: {mainCityList}")
        min_years = df["crashYear"].dropna().unique().min()
        max_years = df["crashYear"].dropna().unique().max()
        global_start = f"{min_years}-01-01"
        end_date = datetime.now(UTC).date().strftime("%Y-%m-%d")
        stopped_early = False
        for _, row in MainCitydf.iterrows():
            region = row["main city"]
            lat = row.get("latitude") if "latitude" in row else None
            lon = row.get("longitude") if "longitude" in row else None

            # 1) Checkpoint resume: already completed for this end_date
            if progress.get(region) == end_date:
                print(f"Skipping {region}: already completed for {end_date} (checkpoint).")
                continue

            # 2) DB incremental: only fetch dates newer than MAX(Date) in raw.NZ_Weather
            fetch_start = global_start
            db_max = db_max_dates.get(region)
            if db_max and db_max >= fetch_start:
                if db_max >= end_date:
                    print(f"Skipping {region}: DB already has data through {db_max}.")
                    progress[region] = end_date
                    save_progress(progress_file, progress)
                    continue
                fetch_start = (datetime.strptime(db_max, "%Y-%m-%d").date() + timedelta(days=1)).strftime("%Y-%m-%d")
                if fetch_start > end_date:
                    progress[region] = end_date
                    save_progress(progress_file, progress)
                    continue
                print(f"Resuming {region}: fetching {fetch_start}..{end_date} (DB has through {db_max}).")
            else:
                print(f"Fetching {region}: {fetch_start}..{end_date}")

            try:
                weather_df = ExtractWeatherData(region, float(lat), float(lon), fetch_start, end_date)
            except DailyRateLimitExceeded as e:
                # Quota exhausted: checkpoint what we have and stop gracefully so next run resumes.
                save_progress(progress_file, progress)
                print(
                    f"Daily/hourly quota hit after {len(progress)} regions. Progress saved to {progress_file}. "
                    f"Re-run later to resume. Details: {e}"
                )
                stopped_early = True
                break
            except Exception as e:
                save_progress(progress_file, progress)
                print(f"Failed on {region}: {e}. Progress saved to {progress_file}. Re-run to resume.", file=sys.stderr)
                raise
            if weather_df is not None:
                print(f"Extracted Weather DataFrame for {region}: {weather_df.head()}")
                load_to_postgres(weather_df, "NZ_Weather", "raw", dburl, if_exists="append")
                progress[region] = end_date
                save_progress(progress_file, progress)
                db_max_dates[region] = end_date
                time.sleep(args.request_delay)
        if stopped_early:
            print(
                f"Stopped early due to rate limit. Completed {len(progress)}/{len(MainCitydf)} regions. Re-run to resume."
            )
        else:
            print(f"Weather backfill complete: {len(progress)}/{len(MainCitydf)} regions up to {end_date}.")
    print(f"Extracted DataFrame: {df.head() if df is not None else 'No data extracted.'}")
