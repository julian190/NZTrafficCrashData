import requests
import pandas as pd
import logging
import time
import random


class DailyRateLimitExceeded(Exception):
    """Raised when Open-Meteo daily/hourly/monthly quota is hit (non-retryable now)."""
    pass


# Shared keep-alive session: reuses TCP/TLS across cities (faster, fewer handshakes).
# It does NOT fix resets/rate-limits by itself — retry logic in fetch_with_backoff does.
_SESSION = requests.Session()
_SESSION.headers.update({"User-Agent": "NZTrafficCrashData/1.0"})


#Helper function to handle rate limit + transient network errors with exponential backoff
def fetch_with_backoff(url, max_retries=5, base_delay=5, session=None):
    getter = session.get if session is not None else requests.get
    for attempt in range(max_retries):
        try:
            response = getter(url, timeout=60)
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            # Transient blip (e.g. WinError 10054 reset, DNS, TLS handshake drop).
            # NOT a rate limit — just wait and retry the same request.
            if attempt == max_retries - 1:
                raise
            wait = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"Connection error ({type(e).__name__}), retrying in {wait:.1f}s (attempt {attempt + 1}/{max_retries})...")
            time.sleep(wait)
            continue
        if response.status_code == 429:
            body = (response.text or "").lower()
            # Daily/hourly/monthly quotas need a long stop, not a quick retry.
            # Concurrency ("too many concurrent", "queue is full") is retryable.
            if any(k in body for k in ("daily", "hourly", "monthly", "limit exceeded")):
                raise DailyRateLimitExceeded(
                    f"Open-Meteo quota exhausted: {response.status_code} - {response.text}"
                )
            # Honour server-provided Retry-After when present
            retry_after = response.headers.get("Retry-After")
            try:
                server_wait = float(retry_after) if retry_after else 0
            except ValueError:
                server_wait = 0
            backoff = base_delay * (2 ** attempt) + random.uniform(0, 1)
            wait = max(server_wait, backoff)
            logging.warning(f"Rate limited (429), waiting {wait:.1f}s (attempt {attempt + 1}/{max_retries})...")
            time.sleep(wait)
            continue
        if 500 <= response.status_code < 600:
            # Server-side blip — retryable like a connection error
            if attempt == max_retries - 1:
                response.raise_for_status()
            wait = base_delay * (2 ** attempt) + random.uniform(0, 1)
            logging.warning(f"Server error {response.status_code}, retrying in {wait:.1f}s (attempt {attempt + 1}/{max_retries})...")
            time.sleep(wait)
            continue
        response.raise_for_status()
        return response
    raise Exception("Max retries exceeded for rate limit")


def ExtractData():
    try:
        logging.info("Extracting data for NZ traffic crash")
        url = "https://opendata-nzta.opendata.arcgis.com/datasets/8d684f1841fa4dbea6afaefc8a1ba0fc_0.csv"

        df = pd.read_csv(url)
        df["ingested_at"] = pd.Timestamp.now("UTC")
        logging.info("Data extracted successfully for NZ traffic crash")
        return df

    except Exception as e:
        logging.error(f"Cannot extract data for NZ traffic crash: {e}")
        raise Exception("Cannot extract data from the source")
def ExtractWeatherData(region: str, latitude: float, longitude: float, start_date: str, end_date: str):
    try:
        logging.info(f"Extracting weather for {region} ({latitude},{longitude}) {start_date}..{end_date}")

        # Coordinates now come from load/Static/MainCities.csv — no geocoding call,
        # which halves Open-Meteo request volume and avoids geocoding rate limits.
        if not region:
            raise Exception("Region is not provided")
        if latitude is None or longitude is None:
            raise Exception(f"Coordinates missing for region '{region}' - check MainCities.csv")

        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&daily=rain_sum,temperature_2m_mean&timezone=Pacific/Auckland"
        print(f"Weather Data URL: {url}")
        response = fetch_with_backoff(url, session=_SESSION)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch weather data: {response.status_code} - {response.text}")
        data = response.json()
       # print(f"Weather Data: {data}")

        df = pd.DataFrame({
            "Region": region,
            "Date": data["daily"]["time"],
            "rain_sum": data["daily"]["rain_sum"],
            "temperature_2m_mean": data["daily"]["temperature_2m_mean"]
        }
        )
        df["ingested_at"] = pd.Timestamp.now("UTC")
        logging.info("Data extracted successfully for NZ weather")
        return df

    except DailyRateLimitExceeded:
        # Let quota-exhaustion propagate so the caller can checkpoint and stop gracefully
        raise
    except Exception as e:
        logging.error(f"Cannot extract data for NZ weather: {e}")
        raise Exception("Cannot extract data from the source")