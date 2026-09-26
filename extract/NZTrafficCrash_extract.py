from time import time
import requests
import pandas as pd
import logging
import time

#Helper function to handle rate limit errors with exponential backoff
def fetch_with_backoff(url, max_retries=5, base_delay=5):
    for attempt in range(max_retries):
        response = requests.get(url)
        if response.status_code == 429:
            wait = base_delay * (2 ** attempt)
            logging.warning(f"Rate limited, waiting {wait}s...")
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
def ExtractWeatherData(region:str, start_date:str, end_date:str):
    try:
        logging.info("Extracting data for NZ weather")

        #Get the region coordinates
        if  not region:
            raise Exception("Region is not provided")

        regionUrl = f"https://geocoding-api.open-meteo.com/v1/search?name={region}&countryCode=NZ&count=1&language=en&format=json"
        print(f"Region URL: {regionUrl}")
        data = fetch_with_backoff(regionUrl)

        if data.status_code != 200 or not data.json().get("results"):
            raise Exception(f"Failed to fetch region coordinates: {data.status_code} - {data.text}")
        time.sleep(1.5)

        # Fetch region coordinates
        regionDf = pd.json_normalize(data.json().get("results"))
        print(f"Region DataFrame: {regionDf.head()}")
        latitude = regionDf.iloc[0]["latitude"]
        longitude = regionDf.iloc[0]["longitude"]

        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&daily=rain_sum,temperature_2m_mean&timezone=Pacific/Auckland"
        print(f"Weather Data URL: {url}")
        response = fetch_with_backoff(url)
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

    except Exception as e:
        logging.error(f"Cannot extract data for NZ weather: {e}")
        raise Exception("Cannot extract data from the source")