from dotenv import load_dotenv
import os 
from load.postgres_load import delete_table_if_exists, load_to_postgres
from extract.NZTrafficCrash_extract import ExtractData
from extract.NZTrafficCrash_extract import ExtractWeatherData
from datetime import datetime,UTC, time
import pandas as pd
from pathlib import Path
import time

load_dotenv('.env')
dburl = os.getenv('databaseURL')
#print(f"Database URL: {dburl}")

if __name__ == "__main__":
    # Example usage
    df = ExtractData()
  
   
    if df is not None:
        delete_table_if_exists("NZ_Traffic_Crash", "raw", dburl)
        load_to_postgres(df, "NZ_Traffic_Crash","raw",dburl)

         #Weather Data Extraction
        csv_file = Path(__file__).parent / "Static" / "MainCities.csv"
        #print(f"Extracted DataFrame: {df.head()}")
        delete_table_if_exists("NZ_Weather", "raw", dburl)
        distinct_regions = pd.DataFrame({
        'region': df['region'].str.strip().dropna().unique()
        })
        mainCitices = pd.read_csv(csv_file)
        mainCitices['region'] = mainCitices['region'].str.strip()
        MainCitydf = distinct_regions.merge(mainCitices, on='region', how='left')
        mainCityList = MainCitydf['main city'].dropna().unique().tolist()
        #print(f"Distinct regions extracted: {mainCityList}")
        min_years = df['crashYear'].dropna().unique().min()
        max_years = df['crashYear'].dropna().unique().max()
        start_date = f"{min_years}-01-01"
        end_date = datetime.now(UTC).date().strftime("%Y-%m-%d")
        for region in mainCityList:
            weather_df = ExtractWeatherData(region, start_date, end_date)
            if weather_df is not None:
                print(f"Extracted Weather DataFrame for {region}: {weather_df.head()}")
                load_to_postgres(weather_df, "NZ_Weather","raw",dburl, if_exists='append')
                time.sleep(2)
    print(f"Extracted DataFrame: {df.head() if df is not None else 'No data extracted.'}")
  