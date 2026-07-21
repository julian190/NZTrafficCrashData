import pandas as pd
import logging

def ExtractData():
    try:
        url = "https://opendata-nzta.opendata.arcgis.com/datasets/8d684f1841fa4dbea6afaefc8a1ba0fc_0.csv"

        df = pd.read_csv(url)
        df["ingested_at"] = pd.Timestamp.now("UTC")

        return df

    except Exception as e:
        logging.error(f"Cannot extract data for NZ traffic crash: {e}")
        raise Exception("Cannot extract data from the source")