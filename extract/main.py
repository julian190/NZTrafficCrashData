from extract.NZTrafficCrash_extract import ExtractData

if __name__ == "__main__":
    df = ExtractData()
    if df is not None:
        print(df.head())
    else:
        print("No data extracted.")
