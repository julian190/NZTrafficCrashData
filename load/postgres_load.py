import sqlalchemy as sa
import logging 
import pandas as pd
logging.basicConfig(level=logging.INFO)


def load_to_postgres(df : pd.DataFrame, table_name: str,schema: str, db_url: str):
    try:
        engine = sa.create_engine(db_url)
        with engine.begin() as connection:
            logging.info(f"Ensuring schema '{schema}' exists...")
            connection.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

        df.to_sql(table_name, engine,schema = schema, if_exists='replace', index=False)
        logging.info(f"Loaded {len(df)} rows into {table_name} table")
    except Exception as e:
        logging.error(f"Failed to load data into PostgreSQL: {e}")
        raise e


