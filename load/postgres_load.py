import sqlalchemy as sa
import logging 
import pandas as pd
logging.basicConfig(level=logging.INFO)


def load_to_postgres(df : pd.DataFrame, table_name: str,schema: str, db_url: str, if_exists: str = 'replace'):
    try:
        engine = sa.create_engine(db_url)
        with engine.begin() as connection:
            logging.info(f"Ensuring schema '{schema}' exists...")
            connection.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))
            table_exists = sa.inspect(connection).has_table(table_name, schema=schema)

            if if_exists == 'replace' and table_exists:
                logging.info(f"Truncating existing '{schema}.{table_name}' table...")
                connection.execute(
                    sa.text(f'TRUNCATE TABLE "{schema}"."{table_name}";')
                )

            logging.info(f"Loading data into '{schema}.{table_name}' table...")
            # Append preserves dependent views; pandas creates the table when it
            # does not exist yet.
            df.to_sql(
                table_name,
                connection,
                schema=schema,
                if_exists='append' if if_exists == 'replace' else if_exists,
                index=False,
            )
        logging.info(f"Loaded {len(df)} rows into {table_name} table")
    except Exception as e:
        logging.error(f"Failed to load data into PostgreSQL: {e}")
        raise e
def delete_table_if_exists(table_name: str, schema: str, db_url: str):
    try:
        engine = sa.create_engine(db_url)
        with engine.begin() as connection:
            logging.info(f"Clearing table '{schema}.{table_name}' if it exists...")
            if sa.inspect(connection).has_table(table_name, schema=schema):
                connection.execute(
                    sa.text(f'TRUNCATE TABLE "{schema}"."{table_name}";')
                )
    except Exception as e:
        logging.error(f"Failed to clear table '{schema}.{table_name}': {e}")
        raise e

