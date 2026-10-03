import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Add Airflow home folder to path to ensure modules are importable
sys.path.append("/opt/airflow")

from extract.NZTrafficCrash_extract import ExtractData
from load.postgres_load import load_to_postgres


def run_extract_and_load():
    # Use environment variable inside container, fallback to the docker container link if not set
    db_url = os.getenv("databaseURL") or "postgresql://postgres:postgres@postgresSQL:5432/NZTrafficCrashData"
    df = ExtractData()
    if df is not None:
        load_to_postgres(df, "NZ_Traffic_Crash", "raw", db_url)
    else:
        raise ValueError("Extraction returned empty DataFrame")


with DAG("NZTrafficCrash_pipeline", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    extract_load = PythonOperator(task_id="extract_and_load", python_callable=run_extract_and_load)
    dbt_run = BashOperator(
        task_id="dbt_run", bash_command="cd /opt/airflow/dbt/NZCrashData && dbt run --profiles-dir ."
    )
    dbt_test = BashOperator(
        task_id="dbt_test", bash_command="cd /opt/airflow/dbt/NZCrashData && dbt test --profiles-dir ."
    )
    extract_load >> dbt_run >> dbt_test
