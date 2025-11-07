from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# Import modul ETL
from etl_weather.extract import extract_weather_data
from etl_weather.transform import transform_weather
from etl_weather.load import load_to_csv, load_to_postgres  # <-- Import load_to_postgres dan load_to_csv
from etl_weather.config import SETTINGS

# Airflow Variables untuk koneksi
PG_CONN_ID = Variable.get("PG_CONN_ID", default_var="postgres_default")
PG_TABLE   = Variable.get("PG_TABLE",   default_var="public.weather_hourly")

default_args = {
    "owner": "kelompok2",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="ETL (Extract → Transform → Load) Open-Meteo → Postgres and CSV",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["weather", "open-meteo", "etl"],
) as dag:

    @task
    def extract() -> str:
        """Extract data from Open-Meteo API"""
        return extract_weather_data()

    @task
    def transform(raw_path: str) -> list[dict]:
        """Transform the raw JSON into a structured format"""
        return transform_weather(in_path=raw_path)

    @task
    def load_pg(records: list[dict]) -> str:
        """Load data into Postgres (with UPSERT)"""
        return load_to_postgres(records, table=PG_TABLE, pg_conn_id=PG_CONN_ID)

    @task
    def load_csv(records: list[dict]) -> str:
        """Save data to CSV (backup or audit)"""
        return load_to_csv(records, out_path="data/staging/staging_weather_data.csv")

    # Orkestrasi antar task (Extract → Transform → Load ke Postgres dan CSV)
    raw_path = extract()
    records = transform(raw_path)

    # Memuat data ke Postgres
    load_pg(records)

    # Memuat data ke CSV (Opsional)
    load_csv(records)
