from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

# import modul ETL (pastikan PYTHONPATH sudah menyertakan ./src)
from etl_weather.extract import extract_weather_data
from etl_weather.transform import transform_weather
from etl_weather.load import load_to_csv
from etl_weather.config import SETTINGS

default_args = {
    "owner": "kelompok2",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="ETL (Extract → Transform → Load) Open-Meteo → CSV staging",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["weather", "open-meteo", "etl"],
) as dag:

    @task(task_id="extract")
    def _extract() -> str:
        # bisa override via env/Airflow Variables; default dari SETTINGS
        return extract_weather_data()

    @task(task_id="transform")
    def _transform(raw_path: str) -> list[dict]:
        return transform_weather(in_path=raw_path)

    @task(task_id="load")
    def _load(records: list[dict]) -> str:
        return load_to_csv(records)

    # Orkestrasi
    raw_path = _extract()
    records = _transform(raw_path)
    _load(records)
