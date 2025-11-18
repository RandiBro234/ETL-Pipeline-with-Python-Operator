from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import requests
import pandas as pd
import json
import os

# === CONFIGURATIONS ===
PG_CONN_ID = "postgres_default"
PG_TABLE = "weather_hourly"

BASE_DIR = "/home/randinandika/airflow/dags/data"
LANDING_DIR = f"{BASE_DIR}/landing"
STAGING_DIR = f"{BASE_DIR}/staging"

# Create directories
os.makedirs(LANDING_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

default_args = {
    "owner": "kelompok2",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="ETL pipeline: Open-Meteo → PostgreSQL + CSV",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["weather", "etl", "open-meteo"],
) as dag:

    # ========= EXTRACT ===========
    @task
    def extract() -> str:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": -7.25,
            "longitude": 112.75,
            "hourly": "temperature_2m,rain",
            "timezone": "Asia/Jakarta"
        }

        response = requests.get(url, params=params)
        data = response.json()

        raw_file = f"{LANDING_DIR}/raw_weather.json"
        with open(raw_file, "w") as f:
            json.dump(data, f)

        return raw_file

    # ========= TRANSFORM ===========
    @task
    def transform(raw_path: str) -> str:
        with open(raw_path, "r") as f:
            data = json.load(f)

        df = pd.DataFrame({
            "ts": data["hourly"]["time"],
            "temperature": data["hourly"]["temperature_2m"],
            "rain": data["hourly"]["rain"]
    
        })

        staging_csv = f"{STAGING_DIR}/weather_staging.csv"
        df.to_csv(staging_csv, index=False)

        return staging_csv

    # ========= LOAD → POSTGRES (UPSERT) ===========
    @task
    def load_to_postgres(staging_file: str) -> str:
        postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()

        df = pd.read_csv(staging_file)

        # Load ke temporary table (weather_temp)
        df.to_sql(
            "weather_temp",
            con=engine,
            if_exists="append",
            index=False
        )

        return "Staged into weather_temp"

    # ========= LOAD → CSV BACKUP ===========
    @task
    def load_to_csv(staging_file: str) -> str:
        df = pd.read_csv(staging_file)
        backup_csv = f"{STAGING_DIR}/backup_weather.csv"
        df.to_csv(backup_csv, index=False)
        return backup_csv

    # CREATE FINAL TABLE
    create_weather_table = SQLExecuteQueryOperator(
        task_id="create_weather_table",
        sql="""
            CREATE TABLE IF NOT EXISTS weather_hourly (
                ts TIMESTAMP PRIMARY KEY,
                temperature NUMERIC,
                rain NUMERIC,
                source_file TEXT,
                batch_id TEXT
            );
        """,
        conn_id=PG_CONN_ID,
        autocommit=True,
    )

    # CREATE TEMP TABLE
    create_temp_table = SQLExecuteQueryOperator(
        task_id="create_temp_table",
        sql="""
            DROP TABLE IF EXISTS weather_temp;
            CREATE TABLE weather_temp (
                ts TIMESTAMP PRIMARY KEY,
                temperature NUMERIC,
                rain NUMERIC,
                source_file TEXT,
                batch_id TEXT
            );
        """,
        conn_id=PG_CONN_ID,
        autocommit=True,
    )

    # MERGE / UPSERT
    merge_data = SQLExecuteQueryOperator(
        task_id="merge_data",
        sql="""
            INSERT INTO weather_hourly
            SELECT *
            FROM weather_temp
            ON CONFLICT (ts) DO UPDATE SET
                temperature = excluded.temperature,
                rain = excluded.rain,
                source_file = excluded.source_file,
                batch_id = excluded.batch_id;
        """,
        conn_id=PG_CONN_ID,
        autocommit=True,
    )

    # ORCHESTRATION
    raw = extract()
    staging = transform(raw)

    load_pg = load_to_postgres(staging)
    load_csv = load_to_csv(staging)

    create_weather_table >> create_temp_table >> load_pg >> merge_data >> load_csv
