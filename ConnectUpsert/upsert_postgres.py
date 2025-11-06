# ConnectUpsert/upsert_postgres.py
from __future__ import annotations
import pandas as pd
from psycopg2.extras import execute_values
from ConnectUpsert.connect_postgres import get_postgres_connection
from scripts.config import SETTINGS

def upsert_weather_data():
    """
    Membaca staging CSV dan melakukan UPSERT ke tabel 'weather_data' di Postgres.
    """
    csv_path = SETTINGS.staging_csv_path
    df = pd.read_csv(csv_path)

    if df.empty:
        print("❌ Tidak ada data untuk di-upsert.")
        return

    conn = get_postgres_connection()
    cur = conn.cursor()

    records = df.to_records(index=False)
    values = [tuple(r) for r in records]

    upsert_query = """
        INSERT INTO weather_data (date, temperature, rain)
        VALUES %s
        ON CONFLICT (date)
        DO UPDATE SET
            temperature = EXCLUDED.temperature,
            rain = EXCLUDED.rain;
    """

    try:
        execute_values(cur, upsert_query, values)
        conn.commit()
        print(f"✅ {len(values)} baris data berhasil di-upsert ke tabel weather_data.")
    except Exception as e:
        conn.rollback()
        print(f"⚠️ Gagal melakukan upsert: {e}")
    finally:
        cur.close()
        conn.close()
