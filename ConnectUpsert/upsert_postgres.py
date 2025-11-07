from sqlalchemy import Table, MetaData, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from ConnectUpsert.connect_postgres import get_postgres_connection
import pandas as pd

def upsert_postgres(records: list[dict], table: str = "public.weather_hourly", pg_conn_id: str = "postgres_default") -> str:
    """
    Melakukan UPSERT data ke PostgreSQL dengan menghandle konflik berdasarkan PRIMARY KEY (ts).
    """
    # Membuat koneksi dan mendapatkan SQLAlchemy engine
    engine = get_postgres_connection(pg_conn_id)
    metadata = MetaData(bind=engine)
    
    # Mendapatkan objek table dari database (sesuaikan dengan skema tabel)
    weather_table = Table(table, metadata, autoload_with=engine)

    with engine.begin() as connection:
        # 1) Pastikan tabel target ada
        connection.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
              ts TIMESTAMP PRIMARY KEY,
              temperature NUMERIC,
              rain NUMERIC,
              source_file TEXT,
              batch_id TEXT
            );
        """)

        # 2) Buat staging (kosong) dan load data ke staging
        staging = table + "_stg"  # Tabel sementara (staging)
        connection.execute(f"DROP TABLE IF EXISTS {staging};")
        connection.execute(f"CREATE TABLE {staging} (LIKE {table} INCLUDING ALL);")

        # 3) Memasukkan data ke staging table (gunakan pandas)
        df = pd.DataFrame.from_records(records)
        df = df.rename(columns={"date": "ts"})  # Mapping nama kolom 'date' ke 'ts'

        # Insert data ke tabel staging
        df.to_sql(staging.split(".")[-1], con=engine, if_exists="append", index=False)

        # 4) UPSERT dari staging ke target
        connection.execute(f"""
            INSERT INTO {table} AS t (ts, temperature, rain, source_file, batch_id)
            SELECT ts, temperature, rain, source_file, batch_id
            FROM {staging}
            ON CONFLICT (ts) DO UPDATE SET
              temperature = EXCLUDED.temperature,
              rain        = EXCLUDED.rain,
              source_file = EXCLUDED.source_file,
              batch_id    = EXCLUDED.batch_id;
        """)

        # 5) Bersihkan staging
        connection.execute(f"DROP TABLE IF EXISTS {staging};")

    return f"Successfully upserted {len(records)} rows into {table}"
