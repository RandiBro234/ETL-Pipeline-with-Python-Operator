# ConnectUpsert/connect_postgres.py
from __future__ import annotations
import psycopg2
from psycopg2.extensions import connection
import os

def get_postgres_connection() -> connection:
    """
    Membuat koneksi ke PostgreSQL menggunakan environment variable.
    Pastikan variabel berikut sudah diset:
    - POSTGRES_HOST
    - POSTGRES_PORT
    - POSTGRES_DB
    - POSTGRES_USER
    - POSTGRES_PASSWORD
    """
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "weather_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres")
    )
    return conn
