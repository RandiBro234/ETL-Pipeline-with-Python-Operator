# ConnectUpsert/connect_postgres.py
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

def get_postgres_connection(pg_conn_id: str = "postgres_default"):
    """
    Menggunakan PostgresHook untuk mengelola koneksi ke PostgreSQL.
    Menggunakan Airflow Connection yang sudah terdefinisi untuk menghindari hardcode kredensial.
    """
    # Menggunakan PostgresHook untuk mendapatkan koneksi
    hook = PostgresHook(postgres_conn_id=pg_conn_id)
    return hook.get_sqlalchemy_engine()  # Mengambil engine SQLAlchemy yang siap digunakan
