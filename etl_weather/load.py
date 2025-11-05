from __future__ import annotations
from pathlib import Path
import pandas as pd
from .config import SETTINGS

def load_to_csv(records: list[dict], out_path: str | Path | None = None) -> str:
    """
    Simpan records hasil transform ke CSV staging. Return: path CSV.
    Ganti fungsi ini ke upsert Postgres/BigQuery jika nanti dibutuhkan.
    """
    dst = SETTINGS.staging_csv_path if out_path is None else Path(out_path)
    dst.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame.from_records(records or [])
    df.to_csv(dst, index=False)
    return str(dst)
