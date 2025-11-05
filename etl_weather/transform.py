from __future__ import annotations
from pathlib import Path
import json
import pandas as pd
from .config import SETTINGS

def transform_weather(in_path: str | Path | None = None) -> list[dict]:
    """
    Baca raw JSON, lakukan validasi ringan & cleaning, kembalikan list of records.
    Validasi ringan disisipkan di tahap Transform agar ETL tetap 3 langkah.
    """
    src = SETTINGS.raw_json_path if in_path is None else Path(in_path)
    if not src.exists():
        raise FileNotFoundError(f"Raw JSON not found: {src}")

    with src.open() as f:
        data = json.load(f)

    hourly = data.get("hourly") or {}
    for k in ("time", "temperature_2m", "rain"):
        if k not in hourly:
            raise ValueError(f"Missing key 'hourly.{k}'")

    df = pd.DataFrame({
        "date": hourly["time"],
        "temperature": hourly["temperature_2m"],
        "rain": hourly["rain"],
    })

    # cleaning & type casting
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).reset_index(drop=True)

    # di sini kamu bisa tambah feature engineering kalau perlu
    return df.to_dict(orient="records")
