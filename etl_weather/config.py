from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import os

@dataclass(frozen=True)
class Settings:
    # koordinat & zona waktu default (bisa override via Airflow Variables/env)
    latitude: float = float(os.getenv("WEATHER_LAT", "-7.25"))
    longitude: float = float(os.getenv("WEATHER_LON", "112.75"))
    timezone: str = os.getenv("WEATHER_TZ", "Asia/Jakarta")

    # path file (relatif ke root project / AIRFLOW_HOME)
    raw_json_path: Path = Path(os.getenv("RAW_JSON_PATH", "data/landing/raw_weather_data.json"))
    staging_csv_path: Path = Path(os.getenv("STAGING_CSV_PATH", "data/staging/staging_weather_data.csv"))

    def ensure_dirs(self) -> None:
        self.raw_json_path.parent.mkdir(parents=True, exist_ok=True)
        self.staging_csv_path.parent.mkdir(parents=True, exist_ok=True)

SETTINGS = Settings()
