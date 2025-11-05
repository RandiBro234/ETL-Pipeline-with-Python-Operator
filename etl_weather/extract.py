from __future__ import annotations
from pathlib import Path
import json
import requests
from .config import SETTINGS

def extract_weather_data(
    lat: float | None = None,
    lon: float | None = None,
    tz: str | None = None,
    out_path: str | Path | None = None,
) -> str:
    """
    Ambil data cuaca dari Open-Meteo dan simpan sebagai JSON (raw).
    Return: path file JSON (string).
    """
    latitude = SETTINGS.latitude if lat is None else lat
    longitude = SETTINGS.longitude if lon is None else lon
    timezone = SETTINGS.timezone if tz is None else tz
    out = SETTINGS.raw_json_path if out_path is None else Path(out_path)

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,rain",
        "timezone": timezone,
    }
    resp = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        json.dump(data, f)
    return str(out)
