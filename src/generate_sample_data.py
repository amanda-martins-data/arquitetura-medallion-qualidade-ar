"""
generate_sample_data.py
------------------------
Gera um payload sintético no MESMO formato retornado por `extract.py`
(schema da OpenAQ v3), para permitir rodar e testar o restante do
pipeline (bronze -> silver -> gold) sem precisar de uma API key ou
acesso à internet.

Isso não substitui a extração real — é um fixture de desenvolvimento,
útil para CI, demos e onboarding de quem for revisar o repositório.

Uso:
    python src/generate_sample_data.py
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

CITIES = {
    "São Paulo": [
        {"id": 1001, "name": "São Paulo - Cerqueira César"},
        {"id": 1002, "name": "São Paulo - Ibirapuera"},
    ],
    "Rio de Janeiro": [
        {"id": 2001, "name": "Rio de Janeiro - Centro"},
    ],
    "Belo Horizonte": [
        {"id": 3001, "name": "Belo Horizonte - Pampulha"},
    ],
}

PARAMETERS = [
    {"name": "pm25", "unit": "µg/m³", "range": (5, 60)},
    {"name": "pm10", "unit": "µg/m³", "range": (10, 90)},
    {"name": "o3", "unit": "ppb", "range": (5, 80)},
    {"name": "no2", "unit": "ppb", "range": (2, 50)},
    {"name": "co", "unit": "ppm", "range": (0.1, 2.5)},
]


def generate(days_back: int = 7, readings_per_day: int = 4, seed: int = 42) -> Path:
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    records = []

    for city, stations in CITIES.items():
        for station in stations:
            for day_offset in range(days_back, -1, -1):
                day = now - timedelta(days=day_offset)
                for reading in range(readings_per_day):
                    ts = day.replace(
                        hour=(reading * (24 // readings_per_day)), minute=0, second=0, microsecond=0
                    )
                    for param in PARAMETERS:
                        lo, hi = param["range"]
                        value = round(rng.uniform(lo, hi), 2)
                        records.append(
                            {
                                "parameter": param["name"],
                                "value": value,
                                "unit": param["unit"],
                                "date": {
                                    "utc": ts.isoformat().replace("+00:00", "Z"),
                                    "local": ts.isoformat(),
                                },
                                "coordinates": {"latitude": None, "longitude": None},
                                "_city_query": city,
                                "_location_id": station["id"],
                                "_location_name": station["name"],
                            }
                        )

    payload = {
        "extracted_at": now.isoformat(),
        "date_from": (now - timedelta(days=days_back)).date().isoformat(),
        "date_to": now.date().isoformat(),
        "cities": list(CITIES.keys()),
        "record_count": len(records),
        "results": records,
        "_synthetic": True,
    }

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DATA_DIR / f"openaq_{now.date().isoformat()}.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Gerados {len(records)} registros sintéticos em {out_path}")
    return out_path


if __name__ == "__main__":
    generate()
