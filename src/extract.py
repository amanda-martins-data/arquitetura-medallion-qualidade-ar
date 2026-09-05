"""
extract.py
----------
Camada de extração (E do ELT): busca medições de qualidade do ar na API
pública da OpenAQ (https://openaq.org) para um conjunto de cidades
brasileiras e salva o payload bruto em disco, particionado por data.

A OpenAQ v3 exige uma API key gratuita (https://explore.openaq.org/register).
A key deve ser exportada como variável de ambiente OPENAQ_API_KEY.

Uso:
    export OPENAQ_API_KEY="sua_chave_aqui"
    python src/extract.py --cidades "São Paulo,Rio de Janeiro,Belo Horizonte"
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

BASE_URL = "https://api.openaq.org/v3"
RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("extract")


class OpenAQClient:
    """Client fino sobre a API v3 da OpenAQ."""

    def __init__(self, api_key: str, timeout: int = 30):
        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key})
        self.timeout = timeout

    def find_locations(self, city: str, country_iso: str = "BR", limit: int = 10) -> list[dict]:
        """Resolve o nome de uma cidade para IDs de estações de monitoramento."""
        resp = self.session.get(
            f"{BASE_URL}/locations",
            params={"iso": country_iso, "limit": limit, "city": city},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])

    def get_measurements(
        self, location_id: int, date_from: str, date_to: str, limit: int = 1000
    ) -> list[dict]:
        """Busca medições de uma estação em um intervalo de datas (ISO 8601)."""
        resp = self.session.get(
            f"{BASE_URL}/locations/{location_id}/measurements",
            params={"date_from": date_from, "date_to": date_to, "limit": limit},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])


def extract_for_cities(cities: list[str], days_back: int = 7) -> Path:
    api_key = os.environ.get("OPENAQ_API_KEY")
    if not api_key:
        log.error(
            "Variável OPENAQ_API_KEY não definida. "
            "Registre-se gratuitamente em https://explore.openaq.org/register "
            "e exporte a chave antes de rodar este script."
        )
        sys.exit(1)

    client = OpenAQClient(api_key)
    today = date.today()
    date_from = (today - timedelta(days=days_back)).isoformat()
    date_to = today.isoformat()

    all_records: list[dict] = []
    for city in cities:
        log.info("Buscando estações para %s", city)
        locations = client.find_locations(city)
        if not locations:
            log.warning("Nenhuma estação encontrada para %s", city)
            continue

        for loc in locations:
            location_id = loc["id"]
            log.info("  Coletando medições da estação %s (%s)", location_id, loc.get("name"))
            try:
                measurements = client.get_measurements(location_id, date_from, date_to)
            except requests.HTTPError as exc:
                log.warning("  Falha ao buscar estação %s: %s", location_id, exc)
                continue

            for m in measurements:
                m["_city_query"] = city
                m["_location_id"] = location_id
                m["_location_name"] = loc.get("name")
            all_records.extend(measurements)

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DATA_DIR / f"openaq_{today.isoformat()}.json"
    payload = {
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "date_from": date_from,
        "date_to": date_to,
        "cities": cities,
        "record_count": len(all_records),
        "results": all_records,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Salvos %d registros em %s", len(all_records), out_path)
    return out_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extrai dados de qualidade do ar da OpenAQ.")
    parser.add_argument(
        "--cidades",
        type=str,
        default="São Paulo,Rio de Janeiro,Belo Horizonte",
        help="Lista de cidades separadas por vírgula.",
    )
    parser.add_argument("--dias", type=int, default=7, help="Janela de dias para trás.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cities = [c.strip() for c in args.cidades.split(",") if c.strip()]
    extract_for_cities(cities, days_back=args.dias)
