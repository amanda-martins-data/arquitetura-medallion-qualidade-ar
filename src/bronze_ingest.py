"""
bronze_ingest.py
-----------------
Primeira camada do lake: lê o(s) JSON(s) bruto(s) da zona de landing
(gerados por extract.py ou generate_sample_data.py) e grava na camada
Bronze em Parquet, particionado por data de ingestão.

Bronze é a camada de "verdade histórica": nunca é sobrescrita nem
limpa — cada execução adiciona um novo arquivo particionado por
`ingestion_date`. Se uma regra de limpeza da Silver mudar amanhã, dá
para reprocessar tudo a partir daqui sem precisar re-extrair da fonte.

Uso:
    python src/bronze_ingest.py
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

LANDING_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
BRONZE_DIR = Path(__file__).resolve().parent.parent / "bronze" / "air_quality"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("bronze_ingest")


def flatten_record(record: dict, extracted_at: str, source_file: str) -> dict:
    """Achata o registro da API sem aplicar NENHUMA regra de limpeza —
    isso é responsabilidade da camada Silver. Bronze preserva o dado
    o mais próximo possível do formato de origem."""
    return {
        "city_query": record.get("_city_query"),
        "location_id": record.get("_location_id"),
        "location_name": record.get("_location_name"),
        "parameter": record.get("parameter"),
        "value": record.get("value"),
        "unit": record.get("unit"),
        "measured_at_utc": record.get("date", {}).get("utc"),
        "measured_at_local": record.get("date", {}).get("local"),
        "latitude": record.get("coordinates", {}).get("latitude"),
        "longitude": record.get("coordinates", {}).get("longitude"),
        "extracted_at": extracted_at,
        "_source_file": source_file,
        "_is_synthetic": bool(record.get("_synthetic", False)),
    }


def ingest_to_bronze(ingestion_date: date | None = None) -> Path:
    ingestion_date = ingestion_date or date.today()
    files = sorted(LANDING_DIR.glob("openaq_*.json"))
    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo openaq_*.json em {LANDING_DIR}. "
            "Rode src/extract.py ou src/generate_sample_data.py primeiro."
        )

    rows: list[dict] = []
    for f in files:
        payload = json.loads(f.read_text(encoding="utf-8"))
        extracted_at = payload.get("extracted_at")
        is_synthetic = payload.get("_synthetic", False)
        for record in payload.get("results", []):
            record["_synthetic"] = is_synthetic
            rows.append(flatten_record(record, extracted_at, f.name))
        log.info("Lido %s (%d registros)", f.name, len(payload.get("results", [])))

    df = pd.DataFrame(rows)
    df["ingestion_date"] = ingestion_date.isoformat()
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    partition_dir = BRONZE_DIR / f"ingestion_date={ingestion_date.isoformat()}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    out_path = partition_dir / f"part-{datetime.now(timezone.utc).strftime('%H%M%S')}.parquet"
    df.to_parquet(out_path, index=False, engine="pyarrow")

    log.info("Gravados %d registros em %s", len(df), out_path)
    return out_path


if __name__ == "__main__":
    ingest_to_bronze()
