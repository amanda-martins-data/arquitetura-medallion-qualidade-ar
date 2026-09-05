"""
silver_transform.py
--------------------
Segunda camada do lake: lê TODO o histórico da Bronze, aplica as regras
de limpeza/tipagem/deduplicação e regrava a Silver particionada por
`measured_date` (data da medição, não da ingestão).

Diferença de partição em relação à Bronze é proposital: Bronze
particiona por quando o dado chegou (ingestion_date), Silver particiona
por quando o evento aconteceu (measured_date) — é isso que torna a
Silver diretamente consultável por período de interesse do negócio.

Idempotência: cada partição de measured_date é REESCRITA por completo
a cada execução (não é um append). Rodar duas vezes no mesmo dia não
duplica nada — self-healing por reprocessamento total do dia afetado.

Uso:
    python src/silver_transform.py
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

import duckdb
import pandas as pd

BRONZE_DIR = Path(__file__).resolve().parent.parent / "bronze" / "air_quality"
SILVER_DIR = Path(__file__).resolve().parent.parent / "silver" / "air_quality"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("silver_transform")

CLEAN_QUERY = """
    with bronze as (
        select *
        from read_parquet(?, hive_partitioning = true)
    ),
    cleaned as (
        select
            location_id,
            location_name,
            city_query                          as city,
            parameter,
            unit,
            cast(value as double)               as value,
            cast(measured_at_utc as timestamp)   as measured_at_utc,
            cast(extracted_at as timestamp)      as extracted_at,
            _is_synthetic
        from bronze
        where value is not null
          and measured_at_utc is not null
          and value >= 0
    ),
    deduplicated as (
        select
            *,
            row_number() over (
                partition by location_id, parameter, measured_at_utc
                order by extracted_at desc
            ) as rn
        from cleaned
    )
    select
        location_id,
        location_name,
        city,
        parameter,
        unit,
        value,
        measured_at_utc,
        _is_synthetic,
        date(measured_at_utc) as measured_date
    from deduplicated
    where rn = 1
"""


def transform_to_silver() -> int:
    bronze_glob = str(BRONZE_DIR / "**" / "*.parquet")
    con = duckdb.connect()
    df = con.execute(CLEAN_QUERY, [bronze_glob]).fetchdf()
    con.close()

    if df.empty:
        log.warning("Nenhum registro válido encontrado na Bronze (%s)", bronze_glob)
        return 0

    total_rows = 0
    for measured_date, group in df.groupby("measured_date"):
        date_str = pd.Timestamp(measured_date).strftime("%Y-%m-%d")
        partition_dir = SILVER_DIR / f"measured_date={date_str}"
        if partition_dir.exists():
            shutil.rmtree(partition_dir)  # reescrita completa da partição (idempotência)
        partition_dir.mkdir(parents=True, exist_ok=True)

        group = group.drop(columns=["measured_date"])
        group.to_parquet(partition_dir / "data.parquet", index=False, engine="pyarrow")
        total_rows += len(group)
        log.info("Partição measured_date=%s: %d registros", date_str, len(group))

    log.info("Total gravado na Silver: %d registros", total_rows)
    return total_rows


if __name__ == "__main__":
    transform_to_silver()
