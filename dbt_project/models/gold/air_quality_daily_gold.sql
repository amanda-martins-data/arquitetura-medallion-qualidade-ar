{{
    config(
        materialized='external',
        location='../gold/air_quality_daily/data.parquet'
    )
}}

-- air_quality_daily_gold
-- Camada Gold: uma linha por (cidade, poluente, dia), com a
-- classificação de Índice de Qualidade do Ar (AQI) aplicada quando o
-- poluente é PM2.5 — a métrica mais usada por orgãos ambientais para
-- comunicar risco à saúde ao público em geral.
--
-- Faixas de PM2.5 (µg/m³, média de 24h) simplificadas a partir da
-- escala da EPA (US): referência pública, não substitui a escala
-- oficial de cada país para fins regulatórios.

with silver as (
    select * from {{ source('silver', 'air_quality') }}
),

aggregated as (
    select
        city,
        parameter,
        unit,
        measured_date,
        count(*)              as reading_count,
        round(avg(value), 2)  as avg_value,
        round(min(value), 2)  as min_value,
        round(max(value), 2)  as max_value,
        bool_or(_is_synthetic) as is_synthetic
    from silver
    group by 1, 2, 3, 4
)

select
    *,
    case
        when parameter != 'pm25' then null
        when avg_value <= 12.0   then 'Boa'
        when avg_value <= 35.4   then 'Moderada'
        when avg_value <= 55.4   then 'Insalubre p/ grupos sensíveis'
        when avg_value <= 150.4  then 'Insalubre'
        else 'Muito insalubre'
    end as aqi_category_pm25
from aggregated
order by measured_date desc, city, parameter
