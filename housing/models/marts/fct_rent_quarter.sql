{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['barrio_id', 'quarter_start']
) }}

-- grain: barrio x trimestre. order_by (barrio_id, quarter_start) porque el
-- mart de negocio siempre consulta por barrio y luego su evolución temporal.
with barrio_rent as (
    select
        -- coalesce: territory_id/period_start son Nullable en origen y van en
        -- la sorting key, que en ClickHouse no admite columnas Nullable.
        coalesce(territory_id, '') as barrio_id,
        coalesce(period_start, toDate('1970-01-01')) as quarter_start,
        metric_name,
        rent_value
    from {{ ref('stg_bcn__rent') }}
    where territory_type = 'Barri' and period_type = 'Trimestre'
)

select
    barrio_id,
    quarter_start,
    maxIf(rent_value, metric_name = 'rent_price_monthly') as rent_price_monthly,
    maxIf(rent_value, metric_name = 'rent_price_per_m2') as rent_price_per_m2
from barrio_rent
group by barrio_id, quarter_start
