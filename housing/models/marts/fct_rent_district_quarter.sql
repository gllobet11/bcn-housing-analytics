{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['distrito_id', 'quarter_start']
) }}

-- grain: distrito x trimestre. Valor oficial de Barcelona Dades para el
-- distrito (no la media de sus barrios, que no pondera por nº de contratos).
-- territory_id de los distritos ('lDis...') es el mismo distrito_id del seed.
with district_rent as (
    select
        -- coalesce: Nullable en origen y van en la sorting key
        coalesce(territory_id, '') as distrito_id,
        coalesce(period_start, toDate('1970-01-01')) as quarter_start,
        metric_name,
        rent_value
    from {{ ref('stg_bcn__rent') }}
    where territory_type = 'Districte' and period_type = 'Trimestre'
)

select
    distrito_id,
    quarter_start,
    maxIf(rent_value, metric_name = 'rent_price_monthly') as rent_price_monthly,
    maxIf(rent_value, metric_name = 'rent_price_per_m2') as rent_price_per_m2
from district_rent
group by distrito_id, quarter_start
