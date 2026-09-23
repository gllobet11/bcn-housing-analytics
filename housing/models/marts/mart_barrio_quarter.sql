{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['barrio_id', 'quarter_start']
) }}

-- grain: barrio x trimestre. KPIs del kickoff + variación trimestral (QoQ)
-- de cada uno, calculada con lagInFrame sobre la propia serie del barrio.
with listing_kpis as (
    select
        f.barrio_id,
        toStartOfQuarter(f.snapshot_date) as quarter_start,
        count(distinct f.listing_id) as listings_activos,
        countIf(pt.room_type = 'Entire home/apt') / count()::Float64 as pct_alojamiento_entero,
        avg(f.price) as precio_medio_noche
    from {{ ref('fct_listing_snapshot') }} as f
    left join {{ ref('dim_property_type') }} as pt on f.property_type_id = pt.property_type_id
    group by f.barrio_id, quarter_start
),

rent as (
    select
        barrio_id,
        quarter_start,
        rent_price_monthly,
        rent_price_per_m2
    from {{ ref('fct_rent_quarter') }}
),

-- ClickHouse rellena el lado no-emparejado de un FULL JOIN con el valor por
-- defecto del tipo (p. ej. '' o 1970-01-01), no con NULL, así que un
-- coalesce(k.barrio_id, r.barrio_id) no detectaría el lado ausente. Se
-- calculan antes las claves (barrio, trimestre) que existen en cualquiera de
-- las dos fuentes y se hace LEFT JOIN de cada una sobre ese spine.
keys as (
    select
        barrio_id,
        quarter_start
    from listing_kpis
    union distinct
    select
        barrio_id,
        quarter_start
    from rent
),

-- ON explícito en vez de USING: con USING, lagInFrame() sobre la columna de
-- join deja de resolverse ("Unknown expression identifier") en esta versión
-- de ClickHouse.
joined as (
    select
        -- el auto-alias es necesario: sin él, el WINDOW de la query final no
        -- resuelve barrio_id/quarter_start ("Unknown expression identifier")
        -- en esta versión de ClickHouse.
        ky.barrio_id as barrio_id,  -- noqa: AL09
        ky.quarter_start as quarter_start,  -- noqa: AL09
        k.listings_activos,
        k.pct_alojamiento_entero,
        k.precio_medio_noche,
        r.rent_price_monthly,
        r.rent_price_per_m2
    from keys as ky
    left join listing_kpis as k on ky.barrio_id = k.barrio_id and ky.quarter_start = k.quarter_start
    left join rent as r on ky.barrio_id = r.barrio_id and ky.quarter_start = r.quarter_start
)

select
    barrio_id,
    quarter_start,
    listings_activos,
    pct_alojamiento_entero,
    precio_medio_noche,
    rent_price_monthly,
    rent_price_per_m2,
    listings_activos - lagInFrame(listings_activos) over quarter_window as var_listings_activos,
    pct_alojamiento_entero - lagInFrame(pct_alojamiento_entero) over quarter_window
        as var_pct_alojamiento_entero,
    precio_medio_noche - lagInFrame(precio_medio_noche) over quarter_window
        as var_precio_medio_noche,
    rent_price_monthly - lagInFrame(rent_price_monthly) over quarter_window
        as var_rent_price_monthly,
    rent_price_per_m2 - lagInFrame(rent_price_per_m2) over quarter_window as var_rent_price_per_m2
from joined
window quarter_window as (partition by barrio_id order by quarter_start)
