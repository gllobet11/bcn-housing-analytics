{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    engine='MergeTree()',
    order_by=['barrio_id', 'snapshot_date', 'listing_id'],
    partition_by='toYYYYMM(snapshot_date)'
) }}

-- grain: listing x snapshot trimestral (idéntico a stg_airbnb__listings, ya
-- testeado ahí con unique_combination_of_columns).
-- order_by (barrio_id, snapshot_date, listing_id): las consultas de negocio
-- siempre filtran/agrupan por barrio y trimestre antes que por listing
-- individual, así ClickHouse puede saltar gránulos por barrio+fecha.
-- partition_by toYYYYMM(snapshot_date): cada snapshot trimestral es una carga
-- atómica.
-- incremental_strategy='insert_overwrite': cada carga trimestral cae en su
-- propia partición mensual; dbt-clickhouse construye solo las particiones
-- presentes en los datos nuevos y las reemplaza enteras con REPLACE PARTITION,
-- así que reprocesar el mismo snapshot no duplica filas y el resultado final
-- es idéntico a un --full-refresh procesado partición a partición. No usamos
-- 'delete+insert'/unique_key porque aquí no hay updates fila a fila: cada
-- snapshot_date es una carga atómica, y "append" duplicaría al reejecutar.
with listings as (
    select * from {{ ref('stg_airbnb__listings') }}
    {% if is_incremental() %}
        where _snapshot_date > (
            select max(loaded.snapshot_date) from {{ this }} as loaded
        )
    {% endif %}
),

geography as (
    select
        airbnb_neighbourhood,
        barrio_id
    from {{ ref('seed_barrio_mapping') }}
)

select
    -- coalesce: listing_id, barrio_id (LEFT JOIN) y el hash de
    -- property_type/room_type deben quedar no nulos para ir en la sorting key.
    coalesce(l.listing_id, 0) as listing_id,
    coalesce(g.barrio_id, '') as barrio_id,
    cityHash64(coalesce(l.property_type, ''), coalesce(l.room_type, '')) as property_type_id,
    coalesce(l._snapshot_date, toDate('1970-01-01')) as snapshot_date,
    l.host_id,
    l.host_since,
    l.latitude,
    l.longitude,
    l.accommodates,
    l.bedrooms,
    l.beds,
    l.price,
    l.minimum_nights,
    l.maximum_nights,
    l.availability_30,
    l.availability_60,
    l.availability_90,
    l.availability_365,
    l.number_of_reviews,
    l.first_review,
    l.last_review,
    l.review_scores_rating,
    l.instant_bookable,
    l.license
from listings as l
left join geography as g on l.neighbourhood = g.airbnb_neighbourhood
