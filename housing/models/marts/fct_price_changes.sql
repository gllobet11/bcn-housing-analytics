{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['listing_id', 'changed_at']
) }}

-- grain: un cambio de precio detectado por snp_listings (SCD2 check sobre
-- price/room_type/availability_365). No todo registro de snp_listings es un
-- cambio de precio (puede haber cambiado room_type o availability_365 solo),
-- así que se filtra explícitamente donde el precio difiere del anterior.
with history as (
    select
        -- coalesce: listing_id es Nullable en el snapshot y no puede ir en la
        -- sorting key.
        coalesce(listing_id, 0) as listing_id,
        price,
        dbt_valid_from as changed_at,
        lagInFrame(price) over (
            partition by listing_id order by dbt_valid_from
        ) as previous_price
    from {{ ref('snp_listings') }}
)

select
    listing_id,
    changed_at,
    previous_price,
    price as new_price
from history
where
    previous_price is not null
    and previous_price != price
