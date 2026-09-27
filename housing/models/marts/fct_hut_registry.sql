{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['snapshot_date', 'hutb_num']
) }}

-- grain: licencia HUT (nº HUTB) x snapshot trimestral del registro municipal.
-- Solo expedientes con nº HUTB: sin él no hay cruce posible con los anuncios.
-- El filtro va en una CTE: en ClickHouse un WHERE sobre `hutb_num` en la misma
-- query se resolvería contra el alias `coalesce(hutb_num, 0)`, nunca NULL.
with with_hutb as (
    select * from {{ ref('int_hut_registry__located') }}
    where hutb_num is not null
)

select
    snapshot_date,
    -- coalesce: hutb_num es Nullable y no puede ir en la sorting key
    coalesce(hutb_num, 0) as hutb_num,
    any(hut_registro) as hut_registro,
    any(barrio_id) as barrio_id,
    any(plazas) as plazas,
    any(longitude) as longitude,
    any(latitude) as latitude,
    -- >1: el mismo nº HUTB aparece en varios expedientes del registro
    count() as n_expedientes
from with_hutb
group by snapshot_date, hutb_num
