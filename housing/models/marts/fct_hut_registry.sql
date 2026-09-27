{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['snapshot_date', 'hutb_num']
) }}

-- grain: licencia HUT (nº HUTB) x snapshot trimestral del registro municipal.
-- Solo expedientes con nº HUTB: sin él no hay cruce posible con los anuncios.
with registry as (
    select * from {{ ref('stg_bcn__hut_registry') }}
    where hutb_num is not null
),

barrios as (
    select
        barrio_id,
        toUInt8(codi_barri) as codi_barri,
        lower(replaceRegexpAll(normalizeUTF8NFKD(nom_barri_oficial), '[^a-zA-Z]', ''))
            as nom_barri_key
    from {{ ref('seed_barrio_mapping') }}
),

located as (
    select
        r._snapshot_date as snapshot_date,
        -- coalesce: hutb_num es Nullable en staging (ya filtrado arriba) y no
        -- puede ir en la sorting key.
        coalesce(r.hutb_num, 0) as hutb_num,
        r.hut_registro,
        r.plazas,
        r.longitude,
        r.latitude,
        -- por código si viene (desde 2022T4); si no, por nombre normalizado.
        -- ClickHouse rellena el lado no emparejado del LEFT JOIN con '' (no
        -- NULL), de ahí nullIf antes del coalesce.
        coalesce(nullIf(by_code.barrio_id, ''), nullIf(by_name.barrio_id, ''))
            as barrio_id
    from registry as r
    left join barrios as by_code on r.codi_barri = by_code.codi_barri
    left join barrios as by_name
        on
            lower(replaceRegexpAll(normalizeUTF8NFKD(coalesce(r.nom_barri, '')), '[^a-zA-Z]', ''))
            = by_name.nom_barri_key
)

select
    snapshot_date,
    hutb_num,
    any(hut_registro) as hut_registro,
    any(barrio_id) as barrio_id,
    any(plazas) as plazas,
    any(longitude) as longitude,
    any(latitude) as latitude,
    -- >1: el mismo nº HUTB aparece en varios expedientes del registro
    count() as n_expedientes
from located
group by snapshot_date, hutb_num
