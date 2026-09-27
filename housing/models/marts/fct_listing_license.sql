{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['snapshot_date', 'listing_id']
) }}

-- grain: listing x snapshot de Inside Airbnb (mismo que fct_listing_snapshot),
-- con la licencia declarada clasificada y, si declara un HUTB, contrastada
-- contra el registro municipal vigente en esa fecha.
with listings as (
    select
        f.listing_id,
        f.snapshot_date,
        f.barrio_id,
        f.host_id,
        f.longitude,
        f.latitude,
        f.accommodates,
        f.minimum_nights,
        pt.room_type,
        f.license,
        toUInt32OrNull(extract(f.license, 'HUTB-?(\\d+)')) as hutb_num
    from {{ ref('fct_listing_snapshot') }} as f
    left join {{ ref('dim_property_type') }} as pt on f.property_type_id = pt.property_type_id
),

registry as (
    select
        snapshot_date,
        hutb_num,
        barrio_id,
        plazas,
        longitude,
        latitude,
        1 as found
    from {{ ref('fct_hut_registry') }}
),

-- el registro vigente en cada snapshot de Airbnb es el último publicado antes.
-- No vale un ASOF JOIN por hutb_num: encontraría la última aparición de esa
-- licencia aunque ya se hubiera dado de baja.
registry_as_of as (
    select
        l.snapshot_date,
        max(r.snapshot_date) as registry_snapshot
    from (select distinct snapshot_date from listings) as l
    cross join (select distinct snapshot_date from registry) as r
    where r.snapshot_date <= l.snapshot_date
    group by l.snapshot_date
),

license_usage as (
    select
        snapshot_date,
        hutb_num,
        count() as n_anuncios_misma_licencia,
        uniqExact(host_id) as n_hosts_misma_licencia
    from listings
    where hutb_num is not null
    group by snapshot_date, hutb_num
)

-- alias explícitos (aunque sqlfluff los marque redundantes, AL09): tras varios
-- JOIN, ClickHouse 24.8 no resuelve `l.col` por su nombre corto en el ORDER BY
-- de la tabla destino (mismo bug que en mart_barrio_quarter).
select
    l.listing_id as listing_id,  -- noqa: AL09
    l.snapshot_date as snapshot_date,  -- noqa: AL09
    l.barrio_id as barrio_id,  -- noqa: AL09
    l.host_id as host_id,  -- noqa: AL09
    l.room_type as room_type,  -- noqa: AL09
    l.accommodates as accommodates,  -- noqa: AL09
    l.minimum_nights as minimum_nights,  -- noqa: AL09
    l.license as license,  -- noqa: AL09
    -- HUTB primero: muchos anuncios declaran a la vez el registro autonómico y
    -- el nacional (ESFCTU turístico / ESFCNT no turístico, obligatorio desde 07/2025).
    multiIf(
        l.hutb_num is not null, 'hutb',
        match(l.license, 'ESFCTU'), 'registro_nacional_turistico',
        match(l.license, 'ESFCNT'), 'registro_nacional_no_turistico',
        match(l.license, '(?i)exempt'), 'exempt',
        ifNull(l.license, '') = '', 'sin_licencia',
        'otra'
    ) as license_type,
    l.hutb_num as hutb_num,  -- noqa: AL09
    a.registry_snapshot as registry_snapshot,  -- noqa: AL09
    if(l.hutb_num is null, null, r.found = 1) as hutb_en_registro,
    if(r.found = 1, r.barrio_id = l.barrio_id, null) as hutb_mismo_barrio,
    if(
        r.found = 1,
        greatCircleDistance(l.longitude, l.latitude, r.longitude, r.latitude),
        null
    ) as hutb_distancia_m,
    if(r.found = 1, r.plazas, null) as hutb_plazas_registro,
    u.n_anuncios_misma_licencia as n_anuncios_misma_licencia,  -- noqa: AL09
    u.n_hosts_misma_licencia as n_hosts_misma_licencia  -- noqa: AL09
from listings as l
left join registry_as_of as a on l.snapshot_date = a.snapshot_date
left join registry as r
    on a.registry_snapshot = r.snapshot_date and l.hutb_num = r.hutb_num
left join license_usage as u
    on l.snapshot_date = u.snapshot_date and l.hutb_num = u.hutb_num
