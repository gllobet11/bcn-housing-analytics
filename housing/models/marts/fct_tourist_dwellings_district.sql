{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['distrito_id', 'periodo']
) }}

-- grain: distrito municipal de Barcelona x periodo del INE. En Barcelona los
-- distritos censales del INE (08019DD) coinciden con los 10 distritos municipales.
with districts as (
    select distinct
        distrito_id,
        toUInt8(codi_districte) as codi_districte
    from {{ ref('seed_barrio_mapping') }}
)

select
    d.distrito_id as distrito_id,  -- noqa: AL09
    i.periodo as periodo,  -- noqa: AL09
    i.viviendas_turisticas as viviendas_turisticas,  -- noqa: AL09
    i.plazas as plazas,  -- noqa: AL09
    i.pct_viviendas_turisticas as pct_viviendas_turisticas  -- noqa: AL09
from {{ ref('stg_ine__tourist_dwellings') }} as i
inner join districts as d
    on toUInt8OrZero(substring(i.distrito_ine, 6, 2)) = d.codi_districte
where i.municipio_ine = '08019'
