-- grain: expediente del registro municipal de HUT x snapshot (como staging), con
-- barrio_id resuelto. Lo usan fct_hut_registry (licencias con nº HUTB) y
-- fct_hut_registry_barrio (recuento de expedientes, también sin nº HUTB).
with registry as (
    select * from {{ ref('stg_bcn__hut_registry') }}
),

barrios as (
    select
        barrio_id,
        toUInt8(codi_barri) as codi_barri,
        lower(replaceRegexpAll(normalizeUTF8NFKD(nom_barri_oficial), '[^a-zA-Z]', ''))
            as nom_barri_key
    from {{ ref('seed_barrio_mapping') }}
),

-- sin código de barrio (antes de 2022T4) se empareja por nombre normalizado.
-- 2020T1-T2 perdieron los acentos en el origen ("Fam?lia"): cada '?' actúa de
-- comodín de una letra. Se resuelve una vez por nombre distinto (pocas decenas).
names as (
    select distinct
        registry.nom_barri,
        concat(
            '^',
            lower(replaceRegexpAll(
                normalizeUTF8NFKD(replaceAll(registry.nom_barri, '?', '.')),
                '[^a-zA-Z.]',
                ''
            )),
            '$'
        ) as nom_pattern
    from registry
),

name_lookup as (
    select
        n.nom_barri,
        any(b.barrio_id) as barrio_id
    from names as n
    cross join barrios as b
    where match(b.nom_barri_key, n.nom_pattern)
    group by n.nom_barri
)

select
    r.expediente as expediente,  -- noqa: AL09
    r.hut_registro as hut_registro,  -- noqa: AL09
    r.hutb_num as hutb_num,  -- noqa: AL09
    r.plazas as plazas,  -- noqa: AL09
    r.longitude as longitude,  -- noqa: AL09
    r.latitude as latitude,  -- noqa: AL09
    r._snapshot_date as snapshot_date,
    -- ClickHouse rellena el lado no emparejado del LEFT JOIN con '' (no NULL),
    -- de ahí nullIf antes del coalesce.
    coalesce(nullIf(by_code.barrio_id, ''), nullIf(by_name.barrio_id, '')) as barrio_id
from registry as r
left join barrios as by_code on r.codi_barri = by_code.codi_barri
left join name_lookup as by_name on r.nom_barri = by_name.nom_barri
