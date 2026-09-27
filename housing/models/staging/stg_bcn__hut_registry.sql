with source as (
    select * from {{ source('raw', 'bcn_hut_registry') }}
)

-- CODI_BARRI no existe antes de 2022T4 (los nombres de columna que cambiaron
-- entre trimestres ya se unifican en la ingesta). Columnas del origen en
-- mayúsculas: entre backticks para sqlfluff (CP02), ClickHouse distingue case.
select
    `N_EXPEDIENT` as expediente,
    `NUMERO_REGISTRE_GENERALITAT` as hut_registro,
    -- solo la parte numérica: el formato varía (HUTB-002222 / HUTB002222 /
    -- HUTB0002222) igual que en el campo license de Inside Airbnb.
    toUInt32OrNull(extract(`NUMERO_REGISTRE_GENERALITAT`, '^HUTB-?(\\d+)$')) as hutb_num,
    toUInt8OrNull(`CODI_BARRI`) as codi_barri,
    nullIf(`NOM_BARRI`, '') as nom_barri,
    toUInt16OrNull(`NUMERO_PLACES`) as plazas,
    -- coma decimal en algunas filas reparadas de 2021T3
    toFloat64OrNull(replaceOne(`LONGITUD_X`, ',', '.')) as longitude,
    toFloat64OrNull(replaceOne(`LATITUD_Y`, ',', '.')) as latitude,
    _snapshot_date,
    _loaded_at
from source
-- 2023T4 trae ~4.200 filas de relleno sin expediente, sin HUTB y con la misma
-- coordenada (fuera de Barcelona): no son registros.
where `N_EXPEDIENT` != ''
