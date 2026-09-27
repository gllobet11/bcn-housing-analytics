with source as (
    select * from {{ source('raw', 'ine_tourist_dwellings') }}
)

-- una fila por distrito censal de España y periodo (estadística experimental
-- del INE, dos periodos al año desde 2020-08).
select
    codigo as distrito_ine,
    mun as municipio_ine,
    toUInt32OrNull(vivienda_turistica) as viviendas_turisticas,
    toUInt32OrNull(plazas) as plazas,
    -- % de viviendas turísticas sobre el total de viviendas del distrito (censo)
    toFloat64OrNull(porcentaje_vivienda_turistica) as pct_viviendas_turisticas,
    _snapshot_date as periodo,
    _loaded_at
from source
