{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['barrio_id', 'snapshot_date']
) }}

-- grain: barrio x snapshot trimestral del registro municipal (2018T2 -> hoy).
-- Cuenta expedientes, no nº HUTB: antes de 2020T3 y en 2024T2 el registro no
-- publica el nº, pero sí sirve para la evolución del parque de licencias.
select
    coalesce(barrio_id, '') as barrio_id,
    snapshot_date,
    uniqExact(expediente) as expedientes,
    uniqExactIf(hutb_num, hutb_num is not null) as licencias_hutb,
    -- plazas solo desde 2020T3 (NULL antes)
    sumOrNull(plazas) as plazas
from {{ ref('int_hut_registry__located') }}
where coalesce(barrio_id, '') != ''
group by barrio_id, snapshot_date
