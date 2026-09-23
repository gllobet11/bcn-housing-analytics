{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['property_type_id']
) }}

-- grain: combinación única de property_type x room_type (~30-40 filas).
-- cityHash64 como surrogate key determinista: no cambia entre full-refresh, a
-- diferencia de un row_number().
select
    -- coalesce: cityHash64 sobre argumentos Nullable produce un id Nullable,
    -- que ClickHouse no admite en la sorting key.
    cityHash64(coalesce(property_type, ''), coalesce(room_type, '')) as property_type_id,
    property_type,
    room_type
from (
    select distinct
        property_type,
        room_type
    from {{ ref('stg_airbnb__listings') }}
)
