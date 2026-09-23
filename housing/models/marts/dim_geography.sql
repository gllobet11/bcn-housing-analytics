{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['barrio_id']
) }}

-- grain: un barrio (73 filas). order_by barrio_id porque es la clave de join de todos los fact.
select
    barrio_id,
    barrio_label,
    airbnb_neighbourhood,
    distrito_id,
    distrito_label,
    'Barcelona' as ciudad
from {{ ref('seed_barrio_mapping') }}
