-- Falla si más del 1% de los anuncios (último snapshot) no tiene barrio mapeado al seed.
with latest_snapshot as (
    select max(_snapshot_date) as d from {{ ref('stg_airbnb__listings') }}
),

listings as (
    select l.neighbourhood
    from {{ ref('stg_airbnb__listings') }} as l
    inner join latest_snapshot as s on l._snapshot_date = s.d
)

select
    countIf(
        neighbourhood not in (
            select mapping.airbnb_neighbourhood
            from {{ ref('seed_barrio_mapping') }} as mapping
        )
    ) as unmapped_count,
    count() as total_count,
    unmapped_count / count()::Float64 as unmapped_pct
from listings
having unmapped_pct > 0.01
