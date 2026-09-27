-- raw guarda una copia completa por extracción (_snapshot_date = día de la
-- descarga): staging expone solo la más reciente, si no cada ejecución en un
-- día distinto duplicaría todas las filas.
with source as (
    select * from {{ source('raw', 'bcn_rent') }}
    where _snapshot_date = (
        select max(newest._snapshot_date) from {{ source('raw', 'bcn_rent') }} as newest
    )
)

select
    stat_id,
    -- ids del kickoff: b37xv8wcjh = €/mes, 5ibudgqbrb = €/m²
    case stat_id
        when 'b37xv8wcjh' then 'rent_price_monthly'
        when '5ibudgqbrb' then 'rent_price_per_m2'
        else stat_id
    end as metric_name,
    period_id,
    period_type,
    -- period_id: "t_Trimestre_2024-04-01T00:00:00Z" / "t_Any_2000-01-01T00:00:00Z"
    toDate(
        parseDateTimeBestEffortOrNull(splitByChar('_', coalesce(period_id, ''))[3])
    ) as period_start,
    territory_id,
    territory_type,
    territory_label,
    toFloat64OrNull(value) as rent_value,
    _snapshot_date,
    _loaded_at
from source
