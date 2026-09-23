{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['date_day']
) }}

-- grain: un día. Rango 2014-01-01..2027-12-31: cubre el histórico de alquiler
-- de Barcelona Dades (desde 2014T1) y los snapshots de Inside Airbnb con margen.
with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="toDate('2014-01-01')",
        end_date="toDate('2027-12-31')"
    ) }}
)

select
    date_day,
    toYear(date_day) as date_year,
    toQuarter(date_day) as quarter_of_year,
    toStartOfQuarter(date_day) as quarter_start_date,
    toMonth(date_day) as date_month,
    toDayOfWeek(date_day) as day_of_week
from spine
