with source as (
    select * from {{ source('raw', 'airbnb_calendar') }}
)

select
    toUInt64(listing_id) as listing_id,
    toDate(date) as calendar_date,
    available = 't' as is_available,
    toInt32OrNull(minimum_nights) as minimum_nights,
    toInt32OrNull(maximum_nights) as maximum_nights,
    _snapshot_date,
    _loaded_at
from source
