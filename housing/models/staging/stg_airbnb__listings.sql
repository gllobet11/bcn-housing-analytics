with source as (
    select * from {{ source('raw', 'airbnb_listings') }}
)

select
    toUInt64(id) as listing_id,
    toUInt64OrNull(host_id) as host_id,
    name,
    toDateOrNull(host_since) as host_since,
    neighbourhood_cleansed as neighbourhood,
    neighbourhood_group_cleansed as neighbourhood_group,
    toFloat64OrNull(latitude) as latitude,
    toFloat64OrNull(longitude) as longitude,
    property_type,
    room_type,
    toInt32OrNull(accommodates) as accommodates,
    toInt32OrNull(bedrooms) as bedrooms,
    toInt32OrNull(beds) as beds,
    -- "$1,234.00" -> 1234.00
    toDecimal64OrNull(replaceRegexpAll(price, '[^0-9.]', ''), 2) as price,
    toInt32OrNull(minimum_nights) as minimum_nights,
    toInt32OrNull(maximum_nights) as maximum_nights,
    toInt32OrNull(availability_30) as availability_30,
    toInt32OrNull(availability_60) as availability_60,
    toInt32OrNull(availability_90) as availability_90,
    toInt32OrNull(availability_365) as availability_365,
    toInt32OrNull(number_of_reviews) as number_of_reviews,
    toDateOrNull(first_review) as first_review,
    toDateOrNull(last_review) as last_review,
    toFloat64OrNull(review_scores_rating) as review_scores_rating,
    instant_bookable = 't' as instant_bookable,
    license,
    _snapshot_date,
    _loaded_at
from source
