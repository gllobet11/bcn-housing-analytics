{% snapshot snp_listings %}
    {{
        config(
            target_schema=target.schema,
            unique_key='listing_id',
            strategy='check',
            check_cols=['price', 'room_type', 'availability_365'],
        )
    }}

    -- solo el snapshot trimestral más reciente: dbt snapshot compara este
    -- "estado actual" contra la última fila registrada por listing_id, y solo
    -- puede haber un estado actual por listing en cada invocación.
    select
        state.listing_id,
        state.neighbourhood,
        state.price,
        state.room_type,
        state.availability_365,
        state._snapshot_date
    from {{ ref('stg_airbnb__listings') }} as state
    where state._snapshot_date = (
        select max(recent._snapshot_date)
        from {{ ref('stg_airbnb__listings') }} as recent
    )
{% endsnapshot %}
