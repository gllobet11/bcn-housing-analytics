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
        snap.listing_id,
        snap.neighbourhood,
        snap.price,
        snap.room_type,
        snap.availability_365,
        snap._snapshot_date
    from {{ ref('stg_airbnb__listings') }} as snap
    where snap._snapshot_date = (
        select max(newest._snapshot_date)
        from {{ ref('stg_airbnb__listings') }} as newest
    )
{% endsnapshot %}
