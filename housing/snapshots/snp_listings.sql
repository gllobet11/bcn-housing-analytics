{% snapshot snp_listings %}
    {{
        config(
            target_schema=target.schema,
            unique_key='listing_id',
            strategy='check',
            check_cols=['price', 'room_type', 'availability_365'],
        )
    }}

    -- solo un snapshot trimestral por invocación (el más reciente, o el de
    -- --vars '{snapshot_date: ...}' para reproducir el histórico en orden):
    -- dbt snapshot compara este "estado actual" contra la última fila
    -- registrada por listing_id, y solo puede haber uno por listing.
    select
        snap.listing_id,
        snap.neighbourhood,
        snap.price,
        snap.room_type,
        snap.availability_365,
        snap._snapshot_date
    from {{ ref('stg_airbnb__listings') }} as snap
    where snap._snapshot_date = (
        {% if var('snapshot_date', none) %}
            toDate('{{ var("snapshot_date") }}')
        {% else %}
            select max(newest._snapshot_date)
            from {{ ref('stg_airbnb__listings') }} as newest
        {% endif %}
    )
{% endsnapshot %}
