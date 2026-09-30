with raw_psuministro as (
    select distinct
        *,
        _partitiondate as snapshot_date
    from {{ source('cima', 'psuministro') }}
),

renamed_and_casted as (
    select
        -- Primary Keys & Identifiers
        cast(cn as string) as national_drug_code,
        cast(nombre as string) as presentation_name,

        -- Categorization
        cast(tipoproblemasuministro as int64) as shortage_type_id,

        -- Dates (converted from Unix milliseconds to BigQuery timestamps)
        timestamp_millis(cast(fini as int64)) as shortage_start_date,
        timestamp_millis(cast(ffin as int64)) as shortage_end_date,

        -- Flags & Text
        cast(activo as boolean) as is_active,
        cast(observ as string) as observation,

        -- Partition tracking
        snapshot_date

    from raw_psuministro
)

select
    -- Surrogate primary key: national_drug_code + snapshot_date
    {{
        dbt_utils.generate_surrogate_key([
            'national_drug_code',
            'snapshot_date'
        ])
    }} as shortage_snapshot_key,
    *
from renamed_and_casted
