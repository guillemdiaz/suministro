with raw_medicamentos as (
    select
        *,
        _partitiondate as snapshot_date
    from {{ source('cima', 'medicamentos') }}
),

renamed_and_casted as (
    select
        -- Core identifiers
        cast(nregistro as string) as registration_number,

        -- Descriptive information
        nombre as medication_name,
        pactivos as active_ingredients_list,
        labtitular as marketing_authorization_holder,
        labcomercializador as marketing_lab,
        cpresc as prescription_conditions,

        -- Dates (nested inside the 'estado' struct as Unix timestamps)
        timestamp_millis(cast(estado.aut as int64)) as authorization_date,
        timestamp_millis(cast(estado.susp as int64)) as suspension_date,
        timestamp_millis(cast(estado.rev as int64)) as revocation_date,

        -- Flags & Indicators (Booleans)
        cast(comerc as boolean) as is_commercialized,
        cast(receta as boolean) as requires_prescription,
        cast(generico as boolean) as is_generic,
        cast(conduc as boolean) as affects_driving,
        cast(triangulo as boolean) as has_black_triangle,
        cast(huerfano as boolean) as is_orphan_drug,
        cast(biosimilar as boolean) as is_biosimilar,
        cast(ema as boolean) as is_ema_registered,
        cast(notas as boolean) as has_safety_notes,
        cast(materialesinf as boolean) as has_safety_materials,
        cast(psum as boolean) as has_active_shortage,

        -- Therapeutic category (kept as an array for unnesting downstream)
        atcs as atc_codes,

        -- Partition tracking
        snapshot_date

    from raw_medicamentos
)

select
    -- Surrogate primary key: one row per registration_number per snapshot_date
    {{ dbt_utils.generate_surrogate_key([
        'registration_number',
        'snapshot_date'
    ]) }} as medication_snapshot_key,
    *
from renamed_and_casted
