with shortages as (
    select * from {{ ref('stg_cima__psuministro') }}
),

presentations as (
    select * from {{ ref('stg_cima__presentaciones') }}
),

medications as (
    select * from {{ ref('stg_cima__medicamentos') }}
),

enriched as (
    select
        -- Core shortage event facts
        shortages.national_drug_code,
        shortages.snapshot_date,
        shortages.presentation_name,
        shortages.shortage_type_id,
        shortages.shortage_start_date,
        shortages.shortage_end_date,
        shortages.is_active,
        shortages.observation,

        -- Parent presentation characteristics
        presentations.registration_number,
        presentations.marketing_authorization_holder,
        presentations.is_commercialized,

        -- Parent medication characteristics
        medications.atc_codes

    from shortages
    left join presentations
        on
            shortages.national_drug_code = presentations.national_drug_code
            and shortages.snapshot_date = presentations.snapshot_date
    left join medications
        on
            presentations.registration_number = medications.registration_number
            and shortages.snapshot_date = medications.snapshot_date
)

select * from enriched
