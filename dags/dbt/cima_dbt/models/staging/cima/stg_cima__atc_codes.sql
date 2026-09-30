with raw_atc_codes as (
    select * from {{ source('cima', 'atc_codes') }}
),

renamed_and_casted as (
    select
        cast(codigo as string) as atc_code,
        cast(nombre as string) as atc_name
    from raw_atc_codes
)

select * from renamed_and_casted
