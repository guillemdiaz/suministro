with raw_laboratorios as (
    select * from {{ source('cima', 'laboratorios') }}
),

renamed_and_casted as (
    select cast(nombre as string) as laboratory_name
    from raw_laboratorios
)

select * from renamed_and_casted
