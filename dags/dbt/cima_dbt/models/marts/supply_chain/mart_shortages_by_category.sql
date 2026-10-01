with shortages as (
    select * from {{ ref('fct_shortages_daily') }}
),

unnested as (
    select
        shortages.national_drug_code,
        shortages.snapshot_date,
        atc.codigo as atc_code,
        atc.nombre as atc_name,
        atc.nivel as atc_level
    from shortages, unnest(atc_codes) as atc
    -- Keep only Level 3 so shortages aren't counted multiple times
    where atc.nivel = 3
),

by_category as (
    select
        atc_code,
        atc_name,
        count(distinct national_drug_code) as distinct_products_affected,
        count(*) as total_shortage_days
    from unnested
    group by atc_code, atc_name
)

select * from by_category
