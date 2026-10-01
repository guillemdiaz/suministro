with shortages as (
    select * from {{ ref('fct_shortages_daily') }}
),

by_lab as (
    select
        -- Fallback for CIMA API fetch timeouts missing the presentation data
        coalesce(marketing_authorization_holder, 'Unknown (API fetch failed)')
            as marketing_authorization_holder,
        count(distinct national_drug_code) as distinct_products_affected,
        count(*) as total_shortage_days,
        min(shortage_start_date) as first_shortage_seen,
        -- Treat ongoing shortages (null end date) as continuing up to the
        -- present moment
        max(coalesce(shortage_end_date, current_timestamp()))
            as last_shortage_seen
    from shortages
    group by marketing_authorization_holder
)

select * from by_lab
