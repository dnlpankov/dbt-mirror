-- models/staging/scraper/stg_scraper__records.sql

with source as (
    select * from {{ ref('stg_scraper__records') }}
)

, transformed as (
    select
        'records' as source
        , date_eet
        , country_code
        , campaign_name
        , ga_campaign_name
        , campaign_vertical
        , brand_name
        , NULL as outclicks
        , NULL as unique_outclicks
        , NULL as avg_list_position
        , NULL as pos_list
        , sum(signed_up) as signups
        , sum(deposited_first_time) as cpa_count
        , sum(acquisition_commission) as cpa_commissions
        , coalesce(
            sum(total_commission - acquisition_commission) filter
            (
                where total_commission - acquisition_commission <> 0
                and gtee_count = 0
            ), 0
        ) as revshare_commissions
        , sum(gtee_count) as gtee_count
        , sum(gtee_commissions) as gtee_commissions
        , avg(deposited_amount) filter
        (where deposited_first_time > 0) as avg_deposit_amount
    from source
    where
        deposited_first_time > 0.5
        -- and date_cet > '2024-03-31'
        --and deal_id is null
        --and gtee_commissions > 0 --and cpa_count>0.5 and total_commission>cpa_commissions -- noqa: LT05
    --and user_id='ae4eb2f5ad8ebf29'
    group by
        source, date_eet, country_code, campaign_name
        , ga_campaign_name, campaign_vertical, brand_name
)


select * from transformed