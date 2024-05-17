-- models/staging/scraper/stg_scraper__records.sql

with source as (
    select * from {{ ref('stg_scraper__records') }}
)

, transformed as (
    select
        id--grain_id
        , created_at
        , user_id
        , deal_id
        , date_parsed as date_cet
        , click_id
        , geo as country_code
        , registrations as signed_up
        , cpa_count as deposited_first_time
        , gtee_count
        , cpa_commissions as acquisition_commission
        , deposits as acquisition_deposit
        , total_commission
        , gtee_commissions
        , net_revenue
        , revshare_commissions
        , ga_campaign_name
        , campaign_vertical
        , brand_name
        , campaign_name
    from source
    where
        date_parsed > '2024-03-31'
        and deposited_first_time > 0.5
        --and deal_id is null
        --and gtee_commissions > 0 --and cpa_count>0.5 and total_commission>cpa_commissions -- noqa: LT05
    --and user_id='ae4eb2f5ad8ebf29'
)