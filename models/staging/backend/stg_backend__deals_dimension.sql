{{ config(materialized= 'view' ) }}

with source as (
    select * from {{ source('backend','deals') }} limit 1
)
,



-- select
--     *
-- from source

main as (
    select
        id as deal_id
        , brand_name
        , geo as country_code
        , deal_start_date as start_date
        , deal_end_date as end_date
        , deal_cpa as first_time_deposit_commission
        , deal_gtee as guaranteed_commission
        , deal_revshare as revenue_share_commission
        , campaign_name as campaign_group -- campaign_name? 
        , gap_campaign_name as google_ads_campaign_id -- ga_campaign_name? 
        , traffic_types::integer as campaign_vertical_id
        , traffic_sources::integer as traffic_source_id --(FB, Google, etc) tables with names
    from source
)

select * from main
--where deal_id = 2085


-- select betting_type, traffic_sources, count(deal_id)
-- from main
-- group by betting_type, traffic_sources