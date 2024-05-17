with main as (
    select id
        , brand_name
        , geo as country_code
        , deal_start_date as start_date
        , deal_end_date as end_date
        , deal_cpa as first_time_deposit_commission
        , deal_gtee as guaranteed_commission
        , deal_revshare as revenue_share_commission
        , campaign_name as campaign_group
        , gap_campaign_name as google_ads_campaign_id
        -- , traffic_types as betting_type
        -- , traffic_sources
    from deals
)

select * from main
where id=2085
-- select betting_type, traffic_sources, count(id)
-- from main
-- group by betting_type, traffic_sources