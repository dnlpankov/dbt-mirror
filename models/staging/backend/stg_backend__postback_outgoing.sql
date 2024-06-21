{{ config(materialized= 'table' ) }}

with main as (
    select
        id as outclick_id
        , timestamp
        , brand_name as brand_id
        , click_id
        , user_id
        , geo as country_code
        , campaign_name as campaign_group_id
        --, campaign_vertical_id --getting from deal [deal characteristics] Peter (traffic_types)
        , adgroup_name as ga_campaign_id  --ga_campaign_name
        --, traffic_source_id --no? 
        , adclickid as ad_click_id
        , money_page_name as moneypage_id --money_page_name? 
        --, affiliate_account_id --in the deals table Peter discussion 
        -- imports_providers.id as affiliate_account_id --provider_id; 

    from postback_outgoing
    where timestamp> '2024-03-31'
)

select * from main