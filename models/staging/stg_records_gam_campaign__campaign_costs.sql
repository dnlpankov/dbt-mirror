-- models/campaign_level_data.sql
{{ config(materialized='table') }}

--with tmp as (
select 
    day as date_cet, 
    geo as country_code, 
    -- CASE
    --  When campaign_names_mapping.campaign_vertical='casino' Then 'simple'
    --  When campaign_names_mapping.campaign_vertical='sports' THEN 'sports'
    --  else 'other'
    -- END as campaign_vertical,
    'simple' as campaign_vertical,
    console_campaign_name as campaign_name, 
    lower(campaign) as ga_campaign_name,
    campaign_names_mapping.traffic_source,
    sum(cost) as cost
from {{ source('main','records_gap_campaigns') }}  records_gap_campaigns
-- left join {{ source('main','campaign_names_mapping') }} campaign_names_mapping 
-- on campaign_names_mapping.gap_campaign_name=records_gap_campaigns.campaign
left join {{ source('main','campaign_names_mapping') }} campaign_names_mapping 
on campaign_names_mapping.gap_campaign_name=records_gap_campaigns.campaign
where 
    day >'2023-12-31' --matomo
    --and campaign_names_mapping.campaign_vertical is not NULL -- exclude all the missing for the campaign vertical data
    and campaign_names_mapping.campaign_vertical ='casino'
group by day, country_code, campaign_name, ga_campaign_name, campaign_vertical, campaign_names_mapping.traffic_source, campaign_names_mapping.campaign_vertical

-- select betting_type, 
-- count(*) 
-- from tmp group by betting_type
-- select camp
-- from tmp 
-- where betting_type='other' and camp is not NULL