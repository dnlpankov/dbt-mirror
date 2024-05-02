-- models/campaign_level_data.sql
{{ config(materialized='table') }}

(
    select 
        actions.date_cet, 
        country_code, 
        campaign_name, 
        ga_campaign_name,
        traffic_source, 
        brand_name,
        betting_type,
        outclicks,
        unique_outclicks,
        avg_list_position,
        pos_list,
        NULL as signups, 
        NULL as cpa_count, 
        NULL AS cpa_commissions,
        NULL AS revshare_commissions,
        NULL as gtee_count, 
        NULL as gtee_commissions,
        NULL AS avg_deposit_amount
    from {{ref('stg_matomo_actions_visits__our_page_events')}} actions
    left join {{ref('stg_campaign_names_mapping__traffic_sources')}} sources
    on actions.ga_campaign_name=sources.affiliate_campaign_name
)
union all
(
select 
    date_cet, 
    country_code, 
    campaign_name, 
    ga_campaign_name, 
    traffic_source,
    brand_name,
    betting_type, 
    NULL as outclicks, 
    NULL as unique_outclicks, 
    NULL as avg_list_position, 
    NULL as pos_list,
    signups, 
    cpa_count, 
    cpa_commissions,
    revshare_commissions,
    gtee_count, 
    gtee_commissions,
    avg_deposit_amount
from {{ ref('stg_record__casino_events') }} records
-- where right(brand_name,6)<>'sports'
--     and date_parsed > '2023-12-31'
--[[ and date_parsed in ( select date_parsed from calendar where {{calendar_date}} ) ]]
-- [[ and geo in (select distinct geo from campaign_names_mapping WHERE {{country_code_var}}) ]]
-- [[ and campaign_name in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{campaign_name_var}}) ]]
-- [[ and campaign_name in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{traffic_source}}) ]]
-- [[ and {{brand_name_var}} ]]
--group by date_parsed, country_code, campaign_name, ga_campaign_name, brand_name
)