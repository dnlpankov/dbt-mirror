-- models/campaign_level_data.sql
{{ config(materialized='table') }}

(
    select 
        date_cet, 
        country_code, 
        campaign_name, 
        ga_campaign_name, 
        brand_name,
        unique_outclicks,
        betting_type,
        sources.traffic_source,
        NULL as cost
    from {{ref('stg_matomo_actions_visits__our_page_events')}} actions
    left join {{ref('stg_campaign_names_mapping__traffic_sources')}} sources
    on actions.ga_campaign_name=sources.affiliate_campaign_name
    --where  
        --matomo_actions.type = 'event' 
        --AND matomo_actions.subtitle = 'Category: "OutClicks, Action: "Click on casino banner"'
        --and right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)<>'sports'
        --[[ and parse_matomo_timestamp(timestamp) in ( select date_parsed from calendar where {{calendar_date}} ) ]]
        --[[ and "left"(matomo_actions.eventname::text, 2) in ( select distinct geo from campaign_names_mapping WHERE {{country_code_var}} ) ]]
        --[[ and lower(sitename) in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{campaign_name_var}})]]
        --[[ and lower(sitename) in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{traffic_source}})]]
    --group by campaign_name, ga_campaign_name, date_cet, brand_name, country_code, traffic_source
)
/*GAP campaigns aggregated data from records_gap_campaigns table*/
union all
(    select 
        date_cet,
        country_code, 
        campaign_name, 
        ga_campaign_name, 
        betting_type,
        NULL as brand_name, 
        NULL as unique_outclicks,
        traffic_source,
        cost
    from {{ref('stg_records_gam_campaign__campaign_costs')}}
--    group by date_cet, country_code, campaign_name, ga_campaign_name, brand_name, unique_outclicks, betting_type, traffic_source
)

-- SELECT 
--     COALESCE(matomo.date_cet, cost.date_cet) as date_cet,
--     COALESCE(matomo.country_code, cost.country_code) as country_code,
--     COALESCE(matomo.betting_type, cost.betting_type) as betting_type,
--     COALESCE(matomo.campaign_name, cost.campaign_name) as campaign_name,
--     COALESCE(matomo.ga_campaign_name, cost.ga_campaign_name) as ga_campaign_name,
--     matomo.brand_name,
--     matomo.unique_outclicks,
--     cost.cost
-- FROM {{ ref('stg_matomo_actions_visits__our_page_events') }} matomo
-- FULL OUTER JOIN {{ ref('stg_records_gam_campaign__campaign_costs') }} cost
-- on matomo.date_cet = cost.date_cet
--     AND matomo.country_code = cost.country_code
--     and matomo.betting_type=cost.betting_type
--     AND matomo.campaign_name = cost.campaign_name
--     AND matomo.ga_campaign_name = cost.ga_campaign_name

-- where COALESCE(matomo.country_code, cost.country_code)='de'
--     and COALESCE(matomo.date_cet, cost.date_cet)='2024-04-23'
--     and COALESCE(matomo.betting_type, cost.betting_type)='simple'
--     and COALESCE(matomo.brand_name, cost.brand_name)='ninecas'
-- order by campaign_name, ga_campaign_name     