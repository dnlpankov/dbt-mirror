-- models/campaign_level_data.sql
{{ config(materialized='table') }}


SELECT 
    COALESCE(matomo.date_cet, cost.date_cet) as date_cet,
    COALESCE(matomo.country_code, cost.country_code) as country_code,
    COALESCE(matomo.betting_type, cost.betting_type) as betting_type,
    COALESCE(matomo.campaign_name, cost.campaign_name) as campaign_name,
    COALESCE(matomo.ga_campaign_name, cost.ga_campaign_name) as ga_campaign_name,
    matomo.brand_name,
    matomo.unique_outclicks,
    cost.cost
FROM {{ ref('stg_matomo_actions_visits__our_page_events') }} matomo
FULL OUTER JOIN {{ ref('stg_records_gam_campaign__campaign_costs') }} cost
on matomo.date_cet = cost.date_cet
    AND matomo.country_code = cost.country_code
    and matomo.betting_type=cost.betting_type
    AND matomo.campaign_name = cost.campaign_name
    AND matomo.ga_campaign_name = cost.ga_campaign_name

-- where COALESCE(matomo.country_code, cost.country_code)='de'
--     and COALESCE(matomo.date_cet, cost.date_cet)='2024-04-23'
--     and COALESCE(matomo.betting_type, cost.betting_type)='simple'
--     and COALESCE(matomo.brand_name, cost.brand_name)='ninecas'
-- order by campaign_name, ga_campaign_name     