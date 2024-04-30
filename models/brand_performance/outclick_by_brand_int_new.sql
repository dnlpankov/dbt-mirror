-- models/campaign_level_data.sql
{{ config(materialized='table') }}

SELECT 
    COALESCE(matomo.date_cet, records.date_cet) as date_cet, 
    COALESCE(matomo.country_code, records.country_code) as country_code, 
    COALESCE(matomo.betting_type, records.betting_type) as betting_type,
    COALESCE(matomo.campaign_name, records.campaign_name) as campaign_name, 
    COALESCE(matomo.ga_campaign_name, records.ga_campaign_name) as ga_campaign_name, 
    COALESCE(matomo.brand_name, records.brand_name) as brand_name,
    matomo.outclicks,
    matomo.unique_outclicks,
    matomo.avg_list_position,
    matomo.pos_list,
    records.signups, 
    records.cpa_count, 
    records.cpa_commissions, 
    records.revshare_commissions, 
    records.gtee_count,
    records.gtee_commissions, 
    records.avg_deposit_amount
FROM {{ ref('stg_matomo_actions_visits__our_page_events') }} matomo
FULL OUTER JOIN {{ ref('stg_record__casino_events') }} records ON 
    matomo.date_cet = records.date_cet 
    AND matomo.country_code = records.country_code 
    AND matomo.betting_type=records.betting_type
    and matomo.campaign_name = records.campaign_name 
    AND matomo.ga_campaign_name = records.ga_campaign_name 
    AND matomo.brand_name = records.brand_name
-- where COALESCE(matomo.country_code, records.country_code)='de'
-- and COALESCE(matomo.date_cet, records.date_cet)='2024-04-23'
-- and COALESCE(matomo.betting_type, records.betting_type)='simple'
-- and COALESCE(matomo.brand_name, records.brand_name)='ninecas'
-- order by campaign_name, ga_campaign_name 