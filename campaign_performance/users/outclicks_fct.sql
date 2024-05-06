-- -- models/test_write.sql
{{ config(materialized='table') }}

WITH outclicks AS (
    SELECT * FROM {{ source('main','postbacks_outgoing') }}
),
deals AS (
    SELECT * FROM {{ ref('deals_dim') }}
)

select 
    outclicks.id as outclick_id,
    outclicks.timestamp as created_at_cet, 
    outclicks.user_id, 
    outclicks.deal_id,
    outclicks.adclickid as ad_click_id,
    outclicks.money_page_name as moneypage_template_id, 
    outclicks.provider_id as affiliated_account_id,
    --site_id ??
    outclicks.geo as geo_id,
    deals.ga_campaign_id as ga_campaign_id
from outclicks
left join deals
on outclicks.deal_id = deals.id



where timestamp>'2024-04-01'
