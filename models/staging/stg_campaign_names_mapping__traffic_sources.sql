-- models/campaign_level_data.sql
{{ config(materialized='table') }}

select DISTINCT 
    --lower(tracking_template_campaign_name) as affiliate_campaign_name, 
    campaign_name,
    traffic_source
from {{ source('main','campaign_names_mapping') }}