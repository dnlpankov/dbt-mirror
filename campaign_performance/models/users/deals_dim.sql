-- models/test_write.sql
{{ config(materialized='table') }}

WITH deals AS (
    SELECT * FROM {{ source('main','deals') }}
)

select 
    id as id,
    geo as geo_id,
    created_at as created_at_cet, 
    deal_start_date as started_at, 
    deal_end_date as ended_at,
    deal_cpa as cpa, 
    deal_gtee as deal_guarantee, 
    deal_revshare as deal_revenue_share,
    --deal_guarantee_started_at, 
    --deal_guarantee_ended_at, 
    --campaign_group,
    gap_campaign_name as ga_campaign_id 
    --vertical, 
    --traffic_source
from deals
where created_at>'2024-04-01'
