-- -- models/test_write.sql
{{ config(materialized='table') }}

WITH records_gap_campaigns AS (
    SELECT * FROM {{ source('main','records_gap_campaigns') }}
)

select 
    campaign as ga_campaign_id,
    day as date, 
    clicks as clicks, 
    cost as ad_costs, 
    budget as budget
from records_gap_campaigns
where day>'2024-04-01'
