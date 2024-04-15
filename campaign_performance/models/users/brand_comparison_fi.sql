-- models/campaign_level_data.sql
{{ config(materialized='table') }}

WITH agg_outclicks AS (
    -- Assuming `outclicks_fct` needs to join with `deals_dim` to get `ga_campaign_id`
    SELECT
        date(created_at_cet) as date,
        ga_campaign_id,
        count(*) as total_outclicks
    FROM {{ ref('outclicks_fct') }}
    GROUP BY 1, 2
),

combined_campaign_data AS (
    -- Then, merge this data with the daily_campaign_fct
    SELECT
        co.date,
        co.ga_campaign_id,
        co.total_outclicks,
        dc.clicks,
        dc.ad_costs,
        dc.budget
    FROM agg_outclicks co
    LEFT JOIN {{ ref('daily_campaign_fct') }} dc 
    ON co.ga_campaign_id = dc.ga_campaign_id 
        AND co.date = dc.date
)

SELECT
    date,
    ga_campaign_id,
    total_outclicks,
    clicks,
    ad_costs,
    budget
FROM combined_campaign_data
ORDER BY date, ga_campaign_id
