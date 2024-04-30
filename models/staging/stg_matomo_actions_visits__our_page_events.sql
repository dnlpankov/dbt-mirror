-- models/campaign_level_data.sql
{{ config(materialized='table') }}

select 
    date(timestamp - interval '2 hours') as date_cet, 
    "left"(matomo_actions.eventname::text, 2) as country_code, 
    CASE
     When right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)='sports' Then 'sports'
     else 'simple'
    END as betting_type,
    lower(sitename) as campaign_name, 
    campaignname as ga_campaign_name, 
    "right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3) as brand_name,
    count(matomo_actions.id) as outclicks,
    count(DISTINCT matomo_visits.visitorid) AS unique_outclicks,
    round(avg(eventvalue), 2) AS avg_list_position,
    string_agg(DISTINCT eventvalue::character varying::text, ';'::text) AS pos_list--,
    --NULL as signups, NULL as cpa_count, NULL as cpa_commissions, NULL as revshare_commissions, NULL as gtee_count,
    -- NULL as gtee_commissions, NULL as avg_deposit_amount
from {{ source('main','matomo_actions') }} matomo_actions
left join {{ source('main','matomo_visits') }} matomo_visits 
on matomo_actions.matomo_visit_id=matomo_visits.id
where 
    matomo_actions.type = 'event' 
    AND matomo_actions.subtitle = 'Category: "OutClicks, Action: "Click on casino banner"'
    --and right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)<>'sports'
    and date(timestamp - interval '2 hours') >'2023-12-31'
group by campaign_name, campaignname, date_cet, brand_name, country_code, betting_type
