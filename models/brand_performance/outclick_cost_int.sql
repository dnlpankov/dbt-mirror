-- models/campaign_level_data.sql
{{ config(materialized='table') }}
with main as 
(
    select 
            date(timestamp - interval '2 hours') as date, --matomo update
            "left"(matomo_actions.eventname::text, 2) as country_code, 
            lower(sitename) as campaign_name, 
            campaignname as ga_campaign_name, 
            CASE 
                when right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)<>'sports' then 'casino'
                when right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)='sports' then 'sports'
                else 'other'
            END as campaign_vertical,
            "right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3) as brand_name,
            count(DISTINCT matomo_visits.visitorid) AS unique_outclicks,
            NULL as cost
        from {{ source('main','matomo_actions') }} matomo_actions
        left join {{ source('main','matomo_visits') }} matomo_visits
        on matomo_actions.matomo_visit_id=matomo_visits.id
        where matomo_actions.type = 'event' 
            AND matomo_actions.subtitle = 'Category: "OutClicks, Action: "Click on casino banner"'
            --and right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)<>'sports'
            AND date(timestamp - interval '2 hours')>'2023-12-31' --matomo
        group by campaign_name, campaignname, campaign_vertical, date, brand_name, country_code
        union all
        select 
            day as date, 
            geo as country_code, 
            console_campaign_name as campaign_name, 
            lower(campaign) as ga_campaign_name, 
            CASE 
                when campaign_names_mapping.campaign_vertical='casino' then 'casino'
                when campaign_names_mapping.campaign_vertical='sports' then 'sports'
                else 'other'
            END as campaign_vertical,
            NULL as brand_name, 
            NULL as unique_outclicks, 
            sum(cost) as cost
        from {{ source('main','records_gap_campaigns') }}  records_gap_campaigns
        left join {{ source('main','campaign_names_mapping') }} campaign_names_mapping on campaign_names_mapping.gap_campaign_name=records_gap_campaigns.campaign
        where day >'2023-12-31'
            -- campaign_names_mapping.campaign_vertical='casino'
            -- and day >'2023-12-31' --matomo

        group by day, country_code, campaign_name, ga_campaign_name, campaign_vertical
)

select 
    {{ dbt_utils.generate_surrogate_key(['campaign_name', 'ga_campaign_name', 'campaign_vertical', 'date', 'country_code']) }} as id, 
    *
from main 