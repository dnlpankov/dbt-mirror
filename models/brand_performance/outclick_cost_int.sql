-- models/campaign_level_data.sql
{{ config(materialized='table') }}
with main as 
(
    select
        'matomo' as source, --matomo
        {{matomo_timestamp_to_date('timestamp')}} as date, --matomo update
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
        AND {{matomo_timestamp_to_date('timestamp')}}>'2023-01-01' --matomo
    group by campaign_name, campaignname, campaign_vertical, {{matomo_timestamp_to_date('timestamp')}}, brand_name, country_code
    union all
    select
        'records_gap_campaigns' as source, --'records'
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
    where day >'2023-01-01'
        -- campaign_names_mapping.campaign_vertical='casino'
        -- and day >'2023-12-31' --matomo

    group by day, country_code, campaign_name, ga_campaign_name, campaign_vertical
)


select *,
{{ dbt_utils.generate_surrogate_key(['campaign_name', 'ga_campaign_name', 'campaign_vertical', 'date', 'country_code', 'brand_name', 'source']) }} as id

from main


-- Checking for duplicates
-- test as (
--     select 
--     {{ dbt_utils.generate_surrogate_key(['campaign_name', 'ga_campaign_name', 'campaign_vertical', 'date', 'country_code', 'brand_name']) }} as id, 
--     *
-- from main
-- )

-- select * 
-- from test
-- left join (select id, count(*) from test group by id having count(*)>1) as duplicates on test.id=duplicates.id
-- where duplicates.id is not null --and cost is not null and test.id='df85a909516d6442b4f696089262f04a'
