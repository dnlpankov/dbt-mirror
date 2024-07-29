-- models/campaign_level_data.sql
{{ config(materialized='table') }}
with stg_records as (
    select 
    --'records' as source,
    date_parsed as date, 
    geo as country_code, 
    CASE  
        WHEN campaign_name::text = 'jpluckyslotsonline'::text THEN 'luckyslotsonline'::character varying
        WHEN campaign_name::text = 'ficashstormslots'::text THEN 'cashstormslots'::character varying
        WHEN campaign_name::text = 'goldenlion'::text THEN 'goldenliongames'::character varying
        ELSE campaign_name
    END as campaign_name, 
    lower(adgroup_name) as ga_campaign_name, 
    CASE 
        when right(brand_name,6)<>'sports' then 'casino'
        when right(brand_name,6)='sports' then 'sports'
        else 'other'
    END as campaign_vertical,
    CASE
        WHEN campaign_name::text = 'email' THEN brand_name || ' email'
        WHEN campaign_name::text = 'PA' THEN brand_name || ' PA'
        ELSE brand_name
    END as brand_name, 
    NULL as outclicks, 
    NULL as unique_outclicks, 
    NULL as avg_list_position, 
    NULL as pos_list,
    registrations, --sum(registrations) as signups, 
    cpa_count, --sum(cpa_count) as cpa_count, 
    cpa_commissions, --sum(cpa_commissions) AS cpa_commissions,
    total_commission, -- coalesce(sum(total_commission-cpa_commissions) filter(where total_commission-cpa_commissions<>0 and gtee_count=0),0) AS revshare_commissions,
    gtee_count,
    gtee_commissions,
    deposits --sum(gtee_count) as gtee_count, sum(gtee_commissions) as gtee_commissions,
    --avg(deposits) FILTER(where cpa_count>0) AS avg_deposit_amount
from {{ source('main','records') }} records
where date_parsed > '2023-01-01'
),

 main as (
    select 
        --date(timestamp - interval '2 hours') as date, 
        'matomo' as source,
        {{matomo_timestamp_to_date('timestamp')}} as date,
        "left"(matomo_actions.eventname::text, 2) as country_code, 
        lower(sitename) as campaign_name, 
        campaignname as ga_campaign_name,
        CASE 
            when right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)<>'sports' then 'casino'
            when right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)='sports' then 'sports'
            else 'other'
        END as campaign_vertical, 
        "right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3) as brand_name,
        count(matomo_actions.id) as outclicks,
        count(DISTINCT matomo_visits.visitorid) AS unique_outclicks,
        round(avg(eventvalue), 2) AS avg_list_position,
        string_agg(DISTINCT eventvalue::character varying::text, ';'::text) AS pos_list,
        NULL as signups, NULL as cpa_count, NULL as cpa_commissions, NULL as revshare_commissions, NULL as gtee_count,
        NULL as gtee_commissions, NULL as avg_deposit_amount
    from {{ source('main','matomo_actions') }} matomo_actions
    left join {{ source('main','matomo_visits') }} matomo_visits 
    on matomo_actions.matomo_visit_id=matomo_visits.id
    where 
        matomo_actions.type = 'event' 
        AND matomo_actions.subtitle = 'Category: "OutClicks, Action: "Click on casino banner"'
        --and right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)<>'sports'
        --and date(timestamp - interval '2 hours') >'2023-01-01'
        and {{matomo_timestamp_to_date('timestamp')}} >'2023-01-01'
    --[[ and parse_matomo_timestamp(timestamp) in ( select date_parsed from calendar where {{calendar_date}} ) ]]
    -- [[ and "left"(matomo_actions.eventname::text, 2) in ( select distinct geo from campaign_names_mapping WHERE {{country_code_var}} ) ]]
    -- [[ and lower(sitename) in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{campaign_name_var}})]]
    -- [[ and lower(sitename) in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{traffic_source}})]]
    -- [[ and "right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3) in ( select distinct brand_name from records WHERE {{brand_name_var}} ) ]]
    group by source, campaign_name, campaignname, campaign_vertical, {{matomo_timestamp_to_date('timestamp')}}, brand_name, country_code
    /*affiliate records aggregated data from records table*/
    union all
    select 
        'records' as source,
        date, 
        country_code, 
        campaign_name, 
	    ga_campaign_name, 
        campaign_vertical, 
        brand_name,
        NULL as outclicks, 
        NULL as unique_outclicks, 
        NULL as avg_list_position, 
        NULL as pos_list,
        sum(registrations) as signups, 
        sum(cpa_count) as cpa_count, 
        sum(cpa_commissions) AS cpa_commissions,
        coalesce(sum(total_commission-cpa_commissions) filter(where total_commission-cpa_commissions<>0 and gtee_count=0),0) AS revshare_commissions,
        sum(gtee_count) as gtee_count, sum(gtee_commissions) as gtee_commissions,
        avg(deposits) FILTER(where cpa_count>0) AS avg_deposit_amount
    from stg_records 
        -- right(brand_name,6)<>'sports'
        -- and date_parsed > '2023-12-31'
    --[[ and date_parsed in ( select date_parsed from calendar where {{calendar_date}} ) ]]
    -- [[ and geo in (select distinct geo from campaign_names_mapping WHERE {{country_code_var}}) ]]
    -- [[ and campaign_name in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{campaign_name_var}}) ]]
    -- [[ and campaign_name in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{traffic_source}}) ]]
    -- [[ and {{brand_name_var}} ]]
    group by source, date, country_code, campaign_name, ga_campaign_name, campaign_vertical, brand_name
)

select *,
{{ dbt_utils.generate_surrogate_key(['campaign_name', 'ga_campaign_name', 'campaign_vertical', 'date', 'country_code', 'brand_name', 'source']) }} as id
from main


