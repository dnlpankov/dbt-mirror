-- models/campaign_level_data.sql
{{ config(materialized='table') }}

WITH outclick_cost AS ( 
select 
sum(d.cost)/sum(d.unique_outclicks) as unique_outclick_cost
from (
/*outclicks aggregated data from matomo tables*/
    select 
        date(timestamp - interval '2 hours') as date, 
        "left"(matomo_actions.eventname::text, 2) as country_code, 
        lower(sitename) as campaign_name, 
        campaignname as ga_campaign_name, 
        "right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3) as brand_name,
        count(DISTINCT matomo_visits.visitorid) AS unique_outclicks,
        NULL as cost
    from {{ source('main','matomo_actions') }} matomo_actions
    left join {{ source('main','matomo_visits') }} matomo_visits
    on matomo_actions.matomo_visit_id=matomo_visits.id
    where matomo_actions.type = 'event' 
        AND matomo_actions.subtitle = 'Category: "OutClicks, Action: "Click on casino banner"'
        and right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)<>'sports'
        AND date(timestamp - interval '2 hours')>'2024-02-16'
    group by campaign_name, campaignname, date, brand_name, country_code
    union all
    select 
        day as date, 
        geo as country_code, 
        console_campaign_name as campaign_name, 
        campaign as ga_campaign_name, 
        NULL as brand_name, NULL as unique_outclicks, 
        sum(cost) as cost
    from {{ source('main','records_gap_campaigns') }}  records_gap_campaigns
    left join {{ source('main','campaign_names_mapping') }} campaign_names_mapping on campaign_names_mapping.gap_campaign_name=records_gap_campaigns.campaign
    where 
        campaign_names_mapping.campaign_vertical='casino'
        and day >'2024-02-16'
    group by day, country_code, campaign_name, ga_campaign_name
) d
)

select 
    d.country_code,
    d.brand_name, 
    'https://clickstorm.cashstormcreative.ee/dashboard/53-brand-performance-daily-details?date=past20days&country_code=' || d.country_code || '&brand=' || d.brand_name || '' as Details,
    coalesce(sum(d.outclicks),0) as outclicks, 
    sum(d.unique_outclicks) as unique_outclicks, 
    sum(d.signups) as signups, 
    sum(d.cpa_count) as FTDs, 
    sum(d.gtee_commissions) as gtee_commissions, 
    avg(d.avg_deposit_amount) as avg_deposit_amount, 
    avg(d.avg_list_position) as avg_position,
    (sum(d.signups)/NULLIF(sum(d.unique_outclicks),0)*100)  as signup_rate,
    (sum(d.cpa_count)/NULLIF(sum(d.unique_outclicks),0)*100) as conversion_rate,
    CASE 
        WHEN sum(d.gtee_count)<>0 or sum(d.revshare_commissions)<>0 THEN (sum(d.cpa_commissions)+sum(d.gtee_commissions)+sum(d.revshare_commissions))/sum(d.unique_outclicks) 
        ELSE (sum(d.cpa_commissions)/NULLIF(sum(unique_outclicks),0))
    END as EPC,

    CASE 
        WHEN sum(d.gtee_count)<>0 or sum(d.revshare_commissions)<>0 
            THEN (((sum(d.cpa_commissions)+sum(d.gtee_commissions)+sum(d.revshare_commissions))/sum(d.unique_outclicks))*100/NULLIF((select unique_outclick_cost from outclick_cost),0))-100
        ELSE ((sum(d.cpa_commissions)/NULLIF(sum(unique_outclicks),0))*100/NULLIF((select unique_outclick_cost from outclick_cost),0))-100
    END as ROI,

    CASE 
        WHEN sum(d.gtee_count)<>0 or sum(d.revshare_commissions)<>0 THEN (sum(d.cpa_commissions)/NULLIF(sum(unique_outclicks),0)) 
        ELSE NULL
    END as EPC_excl_gtee_rs,
    (sum(d.cpa_commissions)/NULLIF(sum(d.cpa_count),0)) as avg_commission,
    CASE 
        WHEN sum(d.gtee_commissions)>0 THEN ((sum(d.cpa_commissions)+sum(d.gtee_commissions))/NULLIF(sum(d.cpa_count),0))   
        ELSE (sum(d.cpa_commissions)/NULLIF(sum(d.cpa_count),0))
    END as avg_commission_incl_gtee,
    nullif(sum(d.revshare_commissions),0) as revshare_commissions
from (
/*outclicks aggregated data from matomo tables*/
    select date(timestamp - interval '2 hours') as date, 
    "left"(matomo_actions.eventname::text, 2) as country_code, 
    lower(sitename) as campaign_name, 
    campaignname as ga_campaign_name, 
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
        and right("right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3),6)<>'sports'
        and date(timestamp - interval '2 hours') >'2024-02-16'
    --[[ and parse_matomo_timestamp(timestamp) in ( select date_parsed from calendar where {{calendar_date}} ) ]]
    -- [[ and "left"(matomo_actions.eventname::text, 2) in ( select distinct geo from campaign_names_mapping WHERE {{country_code_var}} ) ]]
    -- [[ and lower(sitename) in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{campaign_name_var}})]]
    -- [[ and lower(sitename) in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{traffic_source}})]]
    -- [[ and "right"(matomo_actions.eventname::text, length(matomo_actions.eventname::text) - 3) in ( select distinct brand_name from records WHERE {{brand_name_var}} ) ]]
    group by campaign_name, campaignname, date, brand_name, country_code
/*affiliate records aggregated data from records table*/
    union all
    select 
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
            WHEN campaign_name::text = 'email' THEN brand_name || ' email'
            WHEN campaign_name::text = 'PA' THEN brand_name || ' PA'
            ELSE brand_name
        END as brand_name, 
        NULL as outclicks, NULL as unique_outclicks, NULL as avg_list_position, NULL as pos_list,
        sum(registrations) as signups, sum(cpa_count) as cpa_count, sum(cpa_commissions) AS cpa_commissions,
        coalesce(sum(total_commission-cpa_commissions) filter(where total_commission-cpa_commissions<>0 and gtee_count=0),0) AS revshare_commissions,
        sum(gtee_count) as gtee_count, sum(gtee_commissions) as gtee_commissions,
        avg(deposits) FILTER(where cpa_count>0) AS avg_deposit_amount
    from {{ source('main','records') }} records
    where right(brand_name,6)<>'sports'
    --[[ and date_parsed in ( select date_parsed from calendar where {{calendar_date}} ) ]]
    -- [[ and geo in (select distinct geo from campaign_names_mapping WHERE {{country_code_var}}) ]]
    -- [[ and campaign_name in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{campaign_name_var}}) ]]
    -- [[ and campaign_name in ( select distinct console_campaign_name from campaign_names_mapping WHERE {{traffic_source}}) ]]
    -- [[ and {{brand_name_var}} ]]
    group by date_parsed, country_code, campaign_name, ga_campaign_name, brand_name
) d
group by d.country_code, d.brand_name
having sum(d.outclicks)>0 or sum(d.signups)>0  or sum(d.cpa_count)>0 or sum(d.gtee_count)>0 or sum(d.revshare_commissions)<>0
order by EPC desc NULLS last, FTDs desc NULLS last, unique_outclicks desc NULLS last, d.country_code