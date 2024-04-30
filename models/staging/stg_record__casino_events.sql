-- models/campaign_level_data.sql
{{ config(materialized='table') }}

select 
    date_parsed as date_cet, 
    geo as country_code,
    CASE
     When right(brand_name,6)='sports' Then 'sports'
     else 'simple'
    END as betting_type,
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
    NULL as outclicks, 
    NULL as unique_outclicks, 
    NULL as avg_list_position, 
    NULL as pos_list,
    sum(registrations) as signups, sum(cpa_count) as cpa_count, sum(cpa_commissions) AS cpa_commissions,
    coalesce(sum(total_commission-cpa_commissions) filter(where total_commission-cpa_commissions<>0 and gtee_count=0),0) AS revshare_commissions,
    sum(gtee_count) as gtee_count, sum(gtee_commissions) as gtee_commissions,
    avg(deposits) FILTER(where cpa_count>0) AS avg_deposit_amount
from {{ source('main','records') }} records
where date_parsed > '2023-12-31'
group by date_parsed, country_code, campaign_name, ga_campaign_name, brand_name

