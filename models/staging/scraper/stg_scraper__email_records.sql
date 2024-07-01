-- models/staging/scraper/stg_scraper__records.sql

{{ config(materialized= 'table' ) }}

with source as (
    select * from {{ source('backend','records') }}
)

-- select
--     "campaign_name"
--     , "geo" as country_code
--     , "brand_name"
--     , timestamp_parsed
--     , date_parsed
--     , "registrations"
--     , "cpa_commissions"
--     , "cpa_count" as first_time_deposit
--     , "registrations" + "cpa_count" as "regs_plus_ftds"
-- from source
-- where
--     ("campaign_name" = 'email')
--     and cpa_count>0 and date_parsed>'2024-05-31'
    -- and ("registrations" + "cpa_count" > 0) # condition for the speed

select
    *
from source
where
    ("campaign_name" = 'email')
    and cpa_count>0 and date_parsed>'2024-05-31'