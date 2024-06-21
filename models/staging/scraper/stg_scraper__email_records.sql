-- models/staging/scraper/stg_scraper__records.sql

{{ config(materialized= 'table' ) }}

with source as (
    select * from {{ source('backend','records') }}
)

select
    "campaign_name"
    , "geo" as country_code
    , "brand_name"
    , "registrations"
    , "cpa_commissions"
    , "cpa_count" as first_time_deposit
    , "registrations" + "cpa_count" as "regs_plus_ftds"
from source
where
    ("campaign_name" = 'email')
    and ("registrations" + "cpa_count" > 0)
