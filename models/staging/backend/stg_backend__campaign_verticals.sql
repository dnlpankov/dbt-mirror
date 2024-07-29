-- models/staging/scraper/stg_scraper__records.sql

{{ config(materialized= 'view' ) }}

with source as (
    select * from {{ source('backend','traffic_sources') }} limit 1
)



select
    *
from source
