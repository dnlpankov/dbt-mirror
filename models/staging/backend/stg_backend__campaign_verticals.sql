-- models/staging/scraper/stg_scraper__records.sql

{{ config(materialized= 'table' ) }}

with source as (
    select * from {{ source('backend','stg_backend__campaign_verticals') }}
)



select
    id::integer
    , name
from source
