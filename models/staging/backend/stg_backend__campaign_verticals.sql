-- models/staging/scraper/stg_scraper__records.sql

{{ config(materialized= 'table' ) }}

with source as (
    select * from {{ source('backend','traffic_types') }}
)

select
    id::integer
    , name
from source
