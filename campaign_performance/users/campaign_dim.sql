-- models/test_write.sql
{{ config(materialized='table') }}

WITH records_gap_campaigns AS (
    SELECT * FROM {{ source('main','records_gap_campaigns') }}
)

select 
    id as id
from records_gap_campaigns
where day>'2024-04-01'