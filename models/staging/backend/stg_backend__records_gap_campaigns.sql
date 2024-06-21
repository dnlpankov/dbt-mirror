{{ config(materialized= 'table' ) }}

with source as (
    select * from {{ source('backend','records_gap_campaigns') }}
)


with main as (
    select
        id as gap_record_id
        , day as date
        , campaign_id as ga_campaign_id
        , clicks as n_clicks
        , impr as n_impressions 
        , cost
        , budget
        , adclickid as ad_click_id

    from deals
)

select * from main