{{ config(materialized= 'table' ) }}


with source as (
    select * from {{ source('backend_danila','stg_aweber__campaign_characteristics') }}
)


with main as (
    select
        country_code
        , brand_name
        , campaign_type
        , aweber_campaign_id
        , sent_at_cet

    from source
)

select * from main
