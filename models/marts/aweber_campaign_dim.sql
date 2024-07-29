{{ config(materialized= 'table' ) }}

with source as (
    select * from {{ source('backend_danila','stg_aweber__campaign_characteristics') }}
)
, main as (
    select
        country_code
        , brand_name
        , campaign_type
        , campaign_id as aweber_campaign_id
        , list_id
        , sent_at_cet::timestamp as sent_at_cet
        , is_welcome_campaign

    from source
    where sent_at_cet is not null
    and list_id != 6784256 --6405745
)

select * from main
