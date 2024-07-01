{{ config(materialized= 'table' ) }}


with chars_source as (
    select * from {{ source('backend_danila','stg_aweber__campaign_characteristics') }}
)
, datas_source as (
    select * from {{ source('backend_danila','stg_aweber__campaign_data') }}
)


, main as (
    select
        campaign_id as aweber_campaign_id
        , account_id
        , cs.list_id
        , cs.sent_at_cet
        , num_emailed
        , num_complaints
        , unique_clicks
        , unique_opens
        , ds.total_clicks
        , ds.total_opens
        , ds.total_spam_complaints
        , ds.total_undelivered
        , ds.total_unsubscribes
        , cs.updated_at_cet
    from chars_source as cs
    left join datas_source as ds
        on cs.list_id = ds.list_id
            and cs.campaign_id = ds.id 
            and cs.sent_at_cet is not null
            and cs.list_id != 6784256 --6405745 
)

select * from main

