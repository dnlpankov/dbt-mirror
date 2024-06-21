{{ config(materialized= 'table' ) }}


with chars_source as (
    select * from {{ source('backend_danila','stg_aweber__campaign_characteristics') }}
)

with datas_source as (
    select * from {{ source('backend_danila','stg_aweber__campaign_data') }}
)


with main as (
    select
        campaign_id
        , account_id
        , list_id
        , sent_at_cet
        , num_emailed
        , num_emailed
        , num_complaints
        , unique_clicks
        , unique_opens
        , ds.total_clicks
        , ds.total_opens
        , ds.total_spam_complaints
        , ds.total_undelivered
        , ds.total_unsubscribers
        , cs.updated_at_cet
    from chars_source cs
    left join datas_source ds
    on cs.list_id=ds.list_id
    cs.campaign_id=ds.id and cs.sent_at_cent not null
)

select * from main

