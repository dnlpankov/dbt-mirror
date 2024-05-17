-- models/staging/scraper/stg_scraper__records.sql

{{
  config(
    materialized='table'
  )
}}

with source as (
    select * from {{ source('postgres','records') }}
)

, transformed as (
    select
        id
        , created_at
        , user_id
        , deal_id
        , date_parsed as date_cet
        , click_id
        , geo as country_code
        , registrations as signed_up
        , cpa_count as deposited_first_time
        , gtee_count
        , cpa_commissions as acquisition_commission
        , deposits as acquisition_deposit
        , total_commission
        , gtee_commissions
        , net_revenue
        , revshare_commissions
        , lower(adgroup_name) as ga_campaign_name
        , case
            when right(brand_name, 6) <> 'sports' then 'casino'
            when right(brand_name, 6) = 'sports' then 'sports'
            else 'other'
        end as campaign_vertical
        , case
            when campaign_name::text = 'email' then brand_name || ' email'
            when campaign_name::text = 'PA' then brand_name || ' PA'
            else brand_name
        end as brand_name

        , case
            when campaign_name = 'jpluckyslotsonline' then 'luckyslotsonline'
            when campaign_name = 'ficashstormslots' then 'cashstormslots'
            when campaign_name = 'goldenlion' then 'goldenliongames'
            else campaign_name
        end as campaign_name
    from source
    where
        date_parsed > '2024-03-31'
        --and cpa_count > 0.5
        --and deal_id is null
        --and gtee_commissions > 0 --and cpa_count>0.5 and total_commission>cpa_commissions -- noqa: LT05
    --and user_id='ae4eb2f5ad8ebf29'
    order by user_id, deal_id, date_parsed
)

-- Add grain_id

, added_grain as (
    select
        *
        , md5(user_id || deal_id || date_cet) as grain_id
    from transformed
)


-- Identify duplicates by assigning row numbers
, ranked_records as (
    select
        *
        , row_number() over (
            partition by grain_id -- columns that define a duplicate
            order by id desc -- criteria to determine which record to keep
        ) as duplicate_count
    from added_grain
)

-- Filter out duplicates, keeping only the first occurrence
, deduplicated_records as (
    select *
    from
        ranked_records
    where
        duplicate_count = 1
)

select * from deduplicated_records



--main where user_id='51a4a42eaaeb12f7' and deal_id='2609' and date_cet='2024-05-16'


-- select user_id, deal_id, date_cet, count(id) as duplicates
-- from main
-- group by user_id, deal_id, date_cet
-- having count(id)>1.1
-- select user_id, date_parsed, registrations, depositing_customers, cpa_count

-- from records
-- where user_id='931800d1c75e2834'
-- order by date_parsed


-- with main as (
--     select user_id, created_at, deal_id, date, date_parsed
--         , case
--             when date ~ '^\d{2}-\d{2}-\d{4}$' then to_date(date, 'DD-MM-YYYY')
--             when date ~ '^\d{4}-\d{2}-\d{2}$' then to_date(date, 'YYYY-MM-DD')
--             when date ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$' then to_timestamp(date, 'YYYY-MM-DD HH24:MI:SS')::date
--             when date ~ '^\d{1,2}/\d{1,2}/\d{2} \d{1,2}:\d{2}:\d{2} (AM|PM)$' then to_timestamp(date, 'MM/DD/YY HH12:MI:SS AM')::date
--             when date ~ '^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2} (AM|PM)$' then to_timestamp(date, 'MM/DD/YYYY HH12:MI:SS AM')::date
--             when date ~ '^\d{4}\.\d{2}\.\d{2}$' then to_date(date, 'YYYY.MM.DD')
--             when date ~ '^\d{5}-\d{2}-\d{2}$' then to_date(substring(date from 1 for 4) || substring(date from 6), 'YYYY-MM-DD')
--             else null
--         end as transformed_date
--     from records
-- ),

-- comparison as 
-- (select
--     *,
--     (case
--         when date_parsed = transformed_date then 1
--         else 0
--     end) as comparison
-- from main)

-- select * from comparison where comparison = 0 and date_parsed>'2024-04-30'
-- select sum(comparison), count(comparison)
-- from comparison
-- where date_parsed>'2024-01-31'