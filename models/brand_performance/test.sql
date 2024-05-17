-- with main as (
--     select
--         id
--         , user_id
--         , conversion_timestamp
--         , registrations
--         , deal_id
--         , date_parsed as date_cet
--         --, click_id
--         , geo as country_code
--         -- , registrations as signed_up
--         --, cpa_count as deposited_first_time
--         -- , gtee_count
--         , cpa_commissions as acquisition_commission
--         -- , total_commission
--         -- , gtee_commissions
--         -- , net_revenue
--         -- , revshare_commissions
--         , lower(adgroup_name) as ga_campaign_name
--         , case
--             when right(brand_name, 6) <> 'sports' then 'casino'
--             when right(brand_name, 6) = 'sports' then 'sports'
--             else 'other'
--         end as campaign_vertical
--         , case
--             when campaign_name::text = 'email' then brand_name || ' email'
--             when campaign_name::text = 'PA' then brand_name || ' PA'
--             else brand_name
--         end as brand_name

--         , case
--             when campaign_name = 'jpluckyslotsonline' then 'luckyslotsonline'
--             when campaign_name = 'ficashstormslots' then 'cashstormslots'
--             when campaign_name = 'goldenlion' then 'goldenliongames'
--             else campaign_name
--         end as campaign_name
--     from records
--     where
--         date_parsed > '2024-03-31'
--         and cpa_count > 0.5
--         --and deal_id is null
--         --and gtee_commissions > 0 --and cpa_count>0.5 and total_commission>cpa_commissions -- noqa: LT05
--     --and user_id='ae4eb2f5ad8ebf29'
--     order by user_id, deal_id, date_parsed
-- )




-- select * from main where user_id='51a4a42eaaeb12f7' and deal_id='2609' and date_cet='2024-05-16'


SELECT id, user_id, date_parsed, date, brand_name, cpa_count, registrations, conversion_timestamp
FROM records
WHERE id='5393572' or id= '5393571'--date_parsed = '2024-05-16' AND user_id = '51a4a42eaaeb12f7'
--GROUP BY user_id, conversion_timestamp, date_parsed, brand_name, cpa_count, registrations
--HAVING COUNT(*) = 1