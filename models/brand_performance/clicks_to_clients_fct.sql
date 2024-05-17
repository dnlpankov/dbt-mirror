select
    timestamp as timestamp_cet
    , deal_id
    , user_id
    , brand_name as brand_id
    , geo as country_code
    -- , campaign_group_id
    , event_type as event_id
    -- , campaign_vertical_id
    -- , google_ads_campaign_id
    -- , traffic_source_id
    , adclickid as ad_click_id
    -- , moneypage_id
    -- , site_id
    -- , affiliate_account_id
    -- , offer_id
from postbacks_outgoing
