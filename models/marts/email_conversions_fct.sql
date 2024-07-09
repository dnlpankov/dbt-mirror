{{ config(materialized='table') }}

WITH campaign_data AS (
    SELECT
        country_code,
        brand_name,
        campaign_type,
        aweber_campaign_id,
        list_id,
        sent_at_cet
    FROM
        {{ ref('aweber_campaign_dim') }}
),
conversion_data AS (
    SELECT
        campaign_name,
        country_code,
        is_welcome_campaign,
        brand_name,
        timestamp_parsed,
        date_parsed,
        registrations,
        cpa_commissions,
        first_time_deposit,
        regs_plus_ftds
    FROM
        {{ ref('stg_scraper__email_records') }}
)
,
longitudinal_campaigns AS (
    SELECT
        c1.list_id,
        c1.country_code,
        c1.brand_name,
        c1.sent_at_cet,
        c1.aweber_campaign_id,
        LEAD(c1.sent_at_cet) OVER (PARTITION BY c1.list_id, c1.country_code, c1.brand_name ORDER BY c1.sent_at_cet, c1.aweber_campaign_id) AS next_sent_at_cet
    FROM
        campaign_data c1
)
,
merged_data AS (
    SELECT
        lc.list_id,
        --lc.country_code as ls_cc,
        --lc.brand_name as ls_bn,
        case when lc.country_code is not null then lc.country_code else cd.country_code end as country_code,
        case when lc.brand_name is not null then lc.brand_name else cd.brand_name end as brand_name,
        cd.is_welcome_campaign,
        --lc.brand_name,
        lc.sent_at_cet,
        lc.next_sent_at_cet,
        lc.aweber_campaign_id,
        cd.campaign_name,
        cd.timestamp_parsed,
        cd.date_parsed,
        cd.registrations,
        cd.cpa_commissions,
        cd.first_time_deposit,
        cd.regs_plus_ftds
    FROM
        longitudinal_campaigns lc
    FULL OUTER JOIN
        conversion_data cd
    ON
        lc.country_code = cd.country_code
        AND lc.brand_name = cd.brand_name
        AND cd.date_parsed >= lc.sent_at_cet
        AND (cd.date_parsed < lc.next_sent_at_cet OR lc.next_sent_at_cet IS NULL)
        and cd.is_welcome_campaign is False
        --and cd.first_time_deposit>0.5
    
),

updated_data as (
    select *
    , CASE
        WHEN sent_at_cet IS NULL THEN true
        WHEN (date_parsed - sent_at_cet) > interval '7 days' THEN true 
        ELSE false 
    END AS is_welcome_campaign_by_time_difference
    , CASE
        WHEN sent_at_cet IS NULL
            THEN DATE_TRUNC('week', date_parsed)::date - interval '1 day'
        WHEN (date_parsed - sent_at_cet) > interval '7 days' 
            THEN DATE_TRUNC('week', date_parsed)::date - interval '1 day'
        ELSE sent_at_cet 
    END AS sent_at_cet_2 
from merged_data
where first_time_deposit>0.5
)
, final_data AS (
    SELECT
        *
        , CASE
            WHEN is_welcome_campaign_by_time_difference
                THEN null
            ELSE next_sent_at_cet
        END AS next_sent_at_cet_2
        , CASE
            WHEN is_welcome_campaign_by_time_difference
                THEN 0
            ELSE aweber_campaign_id
        END AS aweber_campaign_id_2
        , CASE
            WHEN is_welcome_campaign_by_time_difference THEN
                'welcome_' || TO_CHAR(sent_at_cet_2, 'MM-DD-YYYY')
            ELSE
                'broadcast_' || TO_CHAR(sent_at_cet_2, 'MM-DD-YYYY')
        END AS campaign_name_2
    FROM updated_data
)
SELECT
    list_id
    , country_code
    , brand_name
    , campaign_name_2 as campaign_name
    , is_welcome_campaign_by_time_difference AS is_welcome_campaign
    , sent_at_cet_2 AS sent_at_cet
    , next_sent_at_cet_2 AS next_sent_at_cet
    , aweber_campaign_id_2 AS aweber_campaign_id
    , date_parsed
    , registrations
    , first_time_deposit

FROM final_data


-- SELECT
--     list_id 
--     , acf.aweber_campaign_id
--     , country_code
--     , brand_name
--     , md.sent_at_cet
--     , num_emailed
--     , total_opens
--     , total_clicks
--     --total_spam_complaints
--     , total_undelivered
--     , first_time_deposit

--     , next_sent_at_cet
--     -- campaign_name,
--     -- timestamp_parsed,
--     , date_parsed
--     , registrations,
--     cpa_commissions,

--     regs_plus_ftds
-- FROM
--     merged_data md
--     left join {{ ref('aweber_campaign_fct') }} acf 
--     on md.aweber_campaign_id = acf.aweber_campaign_id
-- ORDER BY
--     country_code, brand_name, sent_at_cet, date_parsed

