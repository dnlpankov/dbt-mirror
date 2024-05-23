select records.*
    , deals.traffic_source_id
    , deals.campaign_vertical_id
from {{ ref('stg_scraper__records') }} as records
left join {{ ref('stg_backend__deals_dimension') }} as deals
    on records.deal_id = deals.deal_id and records.deposited_first_time > 0.5
left join {{ ref("stg_backend__traffic_sources") }} as traffic_sources
    on deals.traffic_source_id = traffic_sources.id
left join {{ ref("stg_backend__campaign_verticals") }} as verticals
    on deals.campaign_vertical_id = verticals.id
--where deals.deal_id is null