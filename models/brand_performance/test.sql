select 
    date_parsed, 
    date
    geo as country_code, 
    registrations as signups
from {{ source('main','records') }} records
where right(brand_name,6)<>'sports'
    and date_parsed > '2023-12-31'
    and geo='vn'
    and brand_name='20bet'
    and registrations>0
order by date_parsed desc


-- select * from {{ source('main','campaign_names_mapping') }} where geo='vn'