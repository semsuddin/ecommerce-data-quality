with src as (select * from {{ source('raw', 'products') }})
select
    cast(product_id as int64)             as product_id,
    trim(product_name)                    as product_name,
    initcap(trim(category))               as category,
    cast(unit_price as numeric)           as unit_price
from src
