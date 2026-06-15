with src as (select * from {{ source('raw', 'order_items') }})
select
    cast(order_item_id as int64)          as order_item_id,
    cast(order_id as int64)               as order_id,
    cast(product_id as int64)             as product_id,
    cast(quantity as int64)               as quantity,
    cast(unit_price as numeric)           as unit_price,
    cast(line_total as numeric)           as line_total
from src
