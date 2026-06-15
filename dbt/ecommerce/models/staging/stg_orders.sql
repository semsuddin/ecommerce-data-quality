with src as (select * from {{ source('raw', 'orders') }})
select
    cast(order_id as int64)               as order_id,
    cast(customer_id as int64)            as customer_id,
    cast(order_date as timestamp)         as order_date,
    lower(trim(status))                   as status
from src
