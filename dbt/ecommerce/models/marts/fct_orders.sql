-- Order-grain fact: one row per order with the reconstructed total and item
-- count from order_items, plus a validity flag for downstream filtering.
with orders as (
    select * from {{ ref('stg_orders') }}
    qualify row_number() over (partition by order_id order by order_date desc) = 1
),
items as (
    select
        order_id,
        count(*)            as item_count,
        sum(quantity)       as total_quantity,
        sum(line_total)     as order_amount
    from {{ ref('stg_order_items') }}
    group by order_id
)
select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    coalesce(i.item_count, 0)     as item_count,
    coalesce(i.total_quantity, 0) as total_quantity,
    coalesce(i.order_amount, 0)   as order_amount,
    (o.customer_id in (select customer_id from {{ ref('dim_customers') }})
        and o.status in ('pending','paid','shipped','delivered','cancelled','refunded')
        and o.order_date <= current_timestamp()) as order_is_valid
from orders o
left join items i using (order_id)
