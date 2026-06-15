with ranked as (
    select
        *,
        row_number() over (partition by product_id order by product_id) as rn
    from {{ ref('stg_products') }}
)
select
    product_id,
    product_name,
    category,
    unit_price
from ranked
where rn = 1
