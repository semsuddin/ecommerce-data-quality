-- Deduplicated customer dimension (one row per customer_id), so the mart's
-- primary key is guaranteed unique even though the source had duplicates.
with ranked as (
    select
        *,
        row_number() over (partition by customer_id order by created_at desc) as rn
    from {{ ref('stg_customers') }}
)
select
    customer_id,
    email,
    country,
    created_at,
    (email is not null and regexp_contains(email, r'^[^@\s]+@[^@\s]+\.[^@\s]+$')) as email_is_valid
from ranked
where rn = 1
