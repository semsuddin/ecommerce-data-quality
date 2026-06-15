-- Light normalisation only: cast + rename. No repair, so downstream tests and
-- the validation step can still surface source defects.
with src as (select * from {{ source('raw', 'customers') }})
select
    cast(customer_id as int64)            as customer_id,
    nullif(trim(lower(email)), '')        as email,
    upper(trim(country))                  as country,
    cast(created_at as timestamp)         as created_at
from src
