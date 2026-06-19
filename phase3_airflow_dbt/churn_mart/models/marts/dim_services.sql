with int_customers as (
    select *
    from {{ ref('int_customer_features') }}
)
select
    customer_id,
    phone_service,
    multiple_lines,
    internet_service,
    online_security,
    online_backup,
    device_protection,
    tech_support,
    streaming_tv,
    streaming_movies
from int_customers
