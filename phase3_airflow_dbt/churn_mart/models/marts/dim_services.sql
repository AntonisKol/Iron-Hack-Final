-- dimension table: one row per customer with their service subscriptions
-- this answers WHAT — which services each customer is using
-- kept separate from dim_customers to follow star schema design principles
with int_customers as (
    select *
    from {{ ref('int_customer_features') }}
)
select -- primary key: links back to dim_customers and fct_churn
    customer_id,
    -- phone services
    phone_service,
    multiple_lines,
    -- internet services
    internet_service,
    -- add-on services (Yes / No / No internet service)
    online_security,
    online_backup,
    device_protection,
    tech_support,
    streaming_tv,
    streaming_movies
from int_customers