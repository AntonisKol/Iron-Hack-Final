-- Dimension table: one row per customer with their service subscriptions.
-- Answers WHAT — phone, internet, and all add-on services each customer has.
-- Kept separate from dim_customers so each dimension has a single responsibility.
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