-- Q7: Customer Churn Data Mart — Staging Layer
-- Reads raw Snowflake source, renames columns to snake_case,
-- and converts Yes/No strings to 1/0 integers. No business logic here.
with source as (
    select *
    from {{ source('churn_raw', 'RAW_CHURN') }}
),
cleaned as (
    select
        customerid as customer_id,
        gender,
        seniorcitizen as is_senior,
        case
            when partner = 'Yes' then 1
            else 0
        end as has_partner,
        case
            when dependents = 'Yes' then 1
            else 0
        end as has_dependents,
        tenure,
        contract,
        case
            when paperlessbilling = 'Yes' then 1
            else 0
        end as is_paperless,
        paymentmethod as payment_method,
        monthlycharges as monthly_charges,
        try_to_number(totalcharges) as total_charges,
        phoneservice as phone_service,
        multiplelines as multiple_lines,
        internetservice as internet_service,
        onlinesecurity as online_security,
        onlinebackup as online_backup,
        deviceprotection as device_protection,
        techsupport as tech_support,
        streamingtv as streaming_tv,
        streamingmovies as streaming_movies,
        case
            when churn = 'Yes' then 1
            else 0
        end as is_churned
    from source
)
select *
from cleaned
