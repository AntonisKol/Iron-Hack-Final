-- staging model: clean and standardize the raw churn data
-- source() tells dbt to look at the table defined in sources.yml
-- nothing is calculated here — just cleaning
with source as (
    select *
    from {{ source('churn_raw', 'RAW_CHURN') }}
),
cleaned as (
    select -- identifiers
        customerid as customer_id,
        -- demographics
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
        -- account info
        tenure,
        contract,
        case
            when paperlessbilling = 'Yes' then 1
            else 0
        end as is_paperless,
        paymentmethod as payment_method,
        monthlycharges as monthly_charges,
        -- TotalCharges comes as text in raw data — cast to number
        try_to_number(totalcharges) as total_charges,
        -- services
        phoneservice as phone_service,
        multiplelines as multiple_lines,
        internetservice as internet_service,
        onlinesecurity as online_security,
        onlinebackup as online_backup,
        deviceprotection as device_protection,
        techsupport as tech_support,
        streamingtv as streaming_tv,
        streamingmovies as streaming_movies,
        -- target variable: 1 = churned, 0 = stayed
        case
            when churn = 'Yes' then 1
            else 0
        end as is_churned
    from source
)
select *
from cleaned