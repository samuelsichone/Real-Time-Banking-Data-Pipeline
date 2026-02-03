{{ config(
    materialized='incremental',
    unique_key='transaction_id'
) }}

WITH source_data AS (
    SELECT
        t.transaction_id,
        t.account_id,
        a.customer_id,
        t.amount,
        t.related_account_id,
        t.status,
        t.transaction_type,
        t.transaction_time,
        CURRENT_TIMESTAMP AS load_timestamp,
        ROW_NUMBER() OVER (PARTITION BY t.transaction_id ORDER BY t.transaction_time DESC) AS rn
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN {{ ref('stg_accounts') }} a
        ON t.account_id = a.account_id
)

SELECT
    transaction_id,
    account_id,
    customer_id,
    amount,
    related_account_id,
    status,
    transaction_type,
    transaction_time,
    load_timestamp
FROM source_data
WHERE rn = 1
{% if is_incremental() %}
  AND transaction_time > (SELECT COALESCE(MAX(transaction_time), '1900-01-01') FROM {{ this }})
{% endif %}
