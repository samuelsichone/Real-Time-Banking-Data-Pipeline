{{ config(
    materialized = 'incremental',
    unique_key = 'transaction_id'
) }}

with transactions_deduped as (

    select
        *,
        row_number() over (
            partition by transaction_id
            order by transaction_time desc
        ) as rn
    from {{ ref('stg_transactions') }}

),

accounts_deduped as (

    select
        *,
        row_number() over (
            partition by account_id
            order by updated_at desc
        ) as rn
    from {{ ref('stg_accounts') }}

)

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    current_timestamp as load_timestamp
from transactions_deduped t
left join accounts_deduped a
  on t.account_id = a.account_id
 and a.rn = 1
where t.rn = 1

{% if is_incremental() %}
  and t.transaction_time >
      (select coalesce(max(transaction_time), '1900-01-01') from {{ this }})
{% endif %}
