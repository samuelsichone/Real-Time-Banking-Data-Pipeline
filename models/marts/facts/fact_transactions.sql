{{ config(materialized='incremental', unique_key='transaction_id') }}

with deduped_txn as (
  select
    transaction_id,
    account_id,
    amount,
    related_account_id,
    status,
    transaction_type,
    transaction_time
  from (
    select
      t.*,
      row_number() over (
        partition by t.transaction_id
        order by t.transaction_time desc
      ) as rn
    from {{ ref('stg_transactions') }} t
  ) t
  where rn = 1
),

account_map as (
  -- dedupe accounts deterministically (keeps first customer_id per account_id)
  select account_id, customer_id
  from (
    select
      a.*,
      row_number() over (
        partition by a.account_id
        order by a.customer_id
      ) as rn
    from {{ ref('stg_accounts') }} a
  ) a
  where rn = 1
)

select
  d.transaction_id,
  d.account_id,
  a.customer_id,
  d.amount,
  d.related_account_id,
  d.status,
  d.transaction_type,
  d.transaction_time,
  current_timestamp as load_timestamp
from deduped_txn d
left join account_map a
  on d.account_id = a.account_id

{% if is_incremental() %}
  where d.transaction_time > (
    select coalesce(max(transaction_time), '1970-01-01'::timestamp) from {{ this }}
  )
{% endif %}