{{
    config(
        materialized='incremental',
        engine='MergeTree()',
        order_by='transaction_time',
        unique_key='txn_id',
        incremental_strategy='delete+insert'
    )
}}

select
    txn_id,
    transaction_time,
    type,
    amount,
    user_id,
    merchant_id,
    old_balance_orig,
    new_balance_orig,
    old_balance_dest,
    new_balance_dest,
    error_balance_orig,
    error_balance_dest,
    is_fraud,
    is_flagged_fraud,
    is_transfer,
    is_cash_out,
    is_merchant_dest,
    hour_of_day,
    day_of_week,
    is_all_orig_balance,
    is_dest_zero_init,
    is_org_zero_init,
    is_error_balance_orig,
    is_error_balance_dest,
    part_dt,
    part_hour
from {{ ref('stg_paysim_txn') }}

{% if is_incremental() %}
-- Only load new partitions
where part_dt >= (select max(part_dt) from {{ this }})
{% endif %}
