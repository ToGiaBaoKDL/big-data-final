{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='transaction_time'
    )
}}

select
    txn_id,
    transaction_time,
    type,
    amount,
    user_id,
    merchant_id,
    oldbalanceOrg as old_balance_orig,
    newbalanceOrig as new_balance_orig,
    oldbalanceDest as old_balance_dest,
    newbalanceDest as new_balance_dest,
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
    is_org_zero_init
from {{ ref('stg_paysim_txn') }}
