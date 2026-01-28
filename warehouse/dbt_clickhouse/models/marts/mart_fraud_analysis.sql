{{
    config(
        materialized='table',
        engine='MergeTree()',
        partition_by='toYYYYMMDD(transaction_time)',
        order_by='(transaction_time, user_id)'
    )
}}

with fact as (
    select * from {{ ref('fact_transactions') }}
),

users as (
    select * from {{ ref('dim_users') }}
)

select
    f.transaction_time,
    f.type,
    f.amount,
    f.is_fraud,
    f.merchant_id,
    f.is_merchant_dest,
    f.is_transfer,
    f.is_cash_out,
    f.hour_of_day,
    f.day_of_week,
    f.is_all_orig_balance,
    f.is_dest_zero_init,
    f.is_org_zero_init,
    f.is_error_balance_orig,
    f.is_error_balance_dest,
    
    -- User Attributes
    f.user_id,
    u.total_txns as user_lifetime_txns,
    u.fraud_count as user_lifetime_frauds,
    
    -- Analysis metrics
    (u.fraud_count > 0) as is_high_risk_user
    
from fact f
left join users u on f.user_id = u.user_id
