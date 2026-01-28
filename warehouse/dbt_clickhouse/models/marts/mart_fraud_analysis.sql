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
    f.hour_of_day,
    f.day_of_week,
    f.is_all_orig_balance,
    f.is_dest_zero_init,
    
    -- User Attributes
    f.user_id,
    u.total_txns as user_lifetime_txns,
    u.fraud_count as user_lifetime_frauds,
    
    -- Analysis metrics
    (u.fraud_count > 0) as is_high_risk_user
    
from fact f
left join users u on f.user_id = u.user_id
