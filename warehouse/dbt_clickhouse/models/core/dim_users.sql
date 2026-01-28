{{
    config(
        materialized='table',
        engine='ReplacingMergeTree(updated_at)',
        order_by='user_id'
    )
}}

with source as (
    select * from {{ ref('stg_paysim_txn') }}
),

user_stats as (
    select
        user_id,
        min(transaction_time) as first_seen,
        max(transaction_time) as last_seen,
        count(*) as total_txns,
        sum(amount) as total_volume,
        sum(is_fraud) as fraud_count,
        avg(amount) as avg_txn_amount,
        max(transaction_time) as updated_at
    from source
    group by user_id
)

select * from user_stats
