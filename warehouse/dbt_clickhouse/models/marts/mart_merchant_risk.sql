{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='merchant_id'
    )
}}

with fact as (
    select * from {{ ref('fact_transactions') }}
),

merchant_stats as (
    select
        merchant_id,
        count(*) as total_incoming_txns,
        sum(amount) as total_received_volume,
        avg(amount) as avg_received_amount,
        max(amount) as max_received_amount,
        stddevPop(amount) as stddev_received_amount,
        sum(is_fraud) as fraud_received_count,
        count(distinct user_id) as unique_senders,
        sum(is_fraud) / count(*) as fraud_risk_ratio
    from fact
    where is_merchant_dest = 1 
    group by merchant_id
)

select * from merchant_stats
