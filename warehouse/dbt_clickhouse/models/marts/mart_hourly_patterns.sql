{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='(day_of_week, hour_of_day)'
    )
}}

-- Fraud patterns by hour and day of week
-- Use case: Dashboard, Alert threshold calibration
with hourly_stats as (
    select
        hour_of_day,
        day_of_week,
        count(*) as total_txns,
        sum(is_fraud) as fraud_count,
        sum(amount) as total_amount,
        sum(case when is_fraud = 1 then amount else 0 end) as fraud_amount,
        avg(amount) as avg_txn_amount,
        
        -- Fraud rate calculation
        sum(is_fraud) * 100.0 / count(*) as fraud_rate_pct,
        
        -- Type breakdown for fraud
        sum(case when is_fraud = 1 and is_transfer = 1 then 1 else 0 end) as fraud_transfers,
        sum(case when is_fraud = 1 and is_cash_out = 1 then 1 else 0 end) as fraud_cash_outs
    from {{ ref('fact_transactions') }}
    group by hour_of_day, day_of_week
)

select
    hour_of_day,
    day_of_week,
    total_txns,
    fraud_count,
    round(fraud_rate_pct, 4) as fraud_rate_pct,
    round(total_amount, 2) as total_amount,
    round(fraud_amount, 2) as fraud_amount,
    round(avg_txn_amount, 2) as avg_txn_amount,
    fraud_transfers,
    fraud_cash_outs,
    
    -- Risk level classification
    case
        when fraud_rate_pct > 1.0 then 'HIGH'
        when fraud_rate_pct > 0.5 then 'MEDIUM'
        else 'LOW'
    end as risk_level
from hourly_stats
