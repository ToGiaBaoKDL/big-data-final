{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by='type'
    )
}}

select
    type,
    count(*) as total_txns,
    sum(is_fraud) as fraud_count,
    round(sum(is_fraud) * 100.0 / count(*), 4) as fraud_rate_pct,
    
    -- Amount stats
    round(sum(amount), 2) as total_volume,
    round(avg(amount), 2) as avg_amount,
    round(min(amount), 2) as min_amount,
    round(max(amount), 2) as max_amount,
    
    -- Fraud amount stats
    round(sum(case when is_fraud = 1 then amount else 0 end), 2) as fraud_volume,
    round(avg(case when is_fraud = 1 then amount else null end), 2) as avg_fraud_amount,
    
    -- Balance error analysis
    sum(is_error_balance_orig) as balance_error_orig_count,
    sum(is_error_balance_dest) as balance_error_dest_count,
    
    -- Account patterns
    sum(is_all_orig_balance) as emptied_account_count,
    sum(is_dest_zero_init) as new_dest_account_count,
    
    -- Percentage of total
    round(count(*) * 100.0 / sum(count(*)) over (), 2) as pct_of_total_txns,
    round(sum(amount) * 100.0 / sum(sum(amount)) over (), 2) as pct_of_total_volume

from {{ ref('fact_transactions') }}
group by type
order by total_txns desc
