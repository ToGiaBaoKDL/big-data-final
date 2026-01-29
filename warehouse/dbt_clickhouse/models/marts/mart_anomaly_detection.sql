{{
    config(
        materialized='table',
        engine='MergeTree()',
        partition_by='toYYYYMMDD(transaction_time)',
        order_by='(transaction_time, user_id)'
    )
}}

with anomalies as (
    select
        txn_id,
        transaction_time,
        user_id,
        merchant_id,
        type,
        amount,
        old_balance_orig,
        new_balance_orig,
        old_balance_dest,
        new_balance_dest,
        error_balance_orig,
        error_balance_dest,
        is_fraud,
        is_all_orig_balance,
        is_dest_zero_init,
        hour_of_day,
        
        -- Anomaly flags
        case when abs(error_balance_orig) > 0.01 then 1 else 0 end as has_orig_balance_error,
        case when abs(error_balance_dest) > 0.01 then 1 else 0 end as has_dest_balance_error,
        case when amount > 200000 then 1 else 0 end as is_large_transaction,
        case when hour_of_day >= 22 or hour_of_day <= 6 then 1 else 0 end as is_night_transaction,
        
        -- Anomaly score (0-5)
        (case when abs(error_balance_orig) > 0.01 then 1 else 0 end) +
        (case when abs(error_balance_dest) > 0.01 then 1 else 0 end) +
        (case when amount > 200000 then 1 else 0 end) +
        (case when hour_of_day >= 22 or hour_of_day <= 6 then 1 else 0 end) +
        is_all_orig_balance as anomaly_score
        
    from {{ ref('fact_transactions') }}
)

select
    *,
    -- Priority for investigation
    case
        when is_fraud = 1 then 'CONFIRMED_FRAUD'
        when anomaly_score >= 4 then 'CRITICAL'
        when anomaly_score >= 3 then 'HIGH'
        when anomaly_score >= 2 then 'MEDIUM'
        when anomaly_score >= 1 then 'LOW'
        else 'NORMAL'
    end as investigation_priority,
    
    -- Anomaly reasons
    concat(
        case when has_orig_balance_error = 1 then 'ORIG_BALANCE_ERROR,' else '' end,
        case when has_dest_balance_error = 1 then 'DEST_BALANCE_ERROR,' else '' end,
        case when is_large_transaction = 1 then 'LARGE_TXN,' else '' end,
        case when is_night_transaction = 1 then 'NIGHT_TXN,' else '' end,
        case when is_all_orig_balance = 1 then 'EMPTIED_ACCOUNT,' else '' end
    ) as anomaly_reasons

from anomalies
where anomaly_score >= 1 or is_fraud = 1
