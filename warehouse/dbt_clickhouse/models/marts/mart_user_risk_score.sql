{{
    config(
        materialized='table',
        engine='ReplacingMergeTree(updated_at)',
        order_by='user_id'
    )
}}

with user_base as (
    select * from {{ ref('dim_users') }}
),

user_behavior as (
    select
        user_id,
        -- Transaction patterns
        count(case when is_all_orig_balance = 1 then 1 end) as emptied_account_count,
        count(case when is_dest_zero_init = 1 then 1 end) as to_new_account_count,
        count(case when is_error_balance_orig = 1 or is_error_balance_dest = 1 then 1 end) as balance_error_count,
        
        -- Type distribution
        sum(is_transfer) as transfer_count,
        sum(is_cash_out) as cash_out_count,
        
        -- Time patterns (night transactions: risky hours)
        count(case when hour_of_day >= 22 or hour_of_day <= 6 then 1 end) as night_txns,
        
        -- Amount patterns
        max(amount) as max_txn_amount,
        stddevPop(amount) as amount_volatility
    from {{ ref('fact_transactions') }}
    group by user_id
)

select
    u.user_id,
    u.first_seen,
    u.last_seen,
    u.total_txns,
    u.total_volume,
    u.fraud_count,
    u.avg_txn_amount,
    
    -- Behavior metrics
    coalesce(b.emptied_account_count, 0) as emptied_account_count,
    coalesce(b.to_new_account_count, 0) as to_new_account_count,
    coalesce(b.balance_error_count, 0) as balance_error_count,
    coalesce(b.transfer_count, 0) as transfer_count,
    coalesce(b.cash_out_count, 0) as cash_out_count,
    coalesce(b.night_txns, 0) as night_txns,
    coalesce(b.max_txn_amount, 0) as max_txn_amount,
    coalesce(b.amount_volatility, 0) as amount_volatility,
    
    -- Risk Score Components (0-100 each)
    -- Historical fraud weight: 40
    least(u.fraud_count * 40, 40) as fraud_score,
    -- Emptied account weight: 20
    least(coalesce(b.emptied_account_count, 0) * 5, 20) as emptied_score,
    -- Night transactions weight: 15
    least(coalesce(b.night_txns, 0) * 100.0 / greatest(u.total_txns, 1), 15) as night_score,
    -- Balance errors weight: 15
    least(coalesce(b.balance_error_count, 0) * 3, 15) as error_score,
    -- New accounts weight: 10
    least(coalesce(b.to_new_account_count, 0) * 2, 10) as new_account_score,
    
    -- Total Risk Score (0-100)
    least(
        u.fraud_count * 40 +
        least(coalesce(b.emptied_account_count, 0) * 5, 20) +
        least(coalesce(b.night_txns, 0) * 100.0 / greatest(u.total_txns, 1), 15) +
        least(coalesce(b.balance_error_count, 0) * 3, 15) +
        least(coalesce(b.to_new_account_count, 0) * 2, 10),
        100
    ) as risk_score,
    
    -- Risk Category
    case
        when u.fraud_count > 0 then 'BLOCKED'
        when least(
            u.fraud_count * 40 + 
            least(coalesce(b.emptied_account_count, 0) * 5, 20) + 
            least(coalesce(b.night_txns, 0) * 100.0 / greatest(u.total_txns, 1), 15) + 
            least(coalesce(b.balance_error_count, 0) * 3, 15) + 
            least(coalesce(b.to_new_account_count, 0) * 2, 10),
            100
        ) > 50 then 'HIGH_RISK'
        when least(
            u.fraud_count * 40 + 
            least(coalesce(b.emptied_account_count, 0) * 5, 20) + 
            least(coalesce(b.night_txns, 0) * 100.0 / greatest(u.total_txns, 1), 15) + 
            least(coalesce(b.balance_error_count, 0) * 3, 15) + 
            least(coalesce(b.to_new_account_count, 0) * 2, 10),
            100
        ) > 20 then 'MEDIUM_RISK'
        else 'LOW_RISK'
    end as risk_category,
    
    u.updated_at

from user_base u
left join user_behavior b on u.user_id = b.user_id
