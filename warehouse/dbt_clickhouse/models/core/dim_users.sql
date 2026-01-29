{{
    config(
        materialized='incremental',
        engine='ReplacingMergeTree(updated_at)',
        order_by='user_id',
        unique_key='user_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('stg_paysim_txn') }}
    {% if is_incremental() %}
    where part_dt >= (select max(toString(toDate(last_seen))) from {{ this }})
    {% endif %}
),

-- Calculate stats for users in new data
new_user_stats as (
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

{% if is_incremental() %}
-- Merge with existing data
select
    coalesce(n.user_id, o.user_id) as user_id,
    coalesce(o.first_seen, n.first_seen) as first_seen,
    coalesce(n.last_seen, o.last_seen) as last_seen,
    coalesce(o.total_txns, 0) + coalesce(n.total_txns, 0) as total_txns,
    coalesce(o.total_volume, 0) + coalesce(n.total_volume, 0) as total_volume,
    coalesce(o.fraud_count, 0) + coalesce(n.fraud_count, 0) as fraud_count,
    -- Weighted average
    case 
        when (coalesce(o.total_txns, 0) + coalesce(n.total_txns, 0)) > 0 
        then (coalesce(o.avg_txn_amount * o.total_txns, 0) + coalesce(n.avg_txn_amount * n.total_txns, 0)) 
             / (coalesce(o.total_txns, 0) + coalesce(n.total_txns, 0))
        else 0 
    end as avg_txn_amount,
    coalesce(n.updated_at, o.updated_at) as updated_at
from new_user_stats n
full outer join {{ this }} o on n.user_id = o.user_id
{% else %}
-- First run: just select new stats
select * from new_user_stats
{% endif %}
