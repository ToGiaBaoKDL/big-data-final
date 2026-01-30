with source as (
    select * from {{ source('finance_dw', 'paysim_txn') }}
),

renamed as (
    select
        transaction_time,
        type,
        amount,
        user_id,
        merchant_id,
        -- Balance columns
        oldbalanceOrg as old_balance_orig,
        newbalanceOrig as new_balance_orig,
        oldbalanceDest as old_balance_dest,
        newbalanceDest as new_balance_dest,
        -- Fraud flags
        isFraud as is_fraud,
        isFlaggedFraud as is_flagged_fraud,
        -- Engineered features
        errorBalanceOrig as error_balance_orig,
        errorBalanceDest as error_balance_dest,
        is_transfer,
        is_cash_out,
        is_merchant_dest,
        hour_of_day,
        day_of_week,
        is_all_orig_balance,
        is_dest_zero_init,
        is_org_zero_init,
        is_errorBalanceOrig as is_error_balance_orig,
        is_errorBalanceDest as is_error_balance_dest,
        -- Metadata
        processed_at,
        -- Partition columns
        part_dt,
        part_hour,
        -- Surrogate key
        concat(user_id, '-', toString(toUnixTimestamp(transaction_time))) as txn_id
    from source
)

select * from renamed
