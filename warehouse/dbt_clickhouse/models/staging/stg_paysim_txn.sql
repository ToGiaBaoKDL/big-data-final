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
        isFraud as is_fraud,
        isFlaggedFraud as is_flagged_fraud,
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
        concat(user_id, '-', toString(toUnixTimestamp(transaction_time))) as txn_id
    from source
)

select * from renamed
