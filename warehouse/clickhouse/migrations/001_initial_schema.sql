CREATE DATABASE IF NOT EXISTS finance_dw;

-- Paysim Transactions Table
CREATE TABLE IF NOT EXISTS finance_dw.paysim_txn (
    transaction_time DateTime,
    type String,
    amount Float64,
    user_id String,
    oldbalanceOrg Float64,
    newbalanceOrig Float64,
    merchant_id String,
    oldbalanceDest Float64,
    newbalanceDest Float64,
    isFraud UInt8,
    isFlaggedFraud UInt8,
    
    -- Engineered Features
    errorBalanceOrig Float64,
    errorBalanceDest Float64,
    is_transfer UInt8,
    is_cash_out UInt8,
    is_merchant_dest UInt8,
    
    hour_of_day UInt8,
    day_of_week UInt8,
    is_all_orig_balance UInt8,
    is_dest_zero_init UInt8,
    is_org_zero_init UInt8,
    
    -- Partition Columns
    part_dt String,
    part_hour String
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(transaction_time)
ORDER BY (transaction_time, user_id)
TTL transaction_time + INTERVAL 1 YEAR;
