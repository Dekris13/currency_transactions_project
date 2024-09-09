select count(*) from STV2024031224__STAGING.transactions
where transaction_dt::date  = %s 