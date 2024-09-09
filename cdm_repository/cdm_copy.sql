COPY STV2024031224__DWH.global_metrics (
date_update, 
currency_from, 
amount_total, 
cnt_transactions,
avg_transactions_per_account,
cnt_accounts_make_transactions) 
from stdin DELIMITER '|'