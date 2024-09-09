with first_select as -- финансовые показатели часть 1 
(SELECT
t.operation_id, 
t.account_number_from,
t.account_number_to,
t.currency_code,
t.country,
t.status,
t.transaction_type,
case when t.amount < 0 then -t.amount else t.amount end as amount,
transaction_dt,
c.currency_code as c_currency_code, 
c.currency_code_with, 
c.currency_with_div,
case when t.currency_code = 420 then case when t.amount < 0 then -t.amount else t.amount end
else case when t.amount < 0 then -t.amount * c.currency_with_div  else t.amount * c.currency_with_div end
end as amount_total
from STV2024031224__STAGING.transactions t
left join STV2024031224__STAGING.currencies c on 
case WHEN t.currency_code != 420 then t.currency_code = c.currency_code and  c.currency_code_with = 420
else Null end
where account_number_from >= 0 
and status = 'done' 
and transaction_type != 'authorization' 
and transaction_dt::date = %s------------------------------------------------------------------ изменяемая дата
),
second_select as-- финансовые показатели часть 2
(SELECT 
transaction_dt::date as date_update,
currency_code as currency_from,
SUM(amount_total)  as amount_total,
sum (amount) as cnt_transactions
from first_select
group by transaction_dt::date, currency_code
order by currency_code),
avg_one_account as (			--расчет средний объём транзакций с аккаунта часть 1
SELECT 
transaction_dt::date as transaction_date,
currency_code,
account_number_from,
avg(case when amount < 0 then -amount else amount end) as avg_amount
from STV2024031224__STAGING.transactions
where account_number_from >= 0 
and status = 'done' 
and transaction_type != 'authorization'
and transaction_dt::date = %s--------------------------------------------------------------------- изменяемая дата
group by transaction_dt::date, currency_code, account_number_from)
,
avg_all_accaunt as --расчет средний объём транзакций с аккаунта часть 2
(SELECT 
t0.transaction_date as date_update,
currency_code,
AVG(t0.avg_amount) as avg_transactions_per_account
from avg_one_account t0
group by transaction_date, t0.currency_code
),
unique_account AS-- количество уникальных аккаунтов с совершёнными транзакциями по валюте
(SELECT 
transaction_dt::date as date_update,
currency_code,
count (distinct account_number_from) as cnt_accounts_make_transactions
from STV2024031224__STAGING.transactions
where account_number_from >= 0 
and status = 'done' 
and transaction_type != 'authorization'
and transaction_dt::date = %s --------------------------------------------------------------- изменяемая дата
group by transaction_dt::date, currency_code),
final_select as 
(SELECT 
t0.date_update as date_update,
t0.currency_from as currency_from,
t0.amount_total as amount_total,
t0.cnt_transactions as cnt_transactions,
t1.avg_transactions_per_account as avg_transactions_per_account,
t2.cnt_accounts_make_transactions as cnt_accounts_make_transactions
from second_select t0
join avg_all_accaunt t1 on t0.date_update = t1.date_update and t0.currency_from = t1.currency_code
join unique_account t2 on t0.date_update = t2.date_update and t0.currency_from = t2.currency_code)
select * from final_select;