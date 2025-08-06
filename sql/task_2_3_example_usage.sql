\echo 'Анализ проблем с account_in_sum:'
\i task_2_3_correct_account_in_sum.sql

\echo 'Анализ проблем с account_out_sum:'
\i task_2_3_correct_account_out_sum.sql

\echo 'Исправление данных в rd.account_balance:'
\i task_2_3_update_account_balance.sql

\echo 'Перезагрузка витрины dm.account_balance_turnover:'
CALL refresh_account_balance_turnover();

\echo 'Проверка результата:'
SELECT 
    COUNT(*) AS total_records,
    COUNT(DISTINCT account_rk) AS unique_accounts,
    MIN(effective_date) AS min_date,
    MAX(effective_date) AS max_date
FROM dm.account_balance_turnover;

WITH balance_check AS (
    SELECT 
        account_rk,
        effective_date,
        account_in_sum,
        account_out_sum,
        LAG(account_out_sum) OVER (
            PARTITION BY account_rk 
            ORDER BY effective_date
        ) AS prev_account_out_sum
    FROM dm.account_balance_turnover
)
SELECT 
    COUNT(*) AS remaining_inconsistencies
FROM balance_check
WHERE prev_account_out_sum IS NOT NULL 
  AND account_in_sum != prev_account_out_sum;