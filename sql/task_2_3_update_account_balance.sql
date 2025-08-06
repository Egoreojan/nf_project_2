WITH account_balance_with_prev AS (
    SELECT 
        ab.account_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum,
        LAG(ab.account_out_sum) OVER (
            PARTITION BY ab.account_rk 
            ORDER BY ab.effective_date
        ) AS prev_day_account_out_sum
    FROM rd.account_balance ab
),
corrections_needed AS (
    SELECT 
        account_rk,
        effective_date,
        account_in_sum AS original_account_in_sum,
        prev_day_account_out_sum AS corrected_account_in_sum
    FROM account_balance_with_prev
    WHERE prev_day_account_out_sum IS NOT NULL 
      AND account_in_sum != prev_day_account_out_sum
)
UPDATE rd.account_balance 
SET account_in_sum = cn.corrected_account_in_sum
FROM corrections_needed cn
WHERE rd.account_balance.account_rk = cn.account_rk
  AND rd.account_balance.effective_date = cn.effective_date;

SELECT 
    COUNT(*) AS updated_records_count,
    'account_in_sum corrections applied' AS operation_description
FROM (
    WITH account_balance_with_prev AS (
        SELECT 
            ab.account_rk,
            ab.effective_date,
            ab.account_in_sum,
            LAG(ab.account_out_sum) OVER (
                PARTITION BY ab.account_rk 
                ORDER BY ab.effective_date
            ) AS prev_day_account_out_sum
        FROM rd.account_balance ab
    )
    SELECT 1
    FROM account_balance_with_prev
    WHERE prev_day_account_out_sum IS NOT NULL 
      AND account_in_sum != prev_day_account_out_sum
) corrections;