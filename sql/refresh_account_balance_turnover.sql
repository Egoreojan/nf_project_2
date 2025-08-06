CREATE OR REPLACE PROCEDURE refresh_account_balance_turnover()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Очищаем витрину перед перезагрузкой
    DELETE FROM dm.account_balance_turnover;
    
    -- Заполняем витрину данными на основе прототипа
    INSERT INTO dm.account_balance_turnover (
        account_rk,
        currency_name,
        department_rk,
        effective_date,
        account_in_sum,
        account_out_sum
    )
    WITH corrected_account_balance AS (
        -- Применяем коррекцию account_in_sum на основе предыдущего дня
        SELECT 
            ab.account_rk,
            ab.effective_date,
            -- Корректируем account_in_sum если есть расхождение с предыдущим днем
            CASE 
                WHEN LAG(ab.account_out_sum) OVER (
                    PARTITION BY ab.account_rk 
                    ORDER BY ab.effective_date
                ) IS NOT NULL 
                AND ab.account_in_sum != LAG(ab.account_out_sum) OVER (
                    PARTITION BY ab.account_rk 
                    ORDER BY ab.effective_date
                )
                THEN LAG(ab.account_out_sum) OVER (
                    PARTITION BY ab.account_rk 
                    ORDER BY ab.effective_date
                )
                ELSE ab.account_in_sum
            END AS corrected_account_in_sum,
            ab.account_out_sum
        FROM rd.account_balance ab
    )
    SELECT 
        a.account_rk,
        COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
        a.department_rk,
        cab.effective_date,
        cab.corrected_account_in_sum AS account_in_sum,
        cab.account_out_sum
    FROM rd.account a
    LEFT JOIN corrected_account_balance cab ON a.account_rk = cab.account_rk
    LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd
    WHERE cab.effective_date IS NOT NULL  -- Исключаем записи без данных о балансе
    ORDER BY a.account_rk, cab.effective_date;
    
    -- Логируем результат операции
    RAISE NOTICE 'Витрина dm.account_balance_turnover успешно перезагружена. Обработано записей: %', 
        (SELECT COUNT(*) FROM dm.account_balance_turnover);
    
END;
$$;