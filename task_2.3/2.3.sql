--2.3
--Исследование витрины
SELECT * 
	FROM dm.account_balance_turnover;
	
SELECT DISTINCT currency_name 
	FROM dm.account_balance_turnover;
	
-- Корректировка account_in_sum
SELECT 
    account_rk,
    effective_date,
	account_in_sum,
    CASE
        WHEN account_in_sum <> LAG(account_out_sum, -1, 0) OVER (PARTITION BY account_rk ORDER BY effective_date DESC) 
        THEN LAG(account_out_sum, -1, 0) OVER (PARTITION BY account_rk ORDER BY effective_date DESC)
        ELSE account_in_sum
    END AS account_in_sum_corr,
    account_out_sum
FROM 
    rd.account_balance
ORDER BY 
    account_rk, effective_date ASC;

-- Корректировка account_out_sum
SELECT 
    account_rk,
    effective_date,
    account_in_sum,
    account_out_sum,
    CASE
        WHEN account_out_sum <> LEAD(account_in_sum, -1, 0) OVER (PARTITION BY account_rk ORDER BY effective_date DESC) 
        THEN LEAD(account_in_sum, -1, 0) OVER (PARTITION BY account_rk ORDER BY effective_date DESC)
        ELSE account_out_sum
    END AS account_out_sum_corr
FROM 
    rd.account_balance
ORDER BY 
    account_rk, effective_date ASC;

-- Обновление таблицы rd.account_balance
WITH corrected_account_in_sum AS (
    SELECT 
        account_rk,
        effective_date,
        CASE
            WHEN account_in_sum <> LAG(account_out_sum, -1, 0) OVER (PARTITION BY account_rk ORDER BY effective_date DESC) 
            THEN LAG(account_out_sum, -1, 0) OVER (PARTITION BY account_rk ORDER BY effective_date DESC)
            ELSE account_in_sum
        END AS account_in_sum,
        account_out_sum
    FROM 
        rd.account_balance
    ORDER BY 
        account_rk, effective_date ASC
)
UPDATE 
    rd.account_balance ab
SET 
    account_in_sum = corrected_account_in_sum.account_in_sum
FROM 
    corrected_account_in_sum
WHERE 
    ab.account_rk = corrected_account_in_sum.account_rk
    AND ab.effective_date = corrected_account_in_sum.effective_date;


--Дополнение справочника валют
SELECT * 
	FROM dm.dict_currency;
	
INSERT INTO dm.dict_currency VALUES ('500', 'KZT', '1900-01-01', '2999-12-31');


--Процедура для перезагрузки витрины dm.account_balance_turnover
CREATE OR REPLACE PROCEDURE reload_account_balance_turnover()
LANGUAGE plpgsql
AS $$
BEGIN

    TRUNCATE TABLE dm.account_balance_turnover;

    INSERT INTO dm.account_balance_turnover (
        account_rk,
        currency_name,
        department_rk,
        effective_date,
        account_in_sum,
        account_out_sum
    )
    SELECT
        a.account_rk,
        dc.currency_name,
        a.department_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum
    FROM rd.account a
    LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
    LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
END;
$$;

--Вызов процедуры
CALL reload_account_balance_turnover();

--Проверка
SELECT * 
	FROM dm.account_balance_turnover;

SELECT DISTINCT currency_name 
	FROM dm.account_balance_turnover;