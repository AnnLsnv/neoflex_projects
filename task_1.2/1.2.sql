--1.2

CREATE SCHEMA dm;

CREATE TABLE dm.dm_account_turnover_f (
    on_date DATE,
    account_rk NUMERIC,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8)
);

--ПРОЦЕДУРА РАСЧЕТА ВИТРИНЫ ОБОРОТОВ
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
BEGIN
    -- Запись времени начала расчета
    start_time := NOW();

    -- Удаление данных за указанную дату, если они уже существуют
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;

    BEGIN
	
    -- Вставка данных по проводкам
    INSERT INTO dm.dm_account_turnover_f (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    
	SELECT
		COALESCE (ct.on_date, dt.on_date) AS dt,
		COALESCE (ct.account_rk, dt.account_rk) AS account,
		ct.credit_amount,
		ct.credit_amount_rub,
		dt.debet_amount,
		dt.debet_amount_rub
	FROM
	(SELECT
        i_OnDate AS on_date,
        p.credit_account_rk AS account_rk,
        SUM(p.credit_amount) AS credit_amount,
        SUM(p.credit_amount) * COALESCE(e.reduced_cource, 1) AS credit_amount_rub
    FROM
        ds.ft_posting_f p
    LEFT JOIN
        ds.md_account_d a ON p.credit_account_rk = a.account_rk
    LEFT JOIN
        ds.md_exchange_rate_d e ON a.currency_rk = e.currency_rk AND i_OnDate BETWEEN e.data_actual_date AND e.data_actual_end_date
    WHERE
        p.oper_date = i_OnDate
    GROUP BY
        p.credit_account_rk, e.reduced_cource
		)ct
FULL JOIN 
    (SELECT
        i_OnDate AS on_date,
        p.debet_account_rk AS account_rk,
        SUM(p.debet_amount) AS debet_amount,
        SUM(p.debet_amount) * COALESCE(e.reduced_cource, 1) AS debet_amount_rub
    FROM
        ds.ft_posting_f p
    LEFT JOIN
        ds.md_account_d a ON p.debet_account_rk = a.account_rk
    LEFT JOIN
        ds.md_exchange_rate_d e ON a.currency_rk = e.currency_rk AND i_OnDate BETWEEN e.data_actual_date AND e.data_actual_end_date
    WHERE
        p.oper_date = i_OnDate
    GROUP BY
        p.debet_account_rk, e.reduced_cource
		)dt
	ON ct.on_date=dt.on_date AND ct.account_rk=dt.account_rk;

-- Запись времени окончания расчета и результата
        end_time := NOW();
        INSERT INTO logs.log_info (start_time, end_time, table_name, result)
        VALUES (start_time, end_time, 'dm.dm_account_turnover_f', 'Success');

    EXCEPTION
        WHEN OTHERS THEN
            -- Запись времени окончания расчета и результата в случае ошибки
            end_time := NOW();
            INSERT INTO logs.log_info (start_time, end_time, table_name, result)
            VALUES (start_time, end_time, 'dm.dm_account_turnover_f', 'Failed');
            RAISE;
    END;
END;
$$;

--расчет оборотов за январь
DO $$
DECLARE
    calc_date DATE;
BEGIN
    FOR calc_date IN
        (SELECT generate_series('2018-01-01'::DATE, '2018-01-31'::DATE, '1 day'::INTERVAL))
    LOOP
        call ds.fill_account_turnover_f(calc_date);
    END LOOP;
END;
$$;

--SELECT * FROM dm.dm_account_turnover_f;

--SELECT * FROM logs.log_info order by start_time desc;


CREATE TABLE dm.dm_account_balance_f (
    on_date DATE,
    account_rk NUMERIC,
    balance_out NUMERIC(23, 8),
    balance_out_rub NUMERIC(23, 8)
);

--расчет остатка на 31.12.2017
INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
SELECT
    '2017-12-31'::DATE AS on_date,
    f.account_rk,
    f.balance_out,
    f.balance_out * COALESCE(e.reduced_cource, 1) AS balance_out_rub
FROM
    ds.ft_balance_f f
LEFT JOIN
    ds.md_exchange_rate_d e ON f.currency_rk = e.currency_rk AND '2017-12-31' BETWEEN e.data_actual_date AND e.data_actual_end_date;


--ПРОЦЕДУРА РАСЧЕТА ВИТРИНЫ ОСТАТКОВ
CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    prev_date DATE;
BEGIN
    -- Запись времени начала расчета
    start_time := NOW();

    -- Удаление данных за указанную дату, если они уже существуют
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

    -- Определение предыдущей даты
    prev_date := i_OnDate - INTERVAL '1 day';

    BEGIN
        -- Вставка данных для активных счетов
        INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
        SELECT
            i_OnDate AS on_date,
            a.account_rk,
            COALESCE((SELECT balance_out FROM dm.dm_account_balance_f WHERE on_date = prev_date AND account_rk = a.account_rk), 0)
            + COALESCE((SELECT SUM(debet_amount) FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
            - COALESCE((SELECT SUM(credit_amount) FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0) AS balance_out,
			
            COALESCE((SELECT balance_out_rub FROM dm.dm_account_balance_f WHERE on_date = prev_date AND account_rk = a.account_rk), 0)
            + COALESCE((SELECT SUM(debet_amount_rub) FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
            - COALESCE((SELECT SUM(credit_amount_rub) FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0) AS balance_out_rub
        FROM
            ds.md_account_d a
        WHERE
            a.char_type = 'А' 
			AND i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date;

        -- Вставка данных для пассивных счетов
        INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
        SELECT
            i_OnDate AS on_date,
            a.account_rk,
            COALESCE((SELECT balance_out FROM dm.dm_account_balance_f WHERE on_date = prev_date AND account_rk = a.account_rk), 0)
            - COALESCE((SELECT SUM(debet_amount) FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
            + COALESCE((SELECT SUM(credit_amount) FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0) AS balance_out,
			
            COALESCE((SELECT balance_out_rub FROM dm.dm_account_balance_f WHERE on_date = prev_date AND account_rk = a.account_rk), 0)
            - COALESCE((SELECT SUM(debet_amount_rub) FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0)
            + COALESCE((SELECT SUM(credit_amount_rub) FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate AND account_rk = a.account_rk), 0) AS balance_out_rub
        FROM
            ds.md_account_d a
        WHERE
            a.char_type = 'П' 
			AND i_OnDate BETWEEN a.data_actual_date AND a.data_actual_end_date;

        -- Запись времени окончания расчета и результата
        end_time := NOW();
        INSERT INTO logs.log_info (start_time, end_time, table_name, result)
        VALUES (start_time, end_time, 'dm.dm_account_balance_f', 'Success');

    EXCEPTION
        WHEN OTHERS THEN
            -- Запись времени окончания расчета и результата в случае ошибки
            end_time := NOW();
            INSERT INTO logs.log_info (start_time, end_time, table_name, result)
            VALUES (start_time, end_time, 'dm.dm_account_balance_f', 'Failed');
            RAISE;
    END;
END;
$$;

--расчет витрины остатков за январь
DO $$
DECLARE
    calc_date DATE;
BEGIN
    FOR calc_date IN
        (SELECT generate_series('2018-01-01'::DATE, '2018-01-31'::DATE, '1 day'::INTERVAL))
    LOOP
        CALL ds.fill_account_balance_f(calc_date);
    END LOOP;
END;
$$;

--SELECT * FROM dm.dm_account_balance_f;  

--SELECT * FROM logs.log_info order by start_time desc;
