--1.3
CREATE TABLE dm.dm_f101_round_f (
    from_date DATE,
    to_date DATE,
    chapter CHAR(1),
    ledger_account CHAR(5),
    characteristic CHAR(1),
    balance_in_rub NUMERIC(23,8),
    r_balance_in_rub NUMERIC(23,8),
    balance_in_val NUMERIC(23,8),
    r_balance_in_val NUMERIC(23,8),
    balance_in_total NUMERIC(23,8),
    r_balance_in_total NUMERIC(23,8),
    turn_deb_rub NUMERIC(23,8),
    r_turn_deb_rub NUMERIC(23,8),
    turn_deb_val NUMERIC(23,8),
    r_turn_deb_val NUMERIC(23,8),
    turn_deb_total NUMERIC(23,8),
    r_turn_deb_total NUMERIC(23,8),
    turn_cre_rub NUMERIC(23,8),
    r_turn_cre_rub NUMERIC(23,8),
    turn_cre_val NUMERIC(23,8),
    r_turn_cre_val NUMERIC(23,8),
    turn_cre_total NUMERIC(23,8),
    r_turn_cre_total NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8),
    r_balance_out_rub NUMERIC(23,8),
    balance_out_val NUMERIC(23,8),
    r_balance_out_val NUMERIC(23,8),
    balance_out_total NUMERIC(23,8),
    r_balance_out_total NUMERIC(23,8)
);

--РАСЧЕТ ФОРМЫ 101
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    fr_date DATE;
    t_date DATE;
    prev_date DATE;
BEGIN
    -- Запись времени начала расчета
    start_time := NOW();

	-- Удаление данных за указанную дату, если они уже существуют
    DELETE FROM dm.dm_f101_round_f WHERE from_date = (i_OnDate - INTERVAL '1 month') AND to_date = (i_OnDate - INTERVAL '1 day');

    -- Определение периода отчета
    fr_date := i_OnDate - INTERVAL '1 month';
    t_date := i_OnDate - INTERVAL '1 day';
    prev_date := fr_date - INTERVAL '1 day';

    BEGIN
        -- Вставка данных в витрину
        INSERT INTO dm.dm_f101_round_f (
            from_date, to_date, chapter, ledger_account, characteristic,
            balance_in_rub, r_balance_in_rub, balance_in_val, r_balance_in_val,
            balance_in_total, r_balance_in_total, turn_deb_rub, r_turn_deb_rub,
            turn_deb_val, r_turn_deb_val, turn_deb_total, r_turn_deb_total,
            turn_cre_rub, r_turn_cre_rub, turn_cre_val, r_turn_cre_val,
            turn_cre_total, r_turn_cre_total, balance_out_rub, r_balance_out_rub,
            balance_out_val, r_balance_out_val, balance_out_total, r_balance_out_total
        )
        SELECT
            i_OnDate - INTERVAL '1 month' AS from_date,
            i_OnDate - INTERVAL '1 day' AS to_date,
            l.chapter,
            SUBSTRING(a.account_number FROM 1 FOR 5) AS ledger_account,
            a.char_type AS characteristic,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END) AS balance_in_rub,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END)/1000 AS r_balance_in_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END) AS balance_in_val,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_prev.balance_out_rub ELSE 0 END)/1000 AS r_balance_in_val,
            SUM(b_prev.balance_out_rub) AS balance_in_total,
            SUM(b_prev.balance_out_rub)/1000 AS r_balance_in_total,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END)/1000 AS r_turn_deb_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END)/1000 AS r_turn_deb_val,
            SUM(t.debet_amount_rub) AS turn_deb_total,
            SUM(t.debet_amount_rub)/1000 AS r_turn_deb_total,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END)/1000 AS r_turn_cre_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END)/1000 AS r_turn_cre_val,
            SUM(t.credit_amount_rub) AS turn_cre_total,
            SUM(t.credit_amount_rub)/1000 AS r_turn_cre_total,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_curr.balance_out_rub ELSE 0 END) AS balance_out_rub,
            SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_curr.balance_out_rub ELSE 0 END)/1000 AS r_balance_out_rub,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_curr.balance_out_rub ELSE 0 END) AS balance_out_val,
            SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_curr.balance_out_rub ELSE 0 END)/1000 AS r_balance_out_val,
            SUM(b_curr.balance_out_rub) AS balance_out_total,
            SUM(b_curr.balance_out_rub)/1000 AS r_balance_out_total
        FROM
            ds.md_account_d a
        JOIN
            ds.md_ledger_account_s l ON SUBSTRING(a.account_number FROM 1 FOR 5) = l.ledger_account::text
        LEFT JOIN
            dm.dm_account_balance_f b_prev ON a.account_rk = b_prev.account_rk AND b_prev.on_date = prev_date
        LEFT JOIN
            dm.dm_account_balance_f b_curr ON a.account_rk = b_curr.account_rk AND b_curr.on_date = t_date
        LEFT JOIN
            dm.dm_account_turnover_f t ON a.account_rk = t.account_rk AND t.on_date BETWEEN fr_date AND t_date
        WHERE
            (fr_date BETWEEN a.data_actual_date AND a.data_actual_end_date) 
			AND (t_date BETWEEN a.data_actual_date AND a.data_actual_end_date)
        GROUP BY
            l.chapter, SUBSTRING(a.account_number FROM 1 FOR 5), a.char_type;

        -- Запись времени окончания расчета и результата
        end_time := NOW();
        INSERT INTO logs.log_info (start_time, end_time, table_name, result)
        VALUES (start_time, end_time, 'dm.dm_f101_round_f', 'Success');

    EXCEPTION
        WHEN OTHERS THEN
            -- Запись времени окончания расчета и результата в случае ошибки
            end_time := NOW();
            INSERT INTO logs.log_info (start_time, end_time, table_name, result)
            VALUES (start_time, end_time, 'dm.dm_f101_round_f', 'Failed');
            RAISE;
    END;
END;
$$;

--расчет ф101 за январь 2018 
DO $$
DECLARE
    i_OnDate DATE;
BEGIN
    i_OnDate := '2018-02-01'::DATE;
    CALL dm.fill_f101_round_f(i_OnDate);
END;
$$;

--SELECT * FROM dm.dm_f101_round_f;

--SELECT * FROM logs.log_info order by start_time desc;