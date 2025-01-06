from psycopg2 import sql, Error

def prepare_schema(connection):
    print("Подготовка схемы и таблиц в PostgreSQL")
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        create_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS ds")
        cursor.execute(create_schema_query)
    except (Exception, Error) as error:
        print("Ошибка создания схемы PostgreSQL", error)
        raise error

    # Таблица DS.FT_BALANCE_F
    ft_balance_query = sql.SQL('CREATE TABLE IF NOT EXISTS ds.FT_BALANCE_F ('
        'on_date DATE NOT NULL,'
        'account_rk INTEGER NOT NULL,'
        'currency_rk INTEGER,'
        'balance_out FLOAT,'
        'PRIMARY KEY (on_date, account_rk)'
    ')')
        
    # Таблица DS.FT_POSTING_F
    ft_posting_query = sql.SQL('CREATE TABLE IF NOT EXISTS ds.FT_POSTING_F ('
        'oper_date DATE NOT NULL,'
        'credit_account_rk INTEGER NOT NULL,'
        'debet_account_rk INTEGER NOT NULL,'
        'credit_amount FLOAT,'
        'debet_amount FLOAT'
    ')')

    # Таблица DS.MD_ACCOUNT_D
    md_posting_query = sql.SQL('CREATE TABLE IF NOT EXISTS ds.MD_ACCOUNT_D ('
        'data_actual_date DATE NOT NULL,'
        'data_actual_end_date DATE NOT NULL,'
        'account_rk INTEGER NOT NULL,'
        'account_number VARCHAR(20) NOT NULL,'
        'char_type VARCHAR(1) NOT NULL,'
        'currency_rk INTEGER NOT NULL,'
        'currency_code VARCHAR(3) NOT NULL,'
        'PRIMARY KEY (data_actual_date, account_rk )'

    ')')

    # Таблица DS.MD_CURRENCY_D
    md_currency_query = sql.SQL('CREATE TABLE IF NOT EXISTS ds.MD_CURRENCY_D ('
        'currency_rk INTEGER NOT NULL,'
        'data_actual_date DATE NOT NULL,'
        'data_actual_end_date DATE,'
        'currency_code VARCHAR(3),'
        'code_iso_char VARCHAR(3),'
        'PRIMARY KEY (currency_rk,data_actual_date)'
    ')')

    # Таблица DS.MD_EXCHANGE_RATE_D
    md_exchange_query = sql.SQL('CREATE TABLE IF NOT EXISTS ds.MD_EXCHANGE_RATE_D ('
        'data_actual_date DATE NOT NULL,'
        'data_actual_end_date DATE,'
        'currency_rk INTEGER NOT NULL,'
        'reduced_cource FLOAT,'
        'code_iso_num VARCHAR(3),'
        'PRIMARY KEY (data_actual_date, currency_rk)'
    ')')

    # Таблица DS.MD_LEDGER_ACCOUNT_S
    md_ledger_accounts_query = sql.SQL('CREATE TABLE IF NOT EXISTS ds.MD_LEDGER_ACCOUNT_S ('
        'chapter CHAR(1),'
        'chapter_name VARCHAR(16),'
        'section_number INTEGER,'
        'section_name VARCHAR(22),'
        'subsection_name VARCHAR(21),'
        'ledger1_account INTEGER,'
        'ledger1_account_name VARCHAR(47),'
        'ledger_account INTEGER NOT NULL,'
        'ledger_account_name VARCHAR(153),'
        'characteristic VARCHAR(1),'
        'is_resident INTEGER,'
        'is_reserve INTEGER,'
        'is_reserved INTEGER,'
        'is_loan INTEGER,'
        'is_reserved_assets INTEGER,'
        'is_overdue INTEGER,'
        'is_interest INTEGER,'
        'pair_account VARCHAR(5),'
        'start_date DATE NOT NULL,'
        'end_date DATE,'
        'is_rub_only INTEGER,'
        'min_term INTEGER,'
        'min_term_measure VARCHAR(1),'
        'max_term INTEGER,'
        'max_term_measure VARCHAR(1),'
        'ledger_acc_full_name_translit VARCHAR(1),'
        'is_revaluation VARCHAR(1),'
        'is_correct VARCHAR(1),'
        'PRIMARY KEY (ledger_account,start_date)'
    ')')
    
    try:
        cursor.execute(ft_balance_query)
        cursor.execute(ft_posting_query)
        cursor.execute(md_posting_query)
        cursor.execute(md_currency_query)
        cursor.execute(md_exchange_query)
        cursor.execute(md_ledger_accounts_query)
    except (Exception, Error) as error:
        print("Ошибка создания таблицы PostgreSQL", error)
        cursor.close()
        raise error

    cursor.close()
