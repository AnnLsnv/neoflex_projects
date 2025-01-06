import time
from datetime import datetime
import pandas as pd
from pandas.io.formats.format import math
from psycopg2 import sql, Error

import logger

_base_format = "%Y-%m-%d"

def load_ft_balance_f(df: pd.DataFrame, log: logger.Logger, connection):
    load_start = datetime.now()
    
    df['ON_DATE'] = pd.to_datetime(df['ON_DATE'], format='mixed')
    df["ON_DATE"] =  df["ON_DATE"].dt.strftime(_base_format)
    
    rows = []
    data = df.to_dict('records')
    for row in data:
        rows.append(f"(\'{row["ON_DATE"]}\'::date, {row["ACCOUNT_RK"]}, {row["CURRENCY_RK"]}, {row["BALANCE_OUT"]})")
        
    val = ', '.join(rows)
    
    insert_query = sql.SQL(f"INSERT INTO ds.ft_balance_f(ON_DATE,ACCOUNT_RK,CURRENCY_RK,BALANCE_OUT) VALUES {val} "
                           "ON CONFLICT (ON_DATE, ACCOUNT_RK) DO UPDATE "
                           "SET CURRENCY_RK = excluded.CURRENCY_RK, BALANCE_OUT = excluded.BALANCE_OUT")
    cursor = connection.cursor()
    try:
        #print(insert_query.as_string(cursor))
        cursor.execute(insert_query)
        time.sleep(5)
        load_end = datetime.now()
        log.success("ft_balance_f", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    except (Exception, Error) as error:
        print("Ошибка загрузки данных для DS.FT_BALANCE_F", error)
        load_end = datetime.now()
        log.failed("ft_balance_f", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    finally:
        cursor.close()
        
def load_ft_posting_f(df: pd.DataFrame, log: logger.Logger, connection):
    load_start = datetime.now()
    df['OPER_DATE'] = pd.to_datetime(df['OPER_DATE'], format='mixed')
    df["OPER_DATE"] =  df["OPER_DATE"].dt.strftime(_base_format)
    
    rows = []
    data = df.to_dict('records')
    for row in data:
        rows.append(f"(\'{row["OPER_DATE"]}\'::date, {row["CREDIT_ACCOUNT_RK"]}, {row["DEBET_ACCOUNT_RK"]}, {row["CREDIT_AMOUNT"]}, {row["DEBET_AMOUNT"]})")
        
    val = ', '.join(rows)
    
    insert_query = sql.SQL(f"INSERT INTO ds.ft_posting_f(OPER_DATE,CREDIT_ACCOUNT_RK,DEBET_ACCOUNT_RK,CREDIT_AMOUNT,DEBET_AMOUNT) VALUES {val}")
    cursor = connection.cursor()
    try:
        # Очистка таблицы от предыдущих данных
        truncate_query = sql.SQL("TRUNCATE TABLE ds.ft_posting_f")
        cursor.execute(truncate_query)
        cursor.execute(insert_query)
        time.sleep(5)
        load_end = datetime.now()
        log.success("ft_posting_f", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    except (Exception, Error) as error:
        print("Ошибка загрузки данных для ds.ft_posting_f", error)
        load_end = datetime.now()
        log.failed("ft_posting_f", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    finally:
        cursor.close()
        

def load_md_account_d(df: pd.DataFrame, log: logger.Logger, connection):
    load_start = datetime.now()
    df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='mixed')
    df["DATA_ACTUAL_DATE"] =  df["DATA_ACTUAL_DATE"].dt.strftime(_base_format)
    
    df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='mixed')
    df["DATA_ACTUAL_END_DATE"] =  df["DATA_ACTUAL_END_DATE"].dt.strftime(_base_format)
    
    rows = []
    data = df.to_dict('records')
    for row in data:
        rows.append(f"(\'{row["DATA_ACTUAL_DATE"]}\'::date, \'{row["DATA_ACTUAL_END_DATE"]}\'::date, {row["ACCOUNT_RK"]}, {row["ACCOUNT_NUMBER"]}, \'{row["CHAR_TYPE"]}\', {row["CURRENCY_RK"]}, {row["CURRENCY_CODE"]})")
        
    val = ', '.join(rows)
    
    insert_query = sql.SQL(f"INSERT INTO ds.md_account_d(DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, ACCOUNT_RK, ACCOUNT_NUMBER, CHAR_TYPE, CURRENCY_RK, CURRENCY_CODE) VALUES {val} "
                           "ON CONFLICT (DATA_ACTUAL_DATE, ACCOUNT_RK) DO UPDATE "
                           "SET DATA_ACTUAL_END_DATE = excluded.DATA_ACTUAL_END_DATE, "
                           "ACCOUNT_NUMBER = excluded.ACCOUNT_NUMBER, "
                           "CHAR_TYPE = excluded.CHAR_TYPE, "
                           "CURRENCY_RK = excluded.CURRENCY_RK, "
                           "CURRENCY_CODE = excluded.CURRENCY_CODE")
    cursor = connection.cursor()
    try:
        cursor.execute(insert_query)
        time.sleep(5)
        load_end = datetime.now()
        log.success("md_account_d", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    except (Exception, Error) as error:
        print("Ошибка загрузки данных для ds.md_account_d", error)
        load_end = datetime.now()
        log.failed("md_account_d", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    finally:
        cursor.close()

def load_md_currency_d(df: pd.DataFrame, log: logger.Logger, connection):
    load_start = datetime.now()
    df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='mixed')
    df["DATA_ACTUAL_DATE"] =  df["DATA_ACTUAL_DATE"].dt.strftime(_base_format)
    
    df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='mixed')
    df["DATA_ACTUAL_END_DATE"] =  df["DATA_ACTUAL_END_DATE"].dt.strftime(_base_format)
    
    rows = []
    data = df.to_dict('records')
    for row in data:
        rows.append(f"({row["CURRENCY_RK"]}, \'{row["DATA_ACTUAL_DATE"]}\'::date, \'{row["DATA_ACTUAL_END_DATE"]}\'::date,"
                    f"{int(row["CURRENCY_CODE"]) if not math.isnan(float(row["CURRENCY_CODE"])) else "NULL" }, \'{row["CODE_ISO_CHAR"] if row["CODE_ISO_CHAR"] != 'nan' else ''}\')")
        print(row["CURRENCY_CODE"])
        
    val = ', '.join(rows)
    
    insert_query = sql.SQL(f"INSERT INTO ds.md_currency_d(CURRENCY_RK,DATA_ACTUAL_DATE,DATA_ACTUAL_END_DATE,CURRENCY_CODE,CODE_ISO_CHAR) VALUES {val} "
                           "ON CONFLICT (CURRENCY_RK, DATA_ACTUAL_DATE) DO UPDATE "
                           "SET DATA_ACTUAL_END_DATE = excluded.DATA_ACTUAL_END_DATE, "
                           "CURRENCY_CODE = excluded.CURRENCY_CODE, "
                           "CODE_ISO_CHAR = excluded.CODE_ISO_CHAR")
    cursor = connection.cursor()
    try:
        cursor.execute(insert_query)
        time.sleep(5)
        load_end = datetime.now()
        log.success("md_currency_d", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    except (Exception, Error) as error:
        print("Ошибка загрузки данных для ds.md_currency_d", error)
        load_end = datetime.now()
        log.failed("md_currency_d", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    finally:
        cursor.close()

def load_md_exchange_rate_d(df: pd.DataFrame, log: logger.Logger, connection):
    load_start = datetime.now()
    df['DATA_ACTUAL_DATE'] = pd.to_datetime(df['DATA_ACTUAL_DATE'], format='mixed')
    df["DATA_ACTUAL_DATE"] =  df["DATA_ACTUAL_DATE"].dt.strftime(_base_format)
    
    df['DATA_ACTUAL_END_DATE'] = pd.to_datetime(df['DATA_ACTUAL_END_DATE'], format='mixed')
    df["DATA_ACTUAL_END_DATE"] =  df["DATA_ACTUAL_END_DATE"].dt.strftime(_base_format)
    
 # Удаление дубликатов
    df = df.drop_duplicates(subset=['DATA_ACTUAL_DATE', 'CURRENCY_RK'])

    rows = []
    data = df.to_dict('records')
    for row in data:
        rows.append(f"(\'{row["DATA_ACTUAL_DATE"]}\'::date, \'{row["DATA_ACTUAL_END_DATE"]}\'::date, {row["CURRENCY_RK"]}, {row["REDUCED_COURCE"]}, {row["CODE_ISO_NUM"]})")
        
    print(rows[:5])
    val = ', '.join(rows)
    
    insert_query = sql.SQL(f"INSERT INTO ds.md_exchange_rate_d(DATA_ACTUAL_DATE, DATA_ACTUAL_END_DATE, CURRENCY_RK, REDUCED_COURCE, CODE_ISO_NUM) VALUES {val} "
                           "ON CONFLICT (DATA_ACTUAL_DATE, CURRENCY_RK) DO UPDATE "
                           "SET DATA_ACTUAL_END_DATE = excluded.DATA_ACTUAL_END_DATE, "
                           "REDUCED_COURCE = excluded.REDUCED_COURCE, "
                           "CODE_ISO_NUM = excluded.CODE_ISO_NUM")
    cursor = connection.cursor()
    try:
        cursor.execute(insert_query)
        time.sleep(5)
        load_end = datetime.now()
        log.success("md_exchange_rate_d", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    except (Exception, Error) as error:
        print("Ошибка загрузки данных для ds.md_exchange_rate_d", error)
        load_end = datetime.now()
        log.failed("md_exchange_rate_d", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    finally:
        cursor.close()

def load_md_ledger_account_s(df: pd.DataFrame, log: logger.Logger, connection):
    load_start = datetime.now()
    df['START_DATE'] = pd.to_datetime(df['START_DATE'], format='mixed')
    df["START_DATE"] =  df["START_DATE"].dt.strftime(_base_format)
    
    df['END_DATE'] = pd.to_datetime(df['END_DATE'], format='mixed')
    df["END_DATE"] =  df["END_DATE"].dt.strftime(_base_format)
    
    rows = []
    data = df.to_dict('records')
    for row in data:
        rows.append("(" + ",".join([
            f"\'{row["CHAPTER"]}\'",
            f"\'{row["CHAPTER_NAME"]}\'",
            f"{row["SECTION_NUMBER"]}",
            f"\'{row["SECTION_NAME"]}\'",
            f"\'{row["SUBSECTION_NAME"]}\'",
            f"{row["LEDGER1_ACCOUNT"]}",
            f"\'{row["LEDGER1_ACCOUNT_NAME"]}\'",
            f"{row["LEDGER_ACCOUNT"]}",
            f"\'{row["LEDGER_ACCOUNT_NAME"]}\'",
            f"\'{row["CHARACTERISTIC"]}\'",
            f"\'{row["START_DATE"]}\'::date",
            f"\'{row["END_DATE"]}\'::date",
            ]) +")")
        
    val = ', '.join(rows)
    
    insert_query = sql.SQL("INSERT INTO ds.md_ledger_account_s (CHAPTER,"
                           "CHAPTER_NAME,"
                           "SECTION_NUMBER,"
                           "SECTION_NAME,"
                           "SUBSECTION_NAME,"
                           "LEDGER1_ACCOUNT,"
                           "LEDGER1_ACCOUNT_NAME,"
                           "LEDGER_ACCOUNT,"
                           "LEDGER_ACCOUNT_NAME,"
                           "CHARACTERISTIC,"
                           "START_DATE,"
                           f"END_DATE) VALUES {val} "
                           "ON CONFLICT (LEDGER_ACCOUNT, START_DATE) DO UPDATE "
                           "SET CHAPTER = excluded.CHAPTER,"
                           "SECTION_NUMBER = excluded.SECTION_NUMBER,"
                           "SUBSECTION_NAME = excluded.SUBSECTION_NAME,"
                           "LEDGER1_ACCOUNT = excluded.LEDGER1_ACCOUNT,"
                           "LEDGER1_ACCOUNT_NAME = excluded.LEDGER1_ACCOUNT_NAME,"
                           "LEDGER_ACCOUNT_NAME = excluded.LEDGER_ACCOUNT_NAME,"
                           "END_DATE = excluded.END_DATE")
    cursor = connection.cursor()
    try:
        cursor.execute(insert_query)
        time.sleep(5)
        load_end = datetime.now()
        log.success("md_ledger_account_s", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    except (Exception, Error) as error:
        print("Ошибка загрузки данных для ds.md_ledger_account_s", error)
        load_end = datetime.now()
        log.failed("md_ledger_account_s", load_start.strftime("%Y-%m-%d %H:%M:%S"), load_end.strftime("%Y-%m-%d %H:%M:%S"))
    finally:
        cursor.close()