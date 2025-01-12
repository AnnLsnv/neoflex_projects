import pandas as pd
import psycopg2
from psycopg2 import Error, sql

def create_new_connection(ip: str, user: str, password: str, database: str):
    try:
        connection = psycopg2.connect(user=user,
                                  password=password,
                                  host=ip,
                                  port="5432",
                                  database=database)
        connection.autocommit = True
        
        print("Соединение открыто")
    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
        return None

    return connection

def close_connection(connection):
    connection.close()


def load_deal_info(df: pd.DataFrame, connection):
    # Преобразование данных в список строк для вставки
    rows = []
    data = df.to_dict('records')
    for row in data:
        rows.append(
            f"({row['deal_rk']}, '{row['deal_num']}', '{row['deal_name']}', {row['deal_sum']}, "
            f"{row['client_rk']}, {row['account_rk']}, {row['agreement_rk']}, "
            f"'{row['deal_start_date']}'::date, {row['department_rk']}, {row['product_rk']}, "
            f"'{row['deal_type_cd']}', '{row['effective_from_date']}'::date, '{row['effective_to_date']}'::date)"
        )

    val = ', '.join(rows)

    # Формирование SQL-запроса для вставки данных
    insert_query = sql.SQL(f"""
        INSERT INTO rd.deal_info (
            deal_rk, deal_num, deal_name, deal_sum, client_rk, account_rk, agreement_rk,
            deal_start_date, department_rk, product_rk, deal_type_cd, effective_from_date, effective_to_date
        ) VALUES {val}
    """)

    cursor = connection.cursor()
    try:
        cursor.execute(insert_query)
    except (Exception, Error) as error:
        print("Ошибка загрузки данных для RD.DEAL_INFO", error)
    finally:
        cursor.close()


df = pd.read_csv('data/loan_holiday_info/deal_info.csv', delimiter=",", encoding='cp1251')
conn = create_new_connection("localhost", "postgres", "postgres", "dwh")
load_deal_info(df, connection=conn)
close_connection(conn)
