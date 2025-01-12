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


def load_product(df: pd.DataFrame, connection):
    # Преобразование данных в список строк для вставки
    rows = []
    data = df.to_dict('records')
    for row in data:
        rows.append(
            f"({row['product_rk']}, '{row['product_name']}', '{row['effective_from_date']}'::date, '{row['effective_to_date']}'::date)"
        )

    val = ', '.join(rows)

    # Формирование SQL-запроса для вставки данных
    insert_query = sql.SQL(f"""
        INSERT INTO rd.product (
            product_rk, product_name, effective_from_date, effective_to_date
        ) VALUES {val}
    """)

    cursor = connection.cursor()
    try:
        cursor.execute(sql.SQL('TRUNCATE TABLE rd.product'))
        cursor.execute(insert_query)
    except (Exception, Error) as error:
        print("Ошибка загрузки данных для RD.PRODUCT", error)
    finally:
        cursor.close()


df = pd.read_csv('data/loan_holiday_info/product_info.csv', delimiter=",", encoding='cp1251')
conn = create_new_connection("localhost", "postgres", "postgres", "dwh")
load_product(df, connection=conn)
close_connection(conn)
