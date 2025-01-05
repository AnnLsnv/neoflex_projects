import psycopg2
from psycopg2 import Error

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
    print("Соединение с PostgreSQL закрыто")