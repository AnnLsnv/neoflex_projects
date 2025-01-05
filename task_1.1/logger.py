from psycopg2 import sql, Error

class Logger:
    def __init__(self, connection):
        self.log_cursor = connection.cursor()
        self.table_name = "logs.log_info"
        init_log_schema_query = sql.SQL("CREATE SCHEMA IF NOT EXISTS logs")
        create_log_table_query = sql.SQL(f"CREATE TABLE IF NOT EXISTS {self.table_name}("
                                         "id SERIAL PRIMARY KEY,"
                                         "start_time TIMESTAMP NOT NULL,"
                                         "end_time TIMESTAMP NOT NULL,"
                                         "table_name VARCHAR(50) NOT NULL,"
                                         "result VARCHAR(50) NOT NULL CHECK(result IN ('Success', 'Failed'))"
            ")")
        try:
            self.log_cursor.execute(init_log_schema_query)
            self.log_cursor.execute(create_log_table_query)
        except (Exception, Error) as error:
            print(f"Ошибка создания таблицы {self.table_name}", error)
            raise error
        
    def close(self):
        self.log_cursor.close()
            
    def success(self, table, start_time, end_time):
        success_query = sql.SQL(f"INSERT INTO {self.table_name}(start_time, end_time, table_name, result) "
                                f"VALUES (\'{start_time}\'::timestamp,\'{end_time}\'::timestamp, \'{table}\', \'Success\' )")
        try:
            self.log_cursor.execute(success_query)
        except (Exception, Error) as error:
            print("Ошибка логгирования запроса в PostgreSQL: ", error)
        
    def failed(self, table, start_time, end_time):
        failed_query = sql.SQL(f"INSERT INTO {self.table_name}(start_time, end_time, table_name, result) "
                                f"VALUES (\'{start_time}\'::timestamp,\'{end_time}\'::timestamp, \'{table}\', \'Failed\' )")
        try:
            self.log_cursor.execute(failed_query)
        except (Exception, Error) as error:
            print("Ошибка логгирования запроса в PostgreSQL: ", error)