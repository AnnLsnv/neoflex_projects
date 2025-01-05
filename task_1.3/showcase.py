from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from psycopg2 import Error

import logger


def export_showcase_to_csv(output_file: str, showcase_db_data: dict, log: logger.Logger):
    export_start = datetime.now()
    try:
        engine = create_engine(
            "{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}".format(
                dialect="postgresql",
                driver="psycopg2",
                username=showcase_db_data["user"],
                password=showcase_db_data["password"],
                host=showcase_db_data["ip"],
                port=5432,
                database=showcase_db_data["database"]
            )
        )
        with engine.connect() as connection:
            query = f"SELECT * FROM {showcase_db_data["export_table"]}"
            df = pd.read_sql_query(query, connection)
            df.to_csv(output_file, index=False)
            export_end = datetime.now()
            log.success(showcase_db_data["export_table"], export_start.strftime("%Y-%m-%d %H:%M:%S"), export_end.strftime("%Y-%m-%d %H:%M:%S"))
    except (Exception, Error) as error:
        print(f"Ошибка экспорта витрины {showcase_db_data["table"]}: {error}")
        export_end = datetime.now()
        log.failed(showcase_db_data["export_table"], export_start.strftime("%Y-%m-%d %H:%M:%S"), export_end.strftime("%Y-%m-%d %H:%M:%S"))


def import_showcase_from_csv(input_file: str, showcase_db_data: dict, log: logger.Logger):
    import_start = datetime.now()
    try:
        engine = create_engine(
            "{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}".format(
                dialect="postgresql",
                driver="psycopg2",
                username=showcase_db_data["user"],
                password=showcase_db_data["password"],
                host=showcase_db_data["ip"],
                port=5432,
                database=showcase_db_data["database"]
            )
        )
        with engine.connect() as connection:
            df = pd.read_csv(input_file)
            df.to_sql(showcase_db_data["import_table"], connection, if_exists='replace', index=False, schema='dm')
            import_end = datetime.now()
            log.success(showcase_db_data["import_table"], import_start.strftime("%Y-%m-%d %H:%M:%S"), import_end.strftime("%Y-%m-%d %H:%M:%S"))
    except (Exception, Error) as error:
        print(f"Ошибка импорта витрины {showcase_db_data["import_table"]}: {error}")
        import_end = datetime.now()
        log.failed(showcase_db_data["import_table"], import_start.strftime("%Y-%m-%d %H:%M:%S"), import_end.strftime("%Y-%m-%d %H:%M:%S"))