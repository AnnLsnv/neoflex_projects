import os
import sys
import logger
import getopt
import create_table
import pandas as pd
from dotenv import load_dotenv
from connection import create_new_connection, close_connection
import loaders as ld
import showcase as sc

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

conn = create_new_connection(ip=os.environ.get('POSTGRES_IP', 'localhost'),
                             user=os.environ.get('POSTGRES_USER', 'postgres'),
                             password=os.environ.get('POSTGRES_PASSWORD', 'passwd'),
                             database=os.environ.get('POSTGRES_DATABASE', 'etldb'))

connection_dict = {
    "user": os.environ.get('POSTGRES_USER', 'postgres'),
    "ip": os.environ.get('POSTGRES_IP', 'localhost'),
    "password": os.environ.get('POSTGRES_PASSWORD', 'passwd'),
    "database": os.environ.get('POSTGRES_DATABASE', 'etldb'),
    "export_table": "dm.dm_f101_round_f",
    "import_table": "dm_f101_round_f2",
}

if conn is None:
    print("Не удалось открыть соединение с СУБД")
else:
    lg = logger.Logger(conn)

    # Задаем флаги для команд
    argumentList = sys.argv[1:]
    options = "l:e:i:"
    long_options = ["load_data=", "showcase_export=", "showcase_import="]

    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        for currentArgument, currentValue in arguments:
            print(currentArgument, currentValue)
            if currentArgument in ("-l", "--load_data"):
                create_table.prepare_schema(conn)
                filename = currentValue
                df = pd.read_csv(filename, delimiter=";")
                table_name = filename.split(os.path.sep)[len(filename.split(os.path.sep))-1].split(".")[0]
                match table_name:
                    case 'ft_balance_f':
                        ld.load_ft_balance_f(df, lg, conn)
                    case 'ft_posting_f':
                        ld.load_ft_posting_f(df, lg, conn)
                    case 'md_account_d':
                        ld.load_md_account_d(df, lg, conn)
                    case 'md_currency_d':
                        ld.load_md_currency_d(df, lg, conn)
                    case 'md_exchange_rate_d':
                        ld.load_md_exchange_rate_d(df, lg, conn)
                    case 'md_ledger_account_s':
                        ld.load_md_ledger_account_s(df, lg, conn)

            elif currentArgument in ("-e", "--showcase_export"):
                sc.export_showcase_to_csv(currentValue, connection_dict, lg)

            elif currentArgument in ("-i", "--showcase_import"):
                sc.import_showcase_from_csv(currentValue, connection_dict, lg)
    except getopt.error:
        print("usage:\nmain.py -l|--load_data <data_file>\nmain.py -e|--showcase_export <out_file>\nmain.py -i|--showcase_import <in_file>")

    lg.close()
    close_connection(conn)