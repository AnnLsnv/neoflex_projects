import os
import sys
import logger
import getopt
import create_table
import pandas as pd
from dotenv import load_dotenv
from connection import create_new_connection, close_connection
import loaders as ld

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

conn = create_new_connection(ip=os.environ.get('POSTGRES_IP', 'localhost'),
                             user=os.environ.get('POSTGRES_USER', 'postgres'),
                             password=os.environ.get('POSTGRES_PASSWORD', 'passwd'),
                             database=os.environ.get('POSTGRES_DATABASE', 'etldb'))
if conn is None:
    print("Не удалось открыть соединение с СУБД")
else:
    lg = logger.Logger(conn)

    # Задаем флаги для команд
    argumentList = sys.argv[1:]
    options = "l:"
    long_options = ["load_data="]

    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
        for currentArgument, currentValue in arguments:
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

    except getopt.error:
        print("usage:\nmain.py -l|--load_data <data_file>")

    lg.close()
    close_connection(conn)