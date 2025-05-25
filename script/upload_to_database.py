import os
import sys
import glob
import configparser
import logging
import psycopg2
from datetime import datetime
import pandas as pd


config = configparser.ConfigParser()
dir_script = os.path.dirname(__file__)
dir_project = os.path.dirname(dir_script)
dir_config = os.path.join(dir_project, 'config')
config.read(os.path.join(dir_config, 'config.ini'))
dir_data = os.path.join(dir_project, eval(config['product']['folder_data']))
dir_log = os.path.join(dir_project, eval(config['logger']['folder_log']))

os.makedirs(dir_log, exist_ok=True)
file_log = os.path.join(dir_log, eval(config['logger']['upload_to_database_log']))
date_load = datetime.today().date()
logging.basicConfig(level=logging.INFO, filename=file_log, filemode='w')
load_file = glob.glob('*.csv', root_dir=dir_data)
if not load_file:
    logging.info(f'{date_load} : no data')
    sys.exit()
df = pd.DataFrame()
for file in load_file:
    data = pd.read_csv(os.path.join(dir_data, file))
    df = pd.concat([df, data], ignore_index=True)
connect = eval(config['database']['connect'])
cursor = connect.cursor()
connect.autocommit = True
list_values = [tuple(v) for _, v in df.iterrows()]
str_values = str(list_values)
values = str_values[1:-1]
header = ', '.join([i for i in list(df)])
request = f"insert into shop_cash ({header}) values {values} ON CONFLICT DO NOTHING"
cursor.execute(request)
cursor.close()
connect.close()
logging.info(f'{date_load} : LOAD OK')
