import os
import sys
import glob
import random
import configparser
import logging
from datetime import datetime
import pandas as pd


# маленький комментарий для гит
config = configparser.ConfigParser()
config.read('../config/config.ini')
folder_data = eval(config['product']['folder_data'])
folder_log = eval(config['logger']['folder_log'])
os.makedirs(folder_data, exist_ok=True)
os.makedirs(folder_log, exist_ok=True)
delete_file = glob.glob('*.*', root_dir=folder_data)
for file in delete_file:
    path = os.path.join(folder_data, file)
    os.remove(path)
file_log = os.path.join(folder_log, eval(config['logger']['shop_generator_log']))
date_generate = datetime.today().date()
logging.basicConfig(level=logging.INFO, filename=file_log, filemode='w')
if datetime.weekday(date_generate) not in eval(config['product']['working_days_week']):
    logging.info(f'{date_generate} : no data')
    sys.exit()
product_list = eval(config['product']['product_list'])
count_shop = eval(config['product']['count_shop'])
count_cash = eval(config['product']['count_cash'])
shop_list = [[shop, cash] for shop in range(1, count_shop + 1) for cash in range(1, count_cash + 1)]
shop_random_list = random.sample(shop_list, random.randint(1, len(shop_list)))
for shop_cash in shop_random_list:
    list_product = random.sample(product_list, random.randint(1, len(product_list)))
    list_cash = [[date_generate] + [f's{shop_cash[0]}_c{shop_cash[1]}'] + i + [random.randint(1, 5)]
                 + [random.choice([0, 5, 10])] for i in list_product]
    df_cash = pd.DataFrame(list_cash, columns=['dates', 'doc_id', 'category', 'item', 'price', 'amount', 'discount'])
    df_cash.to_csv(os.path.join(folder_data, f'shop{shop_cash[0]}_cash{shop_cash[1]}.csv'), encoding='utf-8', index=False)
    logging.info(f'create file: shop{shop_cash[0]}_cash{shop_cash[1]}.csv')
logging.info(f'{date_generate} : GENERATE OK')
