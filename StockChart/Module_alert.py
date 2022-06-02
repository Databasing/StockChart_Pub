import sys
import sqlite3
from iexfinance.stocks import Stock, get_historical_data
from datetime import timedelta, date
import time
from lib.create_database import create_db
from lib.data_insert import insert_new_stocks, insert_new_fact_data
from lib.formula import insert_sma, insert_ema, insert_bollinger
from lib.func import get_stock_list
from lib.alert import alert_stock
import os

start = time.time()

'''
Send alerts for current stocks.
'''
#Clear alert folder.
file_list = os.listdir('alert')
for f in file_list:
    os.remove(os.path.join('alert', f))

#Grab ID and symbol of all current stocks.
stock_info_list = get_stock_list()

#Set variables.
id_list = [x[0] for x in stock_info_list]
symbol_list = [x[1] for x in stock_info_list]

#Alert for stocks.
alert_stock(id_list,symbol_list)

end = time.time()
print(end - start)

