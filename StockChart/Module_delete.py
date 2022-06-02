import sys
import sqlite3
from iexfinance.stocks import Stock, get_historical_data
from datetime import timedelta, date
import time
from lib.data_delete import delete_stock
from lib.func import get_stock_id

start = time.time()

'''
Delete stock module.
'''
master_stock = ['SQ']

for stock in master_stock:

    #User Input.
    input_symbol = stock.lower()

    #Grab stock ID.
    stock_id = get_stock_id(input_symbol)

    #Delete stock.
    delete_stock(stock_id)

end = time.time()
print(end - start)
