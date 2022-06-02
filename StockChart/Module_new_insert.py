import sys
import sqlite3
import random
#from iexfinance.stocks import Stock, get_historical_data
from datetime import timedelta, date
import time
from lib.create_database import create_db
from lib.data_insert import insert_new_stocks, insert_new_fact_data
from lib.formula import insert_sma, insert_ema, insert_bollinger
from lib.func import update_dim_status, stock_exist_check


#DELETES AND RECREATE DB FOR TESTING
create_db()

'''
Insert new stock module.
'''
master_symbol = ['amd','msft','nvda','t','nflx','wm','lamr','fb','twtr','wmt','aapl','v','PYPL','mcd','roku','jnj','pru','fdx','dis','intu','adbe','atvi','mu','tgt','lmt','tsla','ea','td']

#master_symbol = ['amd','msft','nvda','t','nflx','wm','lamr','fb','twtr','wmt']
#master_symbol = ['aapl','v','PYPL','mcd','roku','jnj','pru','fdx','dis','intu']
#master_symbol = ['adbe','atvi','mu','tgt','lmt','tsla','ea']
#master_symbol = ['adbe','atvi','mu']


#master_symbol = ['td']


for symbol in master_symbol:

    symbol = symbol.lower()

    #Check if stock exist in api. Grab stock name.
    name = stock_exist_check(symbol)

    #Set moving average period.
    period = 20

    #Insert new stocks.
    insert_new_stocks(symbol,name)

    #Populate data_fact. Get stock_id for subsequent functions.
    stock_id = insert_new_fact_data(symbol)

    #Populate SMA.
    insert_sma(stock_id,period)

    #Populate EMA.
    insert_ema(stock_id,period)

    #Populate Bollinger Bands.
    insert_bollinger(stock_id,period)

    #Populate dim_status.
    update_dim_status(stock_id)

    print(symbol)

