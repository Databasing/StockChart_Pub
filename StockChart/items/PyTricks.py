'''
Python tricks.
'''
import sys
import sqlite3
from iexfinance.stocks import Stock, get_historical_data
from datetime import timedelta, date
import time
from lib.create_database import create_db
from lib.formula import insert_sma, insert_ema

'''
Print a dictionary vertically.
'''

#Create dictionary.
a = Stock('fb').get_quote()

#Turn key and value to it's own list.
key = list(a.keys())
values = list(a.values())

#Loop. Turn all items to string to print.
cnt = 0

while cnt < len(a):
    print(str(cnt) + '. ' + str(key[cnt]) + ': ' + str(values[cnt]))
    cnt += 1

