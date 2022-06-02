'''
Insert data from API into tables.
'''
import sqlite3
from datetime import date
from lib.func import get_stock_id, get_db_conn_string, order_row_number, get_historical_data
import urllib3
from bs4 import BeautifulSoup
from json import loads
import re
import datetime

#Global variables.
conn_string = get_db_conn_string()
manager = urllib3.PoolManager()

#Inserts new stocks into dim_stock.
def insert_new_stocks(symbol, name):

    #Insert stock into dim_stock.
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("""insert into dim_stock (symbol, name)
                        values (?,?) """, [symbol, name])
    conn.commit()
    conn.close()


#Inserts data into fact_data. Takes return value of "insert_new_stocks".
def insert_new_fact_data(symbol):
    #Get relevant ID.
    stock_id = get_stock_id(symbol)

    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Load historical data. loading_flag == 0.
    get_historical_data(stock_id, symbol, 0)

    #Insert row_number.
    order_row_number(stock_id)

    #Insert dim_status.
    cursor.execute("""insert into dim_status (stock_id)
                        values (?) """, [stock_id])

    conn.commit()

    cursor.close()

    return stock_id

#Updates latest price into fact_data.
def update_latest_fact_data(target_stock_id,target_symbol):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Update latest data. loading_flag == 2.
    get_historical_data(target_stock_id, target_symbol, 2)

#Insert latest price into fact_data.
def insert_latest_fact_data(target_stock_id,target_symbol):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Delete earliest data.
    cursor.execute("""
                    delete from
                        fact_data
                    where
                        stock_id = ?
                        and row_number = 1
                    """, [target_stock_id])
    conn.commit()

    #Insert latest data. loading_flag == 1.
    get_historical_data(target_stock_id, target_symbol, 1)

    #Re-order row_number on data_fact.
    order_row_number(target_stock_id)

#Determine if today's stock data resides in fact_data table.
def insert_update_data_fact_controller(id_list,symbol_list):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    counter = 0

    stock_count = len(id_list)

    while counter < stock_count:
        #Grab max date.
        target_stock_id = id_list[counter]
        target_symbol = symbol_list[counter]

        cursor.execute("""
                        select 
                            max(date)
                        from 
                            fact_data
                        where
                            stock_id = ?
                        """, [target_stock_id])
        conn.commit()

        #Set max date from table and current date. Compare.
        stock_date = [x[0] for x in cursor.fetchall()]
        today_date = str(date.today())

        #If dates don't match, determine if today is weekend.
        current_day = datetime.datetime.today().weekday()

        if current_day > 4 or current_day < 7:
            update_flag = 1
        else:
            update_flag = 0

        '''
        #If dates equal, update current stock info. Otherwise, insert.
        if stock_date[0] == today_date:
            update_latest_fact_data(target_stock_id,target_symbol)
        else:
            insert_latest_fact_data(target_stock_id,target_symbol)
        counter += 1
        '''

        #If dates equal, update current stock info. Otherwise, insert.
        if update_flag == 1:
            update_latest_fact_data(target_stock_id,target_symbol)
        else:
            insert_latest_fact_data(target_stock_id,target_symbol)
        counter += 1
