'''
General functions
'''
import sqlite3
import sys
from datetime import date, datetime, timedelta
from lib.db_config import get_db_conn_string
import urllib3
from bs4 import BeautifulSoup
from json import loads
import re
import yfinance as yf
import pandas as pd

#Global variables.
conn_string = get_db_conn_string()
manager = urllib3.PoolManager()

#Get relevant ID.
def get_stock_id(symbol):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("""
                    select 
                        stock_id 
                    from 
                        dim_stock 
                    where
                        symbol = ?
                    """, [symbol])
    conn.commit()

    row = [x[0] for x in cursor.fetchall()]
    stock_id = row[0]

    try:
        stock_id
    except:
        print('Stock not found in database.')
        sys.exit(1)

    conn.close()

    return stock_id

#Get list of stocks.
def get_stock_list():
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("""
                    select 
                        stock_id,
                        symbol
                    from 
                        dim_stock
                    """,)
    conn.commit()

    return cursor.fetchall()

#Get max row_number.
def get_max_row_number(stock_id):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("""
                    select 
                        max(row_number)
                    from 
                        fact_data
                    where
                        stock_id = ?
                    """, [stock_id])
    conn.commit()

    return [x[0] for x in cursor.fetchall()]

#Determine if today's stock data resides in fact_data table.
def insert_update_data_fact_flag(stock_id):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("""
                    select 
                        max(date)
                    from 
                        fact_data
                    where
                        stock_id = ?
                    """, [stock_id])
    conn.commit()

    return [x[0] for x in cursor.fetchall()]

#Populate dim_status based on latest stock info.
def update_dim_status(stock_id):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Grab latest stock info.
    cursor.execute("""
                    select 
                        price,
                        bollinger_top,
                        bollinger_bot,
                        sma,
                        ema,
                        date
                    from 
                        fact_data
                    where
                        stock_id = ?
                    order by
                        row_number desc      
                    limit 2
                    """, [stock_id])
    conn.commit()

    #Convert results/tuple to list for both rows.
    data_tuple = [x for x in cursor.fetchall()]

    #Row 1.
    latest_list_str = str(data_tuple[0])
    latest_list_str = latest_list_str.replace('(', '').replace(')', '')
    latest_list = latest_list_str.split(',')

    #Row 2.
    second_latest_list_str = str(data_tuple[1])
    second_latest_list_str = second_latest_list_str.replace('(', '').replace(')', '')
    second_latest_list = second_latest_list_str.split(',')

    #Set variables.
    price = float(latest_list[0])
    bollinger_top = float(latest_list[1])
    bollinger_bot = float(latest_list[2])
    sma = float(latest_list[3])
    ema = float(latest_list[4])
    current_date = str(date.today())

    #Set second variables.
    second_price = float(second_latest_list[0])
    second_sma = float(second_latest_list[3])
    second_ema = float(second_latest_list[4])

    #Build status list based on conditions. For statuses 0 = No, 1 = Yes.
    status_list = []

    #Price >= bollinger_top
    if price >= bollinger_top:
        status_list.append(1)
    elif (min(price, bollinger_top) / max(price, bollinger_top)) >= .988:
        status_list.append(1)
    else:
        status_list.append(0)

    #Price <= bollinger_bot
    if price <= bollinger_bot:
        status_list.append(1)
    elif (min(price, bollinger_bot) / max(price, bollinger_bot)) >= .988:
        status_list.append(1)
    else:
        status_list.append(0)

    #Calculate bollinger_percent.
    bollinger_range = bollinger_top - bollinger_bot
    bollinger_range_bot = price - bollinger_bot
    bollinger_percent = (bollinger_range_bot / bollinger_range) * 100

    status_list.append(round(bollinger_percent,2))

    #Check EMA >= SMA for both rows.
    if ema >= sma:
        ema_over = 1
    else:
        ema_over = 0

    if second_ema >= second_sma:
        second_ema_over = 1
    else:
        second_ema_over = 0

    #Populate status_list if variables are different. (Tracks moving average crossover)
    if ema_over != second_ema_over:
        status_list.append(1)
    else:
        status_list.append(0)

    #Check if price changed by 6% from today and yesterday.
    if abs((min(price, second_price)/max(price, second_price)) - 1) >= .06:
        status_list.append(1)
    else:
        status_list.append(0)

    #Date
    status_list.append(current_date)

    #Stock_id
    status_list.append(stock_id)

    #Update dim_status.
    cursor.execute("""update
                            dim_status
                        set
                            bollinger_top_status = ?,
                            bollinger_bot_status = ?,
                            bollinger_percent = ?,
                            ema_over_sma_change = ?,
                            price_percent_change = ?,
                            date = ?
                        where
                            stock_id = ?
                                """, status_list)

    conn.commit()

    cursor.close()

#Check if there's data for today on website. 1 = proceed. 0 = exit.
def check_latest_date():
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Set variables.
    current_date = date.today()
    current_date = str(current_date)

    #Get latest date from website. Build link to scrape. Get entire html, decode and parse.
    link = 'https://finance.yahoo.com/quote/sq/history?p=sq'

    get_html = manager.request('GET', link)
    get_html = str(get_html.data.decode('utf-8'))  # <class 'str'>
    soup_get_html = BeautifulSoup(get_html, 'html.parser')  # <class 'bs4.BeautifulSoup'>

    #Capture historical data and parse into list.
    get_historical = soup_get_html.find_all('td')  # <class 'bs4.element.ResultSet'>
    get_historical = str(get_historical)
    soup_get_historical = BeautifulSoup(get_historical, 'html.parser')  # <class 'bs4.BeautifulSoup'>

    historical_list = list(soup_get_historical.stripped_strings)

    #Grab latest date from site.
    for d in historical_list:
        #Remove unwanted characters from list.
        date_flag = d.find(',')
        #Parse latest_date for comparison.
        if date_flag == 6:
            latest_date = d.replace(',', '')
            latest_date = datetime.strptime(latest_date, '%b %d %Y')
            latest_date = str(latest_date.date())
            break

    #Compare today's date and latest_date.
    if current_date == latest_date:
        latest_data_flag = 1
    else:
        latest_data_flag = 0

    return latest_data_flag

def stock_exist_check(symbol):
    #Build link to scrape. Get entire html, decode and parse.
    link = 'https://finance.yahoo.com/quote/' + symbol + '/history?p=' + symbol

    get_html = manager.request('GET', link)
    get_html = str(get_html.data.decode('utf-8'))  # <class 'str'>
    soup_get_html = BeautifulSoup(get_html, 'html.parser')  # <class 'bs4.BeautifulSoup'>

    #Capture stock name and parse into variable.
    get_stock = soup_get_html.find_all('h1')  # <class 'bs4.element.ResultSet'>
    get_stock = str(get_stock)
    soup_get_stock = BeautifulSoup(get_stock, 'html.parser')  # <class 'bs4.BeautifulSoup'>

    stock_name = list(soup_get_stock.stripped_strings)

    try:
        stock_name = stock_name[1]
    except:
        print('Error: Stock \'' + symbol + '\' not found.')
        sys.exit(1)

    return (stock_name)

def order_row_number(stock_id):

    #Insert stock into dim_stock.
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()


    #Re-order row_number on data_fact.
    cursor.execute("""with recursive rn_cte as
                        (
                            select
                                row_number() over (order by date) as new_row_number,
                                stock_id,
                                date
                            from
                                fact_data
                            where
                                stock_id = ?
                        )
                        update
                            fact_data
                        set
                            row_number = (select
                                                new_row_number
                                            from
                                                rn_cte
                                            where
                                                 fact_data.stock_id = rn_cte.stock_id
                                                 and fact_data.date = rn_cte.date)
                        where exists (select
                                            stock_id
                                        from
                                            rn_cte
                                        where
                                             fact_data.stock_id = rn_cte.stock_id
                                             and fact_data.date = rn_cte.date)    
                    """, [stock_id])
    conn.commit()

    conn.close()

def get_historical_data(stock_id,symbol,loading_flag):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Set variables.
    #Grab current date.
    current_date = date.today()
    #Grab ticker from api.
    stock_ticker = yf.Ticker(symbol)
    #Get historical market data from API.
    df = stock_ticker.history(period="130d")

    #Turn pandas dataframe into a list. Reverse list to fit old logic.
    get_historical_list = df.values.tolist()
    get_historical_list.reverse()

    #Set looping variable based on period.
    count = len(get_historical_list) - 1

    #Iterate list to populate data_list and insert into fact_data.
    for d in get_historical_list:
        data_list = []
        stock_date = str(df.index[count])
        
        data_list.append(stock_id)
        data_list.append(stock_date[:10]) #Date
        data_list.append(d[0]) #Open
        data_list.append(d[1]) #High
        data_list.append(d[2]) #Low
        data_list.append(d[3]) #Close
        data_list.append(d[4]) #Volume
        data_list.append(d[3]) #Price (Use close to determine price)

        #Loop incrementer.
        count -= 1
        '''
        historical_flag values
        0 == Load historical
        1 == Insert latest
        2 == Update latest
        '''
        #Update latest
        if loading_flag == 2:
            # Build latest_list to update fact_data.
            latest_list = []

            latest_list.append(data_list[5])  # Price
            latest_list.append(data_list[2])  # Open
            latest_list.append(data_list[5])  # Close
            latest_list.append(data_list[0])  # stock_id
            latest_list.append(data_list[0])  # stock_id

            cursor.execute("""update
                                    fact_data
                                set
                                    price = ?,
                                    open = ?,
                                    close = ?
                                where
                                    stock_id = ?
                                    and row_number = (select max(row_number) from fact_data where stock_id = ?) """,
                           latest_list)
            conn.commit()
            break

        #Insert into fact_data.
        cursor.execute("""insert into fact_data (stock_id, date, open, high, low, close, volume, price)
                            values (?,?,?,?,?,?,?,?) """, data_list)
        conn.commit()

        #Break after one loop.
        if loading_flag == 1:
            break

    conn.close()
