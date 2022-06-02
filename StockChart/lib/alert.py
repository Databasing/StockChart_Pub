'''
Logic to send alerts
'''
import sqlite3
from lib.db_config import get_db_conn_string
from dateutil import parser
import matplotlib.pyplot as plt
import os
from lib.email import email_stock

conn_string = get_db_conn_string()

#Stock alert.
def alert_stock(id_list,symbol_list):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    counter = 0

    stock_count = len(id_list)

    while counter < stock_count:
        #Check dim_status for target stock.
        target_stock_id = id_list[counter]
        target_symbol = symbol_list[counter]

        #Graph stock.
        graph_stock(target_stock_id, target_symbol)

        counter += 1

#Graph stock.
def graph_stock(target_stock_id,target_symbol):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Grab all data.
    cursor.execute("""select
                            date,
                            price,
                            sma,
                            ema,
                            bollinger_top,
                            bollinger_bot
                        from
                            fact_data
                        where
                            stock_id = ?
                            """, [target_stock_id])

    stock_data_list = cursor.fetchall()

    #Declare and populate a list for each column.
    price_date_list = []
    technical_date_list = []
    price_list= []
    sma_list = []
    ema_list = []
    bollinger_top_list = []
    bollinger_bot_list = []

    #Loop through stock_data_list and populate each individual list.
    #Populate date_list and price_list.
    for data in stock_data_list:
        price_date_list.append(parser.parse(data[0]))
        price_list.append(data[1])

    #Populate other list starting from row_number 20.
    for data in stock_data_list[19:]:
        technical_date_list.append(parser.parse(data[0]))
        sma_list.append(data[2])
        ema_list.append(data[3])
        bollinger_top_list.append(data[4])
        bollinger_bot_list.append(data[5])

    #Graph creation.
    #Create plot and modify settings.
    figure = plt.figure()

    ax = figure.add_subplot(111)

    ax.yaxis.tick_right()
    ax.yaxis.set_label_position('right')

    #Plot each line.
    plt.plot_date(x = price_date_list, y = price_list, label = 'price', fmt = '-', linewidth = 1.5, color = 'blue')
    plt.plot_date(x = technical_date_list, y = sma_list, fmt = '-',label = 'sma', linewidth = .8, color = 'orange')
    plt.plot_date(x = technical_date_list, y = ema_list, fmt = '-',label = 'ema', linewidth = .8, color = 'green')
    plt.plot_date(x = technical_date_list, y = bollinger_top_list, fmt = '-',label = 'bol_top', linewidth = .8, color = 'black')
    plt.plot_date(x = technical_date_list, y = bollinger_bot_list, fmt = '-',label = 'bol_bot', linewidth = .8, color = 'black')

    #Labels
    plt.title(target_symbol + ' ' + str(price_list[-1]))

    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend(loc='upper left')
    plt.grid()

    #Save in folder and close.
    plt.savefig(r'/home/cinnabon1/Desktop/Projects/StockChart-master/StockChart/alert/stock_graph_' + target_symbol + '.png')

    plt.close()

    #Send email.
    email_stock(target_stock_id,target_symbol)

