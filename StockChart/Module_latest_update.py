from lib.formula import update_latest_sma, update_latest_ema, update_latest_bollinger
from lib.func import get_stock_list, update_dim_status
from lib.data_insert import insert_update_data_fact_controller
from lib.alert import alert_stock
import os

def module_latest_update():
    '''
    Update current stocks.
    '''

    #Grab ID and symbol of all current stocks.
    stock_info_list = get_stock_list()

    #Set variables.
    id_list = [x[0] for x in stock_info_list]
    symbol_list = [x[1] for x in stock_info_list]
    period = 20 #Moving average period

    #Update data_fact with latest price.
    insert_update_data_fact_controller(id_list,symbol_list)

    #Update latest SMA.
    update_latest_sma(id_list,period)

    #Update latest EMA.
    update_latest_ema(id_list,period)

    #Update latest Bollinger Bands.
    update_latest_bollinger(id_list,period)

    #Populate dim_status.
    for stock_id in id_list:
        update_dim_status(stock_id)


    '''
    #Send alerts for current stocks.
    '''
    #Clear alert folder.
    file_list = os.listdir('/home/cinnabon1/Desktop/Projects/StockChart-master/StockChart/alert')
    for f in file_list:
        os.remove(os.path.join('/home/cinnabon1/Desktop/Projects/StockChart-master/StockChart/alert', f))

    #Alert for stocks.
    alert_stock(id_list,symbol_list)

