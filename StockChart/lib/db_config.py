'''
Database config file.
'''
from sys import platform

#Get conn string based on OS.
def get_db_conn_string():
    if platform == 'win32':
        conn_string = r'lib\sqlite\stockchart.sqlite'
    elif platform == 'linux':
        conn_string = r'/home/cinnabon1/Desktop/Projects/StockChart-master/StockChart/lib/sqlite/stockchart.sqlite'

    return conn_string
