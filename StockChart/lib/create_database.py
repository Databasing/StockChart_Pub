'''
Create statements for tables.
'''
import sqlite3
from lib.db_config import get_db_conn_string

conn_string = get_db_conn_string()

def create_db():
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Check if tables exist.
    cursor.execute("""
                    select name from sqlite_master
                    """)
    conn.commit()

    rows = cursor.fetchall()

    #Drop tables if exist.
    if len(rows) > 1:

        #Drop tables
        cursor.executescript("""
                            drop table dim_stock;
                            drop table fact_data;
                            drop table stg_bollinger;
                            drop table dim_status;
                            """)
        conn.commit()

    #Create dim_stock
    cursor.execute("""create table dim_stock
                   (stock_id integer primary key autoincrement
                   ,symbol text
                   ,name text)
                    """)

    #Create fact_data
    cursor.execute("""CREATE TABLE fact_data
                   (stock_id int
                   ,date text
                   ,row_number int
                   ,price int
                   ,open int
                   ,high int
                   ,low int
                   ,close int
                   ,volume int
                   ,sma int
                   ,ema int
                   ,bollinger_top int
                   ,bollinger_bot int
                   ,primary key (stock_id, date)) without rowid
                    """)

    #Create stg_bollinger
    cursor.execute("""CREATE TABLE stg_bollinger
                   (stock_id int
                   ,row_number int
                   ,price int
                   ,sma int
                   ,stdev int)
                    """)

    #Create dim_status
    cursor.execute("""CREATE TABLE dim_status
                   (stock_id int
                   ,bollinger_top_status int
                   ,bollinger_bot_status int
                   ,bollinger_percent int
                   ,ema_over_sma_change int
                   ,price_percent_change int
                   ,date text)
                    """)

    conn.commit()

    cursor.close()

