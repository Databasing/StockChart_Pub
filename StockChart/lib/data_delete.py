'''
General settings for app.
'''
import sqlite3
from lib.func import get_db_conn_string

conn_string = get_db_conn_string()

#Delete stock from all tables.
def delete_stock(stock_id):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Delete from fact_data.
    cursor.execute("""
                    delete from
                        fact_data
                    where
                        stock_id = ?
                    """, [stock_id])
    conn.commit()

    #Delete from dim_stock.
    cursor.execute("""
                    delete from
                        dim_stock
                    where
                        stock_id = ?
                    """, [stock_id])
    conn.commit()

    #Delete from dim_status.
    cursor.execute("""
                    delete from
                        dim_status
                    where
                        stock_id = ?
                    """, [stock_id])
    conn.commit()

    conn.close()