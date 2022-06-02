'''
General settings for app.
'''
from sys import platform

#Insert period for technical analysis.
def moving_average_settings_tbl():
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("""insert into settings_tbl (name, value)
                        values (?,?) """, latest_data_list)
