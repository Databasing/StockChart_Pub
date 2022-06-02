import sqlite3
from lib.db_config import get_db_conn_string
import smtplib, ssl
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders
import os.path
from decimal import Decimal

conn_string = get_db_conn_string()

#Send email.
def email_stock(target_stock_id,target_symbol):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Grab status for stock.
    cursor.execute("""select
                            bollinger_top_status
                            ,bollinger_bot_status
                            ,ema_over_sma_change
                            ,price_percent_change
                            ,bollinger_percent
                        from
                            dim_status
                        where
                            stock_id = ?
                                """, [target_stock_id])

    data_tuple = [x for x in cursor.fetchall()]

    #Turn data_tuple into a list.
    status_list_str = str(data_tuple[0])
    status_list_str = status_list_str.replace('(', '').replace(')', '')
    status_list = status_list_str.split(',')

    #Set variables.
    bollinger_top_status = int(status_list[0])
    bollinger_bot_status = int(status_list[1])
    ema_over_sma_change = int(status_list[2])
    price_percent_change = int(status_list[3])
    bollinger_percent = Decimal(status_list[4])

    #Build subject and message string.
    subject = 'Stock: ' + target_symbol + '. '

    message = str(bollinger_percent) + "! "

    if ema_over_sma_change == 1:
        message = message + "Moving average CROSSED! "
    if price_percent_change == 1:
        message = message + "Price changed SIGNIFICANTLY! "


    #Copied code for email.
    sender_email = ""
    password = ""
    receiver_email = ""
    subject = subject
    message = message
    file_location = '/home/cinnabon1/Desktop/Projects/StockChart-master/StockChart/alert/stock_graph_' + target_symbol + '.png'

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    msg.attach(MIMEText(message, 'plain'))

    filename = os.path.basename(file_location)
    attachment = open(file_location, "rb")
    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename = 'stock_graph_' + target_symbol + '.png')

    msg.attach(part)

    server = smtplib.SMTP('', 587)
    server.starttls()
    server.login(sender_email, password)
    text = msg.as_string()
    server.sendmail(sender_email, receiver_email, text)
    server.quit()

