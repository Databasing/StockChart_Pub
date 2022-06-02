'''
Formula for technical analysis.
'''
import sqlite3
import statistics
from lib.func import get_stock_id, get_max_row_number, get_db_conn_string


conn_string = get_db_conn_string()

#Populate SMA.
def insert_sma(stock_id,period):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("""with recursive sma_cte as
                    (
                    select
                        stock_id,
                        date,
                        sum(price) over (partition by stock_id order by stock_id, date rows %s preceding)/%s as sma_calc
                    from
                        fact_data
                    where
                        stock_id = ?
                    )
                    update
                        fact_data
                    set
                        sma = (select
                                    sma_calc
                                from
                                    sma_cte
                                where
                                        fact_data.stock_id = sma_cte.stock_id
                                        and fact_data.date = sma_cte.date)
                    where exists (select
                                        stock_id
                                    from
                                        sma_cte
                                    where
                                         fact_data.stock_id = sma_cte.stock_id
                                         and fact_data.date = sma_cte.date)
                    """ % (period - 1, period), [stock_id])

    conn.commit()
    conn.close()

#Update latest SMA.
def update_latest_sma(id_list,period):
    #Get count of stocks and loop.
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    counter = 0

    stock_count = len(id_list)

    while counter < stock_count:
        #Grab stock_id, calculate sma and loop.
        target_stock_id = id_list[counter]

        target_stock_list = []
        target_stock_list.append(target_stock_id)
        target_stock_list.append(target_stock_id)
        target_stock_list.append(target_stock_id)

        cursor.execute("""with recursive sma_cte as
                        (
                        select
                            stock_id,
                            row_number,
                            date,
                            sum(price) over (partition by stock_id order by stock_id, date rows %s preceding)/%s as sma_calc
                        from
                            fact_data
                        where
                            stock_id = ?
                        )
                        update
                            fact_data
                        set
                            sma = (select
                                        sma_calc
                                    from
                                        sma_cte
                                    where
                                            fact_data.stock_id = sma_cte.stock_id
                                            and fact_data.date = sma_cte.date)
                        where row_number = (select
                                                max(row_number)
                                            from
                                                fact_data
                                            where
                                                fact_data.stock_id = ?)
                                and stock_id = ?
                        """ % (period - 1, period), target_stock_list)

        conn.commit()

        counter += 1

    conn.close()

#Populate EMA.
def insert_ema(stock_id,period):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Get initial EMA first.
    cursor.execute("""with recursive ema_cte as
                    (
                    select 
                        stock_id,
                        date,
                        (close - sma) * (cast(2 as float)/(%s + 1)) + sma as ema_calc
                    from 
                        fact_data
                    where
                        stock_id = ?
                        and row_number = %s
                    )
                    update
                        fact_data
                    set
                        ema = (select
                                    ema_calc
                                from
                                    ema_cte
                                where
                                    fact_data.stock_id = ema_cte.stock_id
                                    and fact_data.date = ema_cte.date)
                    where exists (select
                                        stock_id
                                    from
                                        ema_cte
                                    where
                                         fact_data.stock_id = ema_cte.stock_id
                                         and fact_data.date = ema_cte.date)
                    """ % (period, period), [stock_id])

    conn.commit()

    #Get count of remaining EMA rows to populate.
    cursor.execute("""select
                            count(*)
                        from
                            fact_data
                        where
                            row_number > (select 
                                                max(row_number)
                                            from 
                                                fact_data
                                            where 
                                                stock_id = ?
                                                and ema is not null)
                            and stock_id = ?
                        """, [stock_id,stock_id])

    conn.commit()

    row = [x[0] for x in cursor.fetchall()]
    ema_count = row[0]

    #Calculate remaining EMA.
    counter = 0
    while counter <= ema_count:
        cursor.execute("""with recursive ema_cte as
                        (
                        select 
                            stock_id,
                            date,
                            (close - lag(ema) over (partition by stock_id order by stock_id, date)) * (cast(2 as float)/(%s + 1)) + lag(ema) over (partition by stock_id order by stock_id, date) as ema_calc
                        from 
                            fact_data
                        where
                            stock_id = ?
                        )
                        update
                            fact_data
                        set
                            ema = (select
                                        ema_calc
                                    from
                                        ema_cte
                                    where
                                        fact_data.stock_id = ema_cte.stock_id
                                        and fact_data.date = ema_cte.date)
                        where exists (select
                                            stock_id
                                        from
                                            ema_cte
                                        where
                                            fact_data.stock_id = ema_cte.stock_id
                                            and fact_data.date = ema_cte.date
                                            and fact_data.ema is null
                                            and ema_calc is not null)
                                """ % (period), [stock_id])

        counter += 1

    conn.commit()
    conn.close()

#Update latest EMA.
def update_latest_ema(id_list,period):
    #Get count of stocks and loop.
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    counter = 0

    stock_count = len(id_list)

    while counter < stock_count:
        #Grab stock_id, calculate sma and loop.
        target_stock_id = id_list[counter]

        target_stock_list = []
        target_stock_list.append(target_stock_id)
        target_stock_list.append(target_stock_id)
        target_stock_list.append(target_stock_id)

        cursor.execute("""with recursive ema_cte as
                        (
                        select
                            stock_id,
                            row_number,
                            date,
                            (close - lag(ema) over (partition by stock_id order by stock_id, date)) * (cast(2 as float)/(%s + 1)) + lag(ema) over (partition by stock_id order by stock_id, date) as ema_calc
                        from
                            fact_data
                        where
                            stock_id = ?
                        )
                        update
                            fact_data
                        set
                            ema = (select
                                        ema_calc
                                    from
                                        ema_cte
                                    where
                                            fact_data.stock_id = ema_cte.stock_id
                                            and fact_data.date = ema_cte.date)
                        where row_number = (select
                                                max(row_number)
                                            from
                                                fact_data
                                            where
                                                fact_data.stock_id = ?)
                            and stock_id = ?
                        """ % (period), target_stock_list)

        conn.commit()

        counter += 1

    conn.close()

#Populate Bollinger Bands.
def insert_bollinger(stock_id,period):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    #Import target stock into stg_bollinger.
    cursor.execute("""delete from stg_bollinger""")

    conn.commit()

    cursor.execute("""
                    insert into stg_bollinger
                        (stock_id,
                        row_number,
                        price,
                        sma)
                    select
                        stock_id,
                        row_number,
                        price,
                        sma
                    from
                        fact_data
                    where
                        stock_id = ?;
                        """, [stock_id])

    conn.commit()

    #Grab rows requiring stdev calc.
    cursor.execute("""select 
                            row_number
                        from 
                            stg_bollinger 
                        where
                            stock_id = ?
                            and row_number >= ?
                    """, [stock_id,period])

    conn.commit()
    rows = [x[0] for x in cursor.fetchall()]

    #Extraction based on row numbers.
    for rn in rows:
        #Join table where a >= b. Get row_number in reverse order.
        cursor.execute("""with reverse_cte as
                            (
                            select
                                a.row_number as a_rn,
                                b.row_number as b_rn,
                                row_number() over (partition by a.row_number order by b.row_number desc) as b_rn_reverse,
                                b.price
                            from
                                stg_bollinger a
                                join stg_bollinger b
                                on a.stock_id = b.stock_id
                                and a.row_number >= b.row_number
                            where
                                a.stock_id = ?
                            ) 
                            select 
                                price
                            from 
                                reverse_cte
                            where
                                b_rn_reverse <= ?
                                and a_rn = ?
                            """, [stock_id, period, rn])

        conn.commit()

        #Insert standard deviation.
        price_list = [x[0] for x in cursor.fetchall()]
        standard_deviation = statistics.pstdev(price_list)

        cursor.execute("""update
                                stg_bollinger
                            set
                                stdev = ?
                            where
                                stock_id = ?
                                and row_number = ?
                                    """, [standard_deviation, stock_id, rn])

        conn.commit()

    #Update Bollinger Band.
    cursor.execute("""with recursive bol_cte as
                        (
                        select
                            stock_id,
                            row_number,
                            sma + (stdev * 2) as bol_top,
                            sma - (stdev * 2) as bol_bot
                        from
                            stg_bollinger
                        where
                            stock_id = ?
                        )
                        update
                            fact_data
                        set
                            bollinger_top = (select
                                                bol_top
                                            from
                                                bol_cte
                                            where
                                                fact_data.stock_id = bol_cte.stock_id
                                                and fact_data.row_number = bol_cte.row_number),
                            bollinger_bot = (select
                                                bol_bot
                                            from
                                                bol_cte
                                            where
                                                fact_data.stock_id = bol_cte.stock_id
                                                and fact_data.row_number = bol_cte.row_number)                       
                        where exists (select
                                            stock_id
                                        from
                                            bol_cte
                                        where
                                            fact_data.stock_id = bol_cte.stock_id
                                            and fact_data.row_number = bol_cte.row_number
                                            and fact_data.row_number >= ?)
                                """, [stock_id,period])

    conn.commit()
    conn.close()

#Update Bollinger Bands.
def update_latest_bollinger(id_list,period):
    conn = sqlite3.connect(conn_string)
    cursor = conn.cursor()

    counter = 0

    stock_count = len(id_list)

    while counter < stock_count:
        #Grab stock_id and insert into sttg_bollinger.
        target_stock_id = id_list[counter]

        cursor.execute("""delete from stg_bollinger""")

        conn.commit()

        cursor.execute("""
                        insert into stg_bollinger
                            (stock_id,
                            row_number,
                            price,
                            sma)
                        select
                            stock_id,
                            row_number,
                            price,
                            sma
                        from
                            fact_data
                        where
                            stock_id = ?
                            """, [target_stock_id])

        conn.commit()

        max_row_number = get_max_row_number(target_stock_id)
        max_row_number = max_row_number[0]

        #Extraction based on row numbers.
        #Join table where a >= b. Get row_number in reverse order.
        cursor.execute("""with reverse_cte as
                            (
                            select
                                a.row_number as a_rn,
                                b.row_number as b_rn,
                                row_number() over (partition by a.row_number order by b.row_number desc) as b_rn_reverse,
                                b.price
                            from
                                stg_bollinger a
                                join stg_bollinger b
                                on a.stock_id = b.stock_id
                                and a.row_number >= b.row_number
                            where
                                a.stock_id = ?
                            ) 
                            select 
                                price
                            from 
                                reverse_cte
                            where
                                b_rn_reverse <= ?
                                and a_rn = ?
                            """, [target_stock_id, period, max_row_number])

        conn.commit()

        #Insert standard deviation.
        price_list = [x[0] for x in cursor.fetchall()]
        standard_deviation = statistics.pstdev(price_list)

        cursor.execute("""update
                                stg_bollinger
                            set
                                stdev = ?
                            where
                                stock_id = ?
                                and row_number = ?
                                    """, [standard_deviation, target_stock_id, max_row_number])

        conn.commit()

        #Update Bollinger Band.
        cursor.execute("""with recursive bol_cte as
                            (
                            select
                                stock_id,
                                row_number,
                                sma + (stdev * 2) as bol_top,
                                sma - (stdev * 2) as bol_bot
                            from
                                stg_bollinger
                            where
                                stock_id = ?
                            )
                            update
                                fact_data
                            set
                                bollinger_top = (select
                                                    bol_top
                                                from
                                                    bol_cte
                                                where
                                                    fact_data.stock_id = bol_cte.stock_id
                                                    and fact_data.row_number = bol_cte.row_number),
                                bollinger_bot = (select
                                                    bol_bot
                                                from
                                                    bol_cte
                                                where
                                                    fact_data.stock_id = bol_cte.stock_id
                                                    and fact_data.row_number = bol_cte.row_number)                       
                            where row_number = (select
                                                    max(row_number)
                                                from
                                                    bol_cte
                                                where
                                                    fact_data.stock_id = bol_cte.stock_id
                                                    and fact_data.row_number = bol_cte.row_number
                                                    and fact_data.row_number = ?)
                                        """, [target_stock_id, max_row_number])

        conn.commit()

        counter += 1

    conn.close()