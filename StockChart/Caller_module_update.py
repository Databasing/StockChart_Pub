import datetime
from Module_latest_update import module_latest_update
from lib.func import check_latest_date



#Set open and close
current_time = datetime.datetime.today()

open_time = current_time.replace(hour=8, minute=30, second=0, microsecond=0)
closed_time = current_time.replace(hour=15, minute=50, second=0, microsecond=0)

#Day of week.
current_day = datetime.datetime.today().weekday()

#Check if market is open.
if current_day <= 4:
    if current_time >= open_time and current_time <= closed_time:
        latest_data_flag = check_latest_date()
        if latest_data_flag == 1:
            module_latest_update()

#module_latest_update()

