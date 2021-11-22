import pandas as pd
from datetime import datetime

# convert epoch time to readable date time
def format_time(col_name):
   formatted_date_time=pd.to_datetime(col_name.apply(datetime.fromtimestamp))
   return formatted_date_time

# Column to be formatted from miliseconds
format_time(memes['last_update_source'])