import time

def get_sql_date(datetime=False):
    "Returns current date or datetime in proper sql format"
    if datetime:
        return time.strftime(('%Y-%m-%d %H:%M:%S'))
    else:
        return time.strftime(('%Y-%m-%d'))