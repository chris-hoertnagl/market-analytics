from datetime import datetime
from db_connector import spot_engine
from sqlalchemy.sql import select
from sqlalchemy.sql import text
from datetime import date
import pandas as pd

#start_date inclusive, end_date exclusive
#generate Date like '2021-11-06'
def get_data_in_timeframe_from(start_date, end_date, table):
    select_statement = f"SELECT * FROM {table} t WHERE t.time >= '{start_date}' AND t.time < '{end_date}'"
    return pd.read_sql(select_statement, spot_engine)

def delete_spot_table_content(table_name):
    with spot_engine.connect as con:
        con.execute(f"DELETE FROM \"{table_name}\"")
    
