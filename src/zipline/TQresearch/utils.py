import numpy as np
import pandas as pd
import sqlite3
from zipline.data.data_portal import most_recent_data


timestamp = pd.Timestamp.utcnow()
bundle_path = most_recent_data('fundamentals', timestamp)
fundamentals_path = f'{bundle_path}/factor_table.db'


def find_fields(bundle_name:str = 'fundamentals'):
    # Specify the full path to the database file
    timestamp = pd.Timestamp.utcnow()
    bundle_path = most_recent_data(bundle_name, timestamp)
    db_path = f'{bundle_path}/factor_table.db'

    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)

    # Query to fetch data
    query = f'''select * from factor_table
                LIMIT 1
    '''

    # Use pd.read_sql to read data into a DataFrame
    data = pd.read_sql(query, conn)
    conn.close()

    if len(data) == 0:
        raise ValueError('No data in fundamentals bundle, Please use "!zipline ingest -b fundamentals" first.')

    return {data.columns[i]:i for i in range(len(data.columns))}

def GetMRAFundamentals(fields:list = '*',
                        start_dt = '2013-01-01' ,
                        end_dt = pd.Timestamp.utcnow(),
                        assets=None,
                        dataframeloaders = False
                       ):

    fields_to_str = ','.join(fields)

    # Query to fetch data
    scripts = f'''
    with cte as (
	SELECT symbol, 
	fin_date, 
	date,
	ROW_NUMBER() OVER (PARTITION BY symbol, fin_date ORDER BY date) AS annd_date
	FROM
		factor_table
	where fin_date is not null and strftime('%m', fin_date)='12' and date >= '{start_dt}' and strftime('%Y-%m-%d',date) <= '{end_dt}'
    )

    select {fields_to_str} from cte
    where annd_date = 1
    '''
    # Connect to the SQLite database
    conn = sqlite3.connect(fundamentals_path)

    # Use pd.read_sql to read data into a DataFrame
    data = pd.read_sql(scripts, conn)
    conn.close()

    return data

def GetMRQFundamentals(fields:list = '*',
                        start_dt = '2013-01-01' ,
                        end_dt = pd.Timestamp.utcnow(),
                        assets=None,
                        dataframeloaders = False
                       ):

    fields_to_str = ','.join(fields)

    # Query to fetch data
    scripts = f'''
    with cte as (
	SELECT symbol, 
	fin_date, 
	date,
	ROW_NUMBER() OVER (PARTITION BY symbol, fin_date ORDER BY date) AS annd_date
	FROM
		factor_table
	where fin_date is not null and date >= '{start_dt}' and strftime('%Y-%m-%d',date) <= '{end_dt}'
    )

    select {fields_to_str} from cte
    where annd_date = 1
    '''
    # Connect to the SQLite database
    conn = sqlite3.connect(fundamentals_path)

    # Use pd.read_sql to read data into a DataFrame
    data = pd.read_sql(scripts, conn)
    conn.close()

    return data

