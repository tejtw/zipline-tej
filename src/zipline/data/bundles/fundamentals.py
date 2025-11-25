"""
Module for building a complete daily dataset from Quandl's WIKI dataset.
"""
from io import BytesIO
import os
import TejToolAPI
from click import progressbar
import logging
import pandas as pd
import requests
from zipline.utils.calendar_utils import register_calendar_alias
from zipline.utils.api_info import get_api_key_info
from datetime import datetime
import numpy as np
import re
import tejapi
from .core import register
from zipline.utils.numpy_utils import iNaT, float64_dtype, uint32_dtype, uint64_dtype
import bcolz
import zipline.utils.paths as pth
from .core import (
    cache_path,
    to_bundle_ingest_dirname,

) 
import sqlite3
import dask.dataframe as dd
from sqlalchemy import create_engine
import multiprocessing as mp
npartitions_local = mp.cpu_count()

from time import sleep , mktime
log = logging.getLogger(__name__)

tejapi.ApiConfig.page_limit = 10000
ONE_MEGABYTE = 1024 * 1024


def fetch_data_table(api_key, show_progress,  coid, mdate, columns, **kwargs):
    """Fetch Prices data table from TEJ"""
    import os 
    os.environ['TEJAPI_KEY'] = api_key
    if kwargs.get('include_self_acc') == 'Y':
        self_acc = 'Y'
    else:
        self_acc = 'N'


    try :
        data = TejToolAPI.get_history_data(ticker = coid, 
                                           columns = columns, 
                                           start = mdate['gte'], 
                                           end = mdate['lte'],
                                           include_self_acc = self_acc,
                                           require_annd = True,
                                           show_progress = False
                                           )

        if data.size == 0:
            raise ValueError("Did not fetch any fundamental data. Please check the correctness of your ticker, mdate and fields.")
        
        data = data.rename({'coid':'symbol', 'mdate':'date'}, axis =1)
        data['date'] = data['date'].astype('datetime64[ns]')
        if 'fin_date' in data.columns :
            data['fin_date'] = data['fin_date'].astype('datetime64[ns]')
    
    except Exception as e  :
        raise ValueError(f'Error occurs while downloading metadata due to {e} .')
    
    
    return data



def cache_df_columns(dataframe, filepath):
    fileName = os.path.join(filepath, 'cache_columns.txt')
    if os.path.exists(fileName):
        existed_columns = get_cached_columns(filepath)
        columns = set(existed_columns)
        columns.update(set(dataframe.columns.tolist()))
        columns = list(columns)
        with open(fileName, 'w') as file:
            for column in columns:
                file.write(column + '\n')
        return 
    
    with open(fileName, 'w') as file:
        for column in dataframe.columns.tolist():
            file.write(column + '\n')

def get_cached_columns(filepath):
    fileName = os.path.join(filepath, 'cache_columns.txt')
    if os.path.exists(filepath):
        with open(fileName, 'r') as file:
            columns = file.read().splitlines()
        return columns
    else:
        raise FileNotFoundError(f"No cached file found at {filepath}")



def gen_asset_metadata(data, show_progress):
    if show_progress:
        log.info("Generating asset metadata.")

    data = data.groupby(by="symbol").agg({"date": [np.min, np.max]})
    data.reset_index(inplace=True)
    data["start_date"] = data[("date","min")]
    data["end_date"] = data[("date","max")]
    del data["date"]
    data.columns = data.columns.get_level_values(0)

    data["exchange"] = "TEJ_XTAI"
    data["auto_close_date"] = data["end_date"].values + pd.Timedelta(days=1)
    return data

def CreateSQLiteEngine(db_path, db_name = 'my_table', schema_name= 'my_schema'):
    db_url = os.path.join(db_path, db_name)    # 創建 SQLAlchemy 引擎
    conn = sqlite3.connect(db_url)

    return conn

def tej_bundle(
    environ,
    asset_db_writer,
    minute_bar_writer,
    daily_bar_writer,
    adjustment_writer,
    calendar,
    start_session,
    end_session,
    cache,
    show_progress,
    output_dir,
    **kwargs
):
    """
    quandl_bundle builds a daily dataset using Quandl's WIKI Prices dataset.

    For more information on Quandl's API and how to obtain an API key,
    please visit https://docs.quandl.com/docs#section-authentication
    """
    
    source_path = os.environ.get('raw_source')
    DB_name = 'factor_table.db'
    
    api_key = environ.get('TEJAPI_KEY')
    
    if source_path :
        from TejToolAPI import get_internal_code
        source_db = os.path.join(source_path,DB_name)
        con = sqlite3.connect(source_db)
        fields = pd.read_sql('SELECT name FROM PRAGMA_TABLE_INFO("factor_table");',con).name.to_list()
        if 'symbol' in fields :
            fields.remove('symbol')
        if 'date' in fields :
            fields.remove('date')
        for idx , field in enumerate(fields) :
            if field.endswith('_Q') :
                fields[idx] = field[:field.rfind('_Q')]
            elif field.endswith('_A') :
                fields[idx] = field[:field.rfind('_A')]
            elif field.endswith('_TTM') :
                fields[idx] = field[:field.rfind('_TTM')]
                
        new_field = environ.get('fields')
        
        mdate = pd.read_sql(r"SELECT date(max(date),'+1 day') ||','|| date(CURRENT_DATE) as mdate FROM factor_table;", con).mdate
        
        if new_field :
            new_field = re.split('[,; ]',new_field)
            new_field = set(get_internal_code(new_field))
            fields = set(get_internal_code(fields))
            if new_field.difference(fields) :
                fields = new_field.difference(fields)
                mdate = pd.read_sql(r"SELECT date(min(date)) ||','|| date(max(date)) as mdate FROM factor_table;", con).mdate
        
        fields = (lambda x : ';'.join(x))(fields)
        
            
        new_symbol = environ.get('ticker')
        old_symbol = pd.read_sql('SELECT DISTINCT symbol FROM factor_table;', con).symbol.tolist()
        if new_symbol :
            new_symbol = re.split('[,; ]',new_symbol)
            new_symbol = old_symbol + new_symbol
            new_symbol = set(new_symbol)
            old_symbol = set(old_symbol)
            new_symbol = new_symbol.difference(old_symbol)
            if new_symbol :
                symbol = ';'.join(new_symbol)
                mdate = pd.read_sql(r"SELECT date(min(date)) ||','|| date(max(date)) as mdate FROM factor_table;", con).mdate
            else :
                symbol = ';'.join(old_symbol)
                log.info("No new ticker detected, updating.")
            
        if isinstance( mdate , pd.Series) :
            mdate = mdate[0]
            
        environ['mdate'] = mdate
        environ['fields'] = fields
        environ['ticker'] = symbol
        with open(os.path.join(source_path,'self_acc_history.json'),'r',encoding = 'utf-8') as f :
            self_acc = f.read()
        environ['include_self_acc'] = self_acc
        con.close()
        
    coid = environ.get('ticker')
    columns = environ.get('fields')
    mdate = environ.get('mdate')
    include_self_acc = environ.get('include_self_acc')
    kwargs['include_self_acc'] = include_self_acc
    
    with open (os.path.join(output_dir,'self_acc_history.json'),'w',encoding = 'utf-8') as f :
        self_acc = kwargs.get('include_self_acc')
        if self_acc == 'Y' :
            f.write(self_acc)
        else :
            f.write('N')

    # Parse info from coid and mdate
    if coid :
        coid = re.split('[,; ]',coid)
    else :
        raise ValueError(
            "Please set company id in your environment variable and retry."
        )
    
    if mdate :
        mdate = re.split('[,; ]',mdate)

        if len(mdate) == 1 :
            mdate.append(pd.to_datetime(datetime.today()).strftime('%Y%m%d'))

        elif len(mdate) > 2 :
            raise IndexError(
            "mdate must less than or equal to 2 parameters, please reset mdate."
            )
        
        mdate = {'gte':mdate[0],'lte':mdate[1]}
    
    if api_key is None:
        raise ValueError(
            "Please set your TEJAPI_KEY environment variable and retry."
        )

    if isinstance(columns, str):
        columns = re.split('[,; ]',columns)

    else :
        raise ValueError(
            "Please set fields in your environment variable and retry."
        )
        
    raw_data = fetch_data_table(
        api_key=api_key, 
        show_progress=show_progress, 
        coid=coid , 
        mdate=mdate, 
        columns=columns, 
        **kwargs,
    )
    
    # print(raw_data.columns)
    db_output_path = os.path.join(output_dir,DB_name)

    if source_path :
        raw_data['date'] = raw_data['date'].apply(pd.to_datetime)
        con = sqlite3.connect(source_db)
        old_data = pd.read_sql(sql = "SELECT * FROM factor_table;",con = con , parse_dates = raw_data.select_dtypes('datetime').columns.tolist())
        t0_col = raw_data.columns[~raw_data.columns.isin(old_data.columns)].tolist()
        if any( t0_col ) :
            t0 = raw_data.iloc[:,raw_data.columns.get_indexer(['symbol','date'] + t0_col)]
            t1 = old_data.set_index(['symbol','date'])
            t2 = t0.set_index(['symbol','date'])
            raw_data = t1.join(t2,how = 'outer').reset_index()
            del t0, t1 , t2
        else :
            raw_data = pd.concat([old_data,raw_data])
        raw_data = raw_data.drop_duplicates()
        raw_data = raw_data.sort_values(by =['symbol','date'])
        raw_data = raw_data.reset_index(drop = True)
        cols = raw_data.columns[~raw_data.columns.isin(['symbol'])]
        raw_data[cols] = raw_data.groupby(["symbol"]).fillna(method = 'ffill')
        con.close()


    asset_metadata = gen_asset_metadata(raw_data[["symbol", "date"]], show_progress)
    exchanges = pd.DataFrame(
        data=[["TEJ_XTAI", "TEJ_XTAI", "TW"]],
        columns=["exchange", "canonical_name", "country_code"],
    )

    asset_db_writer.write(equities=asset_metadata, exchanges=exchanges)
    adjustment_writer.write()

    # 建立 SQLite 引擎
    schema_name = fundamental_schema

    # create engine of sqlite
    engine = CreateSQLiteEngine(output_dir, db_name=DB_name, schema_name=schema_name)
    # cached_columns(raw_data)
    # print(output_dir)
    cache_df_columns(dataframe = raw_data, filepath= output_dir)
    # print(raw_data.columns)
    raw_data.to_sql(name = 'factor_table',
                    con = engine,
                    index=False,
                    )
    
    engine.close()
    
    get_api_key_info()


import sqlalchemy as sa

metadata = sa.MetaData()

fundamental_schema = sa.Table(
    "factor_table",  # 資料表名稱
    metadata,  # 元資料
    sa.Column("symbol", sa.String, primary_key=True, nullable=False),  # 公司 ID，字串，主鍵
    sa.Column("date", sa.DateTime, primary_key=True, nullable=False),  # 年/月，日期時間，主鍵
    sa.Column("fin_date", sa.DateTime),  # 年/月，日期時間
    sa.Column("share_date", sa.DateTime),  # 年/月，日期時間
    sa.Column("mon_sales_date", sa.DateTime),  # 年/月，日期時間
    sa.Column("High_Low_Diff", sa.Float),  # High_Low_Diff，浮點數
    sa.Column("Transaction", sa.Float),  # Transaction，浮點數
    sa.Column("Turnover", sa.Float),  # Turnover，浮點數
    sa.Column("Market_Cap_Percentage", sa.Float),  # Market_Cap_Percentage，浮點數
    sa.Column("Volume_1000_Shares", sa.Float),  # Volume_1000_Shares，浮點數
    sa.Column("Last_Bid", sa.Float),  # Last_Bid，浮點數
    sa.Column("Adjust_Factor", sa.Float),  # Adjust_Factor，浮點數
    sa.Column("Average_Trade_Price", sa.Float),  # Average_Trade_Price，浮點數
    sa.Column("PBR_TWSE", sa.Float),  # PBR_TWSE，浮點數
    sa.Column("ROI", sa.Float),  # ROI，浮點數
    sa.Column("PBR_TEJ", sa.Float),  # PBR_TEJ，浮點數
    sa.Column("Cash_Dividend_Yield_TEJ", sa.Float),  # Cash_Dividend_Yield_TEJ，浮點數
    sa.Column("Dividend_Yield_TWSE", sa.Float),  # Dividend_Yield_TWSE，浮點數
    sa.Column("PER_TWSE", sa.Float),  # PER_TWSE，浮點數
    sa.Column("PSR_TEJ", sa.Float),  # PSR_TEJ，浮點數
    sa.Column("PER_TEJ", sa.Float),  # PER_TEJ，浮點數
    sa.Column("Listing_Type", sa.String),  # Listing_Type，字串
    sa.Column("Last_Offer", sa.Float),  # Last_Offer，浮點數
    sa.Column("Value_Dollars", sa.Float),  # Value_Dollars，浮點數
    sa.Column("Common_Stock_Shares_Issued_Thousand_Shares", sa.Float),  # Common_Stock_Shares_Issued_Thousand_Shares，浮點數
    sa.Column("Open", sa.Float),  # Open，浮點數
    sa.Column("Low", sa.Float),  # Low，浮點數
    sa.Column("Close", sa.Float),  # Close，浮點數
    sa.Column("High", sa.Float),  # High，浮點數
    sa.Column("Market_Cap_Dollars", sa.Float),  # Market_Cap_Dollars，浮點數
    sa.Column("Trade_Value_Percentage", sa.Float),  # Trade_Value_Percentage，浮點數
    sa.Column("Day_Trading_Volume_1000_Shares", sa.Float),  # Day_Trading_Volume_1000_Shares，浮點數
    sa.Column("Fund_Sell_Vol", sa.Float),  # Fund_Sell_Vol，浮點數
    sa.Column("SBL_Short_Sales_Vol", sa.Float),  # SBL_Short_Sales_Vol，浮點數
    sa.Column("Qfii_Sell_Amt", sa.Float),  # Qfii_Sell_Amt，浮點數
    sa.Column("Total_Buy_Vol", sa.Float),  # Total_Buy_Vol，浮點數
    sa.Column("Dealer_Hedge_Sell_Vol", sa.Float),  # Dealer_Hedge_Sell_Vol，浮點數
    sa.Column("Total_Sell_Vol", sa.Float),  # Total_Sell_Vol，浮點數
    sa.Column("Dealer_Hedge_Diff_Vol", sa.Float),  # Dealer_Hedge_Diff_Vol，浮點數
    sa.Column("Fund_Stock_Holding_Pct", sa.Float),  # Fund_Stock_Holding_Pct，浮點數
    sa.Column("Qfii_Sell_Vol", sa.Float),  # Qfii_Sell_Vol，浮點數
    sa.Column("Qfii_Diff_Amt", sa.Float),  # Qfii_Diff_Amt，浮點數
    sa.Column("Fund_Diff_Amt", sa.Float),  # Fund_Diff_Amt，浮點數
    sa.Column("Total_Buy_Amt", sa.Float),  # Total_Buy_Amt，浮點數
    sa.Column("Margin_Day_Trading_Amt", sa.Float),  # Margin_Day_Trading_Amt，浮點數
    sa.Column("SBL_Short_Sales_Amt", sa.Float),  # SBL_Short_Sales_Amt，浮點數
    sa.Column("Qfii_Stock_Holding_Pct", sa.Float),  # Qfii_Stock_Holding_Pct，浮點數
    sa.Column("Account_Maintenance_Ratio", sa.Float),  # Account_Maintenance_Ratio，浮點數
    sa.Column("Dealer_Hedge_Buy_Amt", sa.Float),  # Dealer_Hedge_Buy_Amt，浮點數
    sa.Column("Margin_Balance_Vol", sa.Float),  # Margin_Balance_Vol，浮點數
    sa.Column("Margin_Sale", sa.Float),  # Margin_Sale，浮點數
    sa.Column("Cash_Redemption", sa.Float),  # Cash_Redemption，浮點數
    sa.Column("Fund_Sell_Amt", sa.Float),  # Fund_Sell_Amt，浮點數
    sa.Column("Total_Diff_Amt", sa.Float),  # Total_Diff_Amt，浮點數
    sa.Column("Qfii_Buy_Amt", sa.Float),  # Qfii_Buy_Amt，浮點數
    sa.Column("SBL_Short_Returns_Vol", sa.Float),  # SBL_Short_Returns_Vol，浮點數
    sa.Column("Margin_Balance_Ratio", sa.Float),  # Margin_Balance_Ratio，浮點數
    sa.Column("Margin_Purchase", sa.Float),  # Margin_Purchase，浮點數
    sa.Column("SBL_Short_Balance_Amt", sa.Float),  # SBL_Short_Balance_Amt，浮點數
    sa.Column("Fund_Buy_Vol", sa.Float),  # Fund_Buy_Vol，浮點數
    sa.Column("Dealer_Hedge_Buy_Vol", sa.Float),  # Dealer_Hedge_Buy_Vol，浮點數
    sa.Column("Margin_Short_Balance_Amt", sa.Float),  # Margin_Short_Balance_Amt，浮點數
    sa.Column("Margin_Short_Maintenance_Ratio", sa.Float),  # Margin_Short_Maintenance_Ratio，浮點數
    sa.Column("Dealer_Proprietary_Buy_Vol", sa.Float),  # Dealer_Proprietary_Buy_Vol，浮點數
    sa.Column("Margin_Quota", sa.Float),  # Margin_Quota，浮點數
    sa.Column("Margin_Short_Balance_Vol", sa.Float),  # Margin_Short_Balance_Vol，浮點數
    sa.Column("Dealer_Proprietary_Buy_Amt", sa.Float),  # Dealer_Proprietary_Buy_Amt，浮點數
    sa.Column("Margin_Maintenance_Ratio", sa.Float),  # Margin_Maintenance_Ratio，浮點數
    sa.Column("SBL_Short_Balance_Vol", sa.Float),  # SBL_Short_Balance_Vol，浮點數
    sa.Column("Margin_Stock_Redemtion", sa.Float),  # Margin_Stock_Redemtion，浮點數
    sa.Column("Qfii_Diff_Vol", sa.Float),  # Qfii_Diff_Vol，浮點數
    sa.Column("Dealer_Hedge_Sell_Amt", sa.Float),  # Dealer_Hedge_Sell_Amt，浮點數
    sa.Column("Dealer_Proprietary_Sell_Vol", sa.Float),  # Dealer_Proprietary_Sell_Vol，浮點數
    sa.Column("Dealer_Proprietary_Diff_Vol", sa.Float),  # Dealer_Proprietary_Diff_Vol，浮點數
    sa.Column("Margin_Day_Trading_Vol", sa.Float),  # Margin_Day_Trading_Vol，浮點數
    sa.Column("Fund_Diff_Vol", sa.Float),  # Fund_Diff_Vol，浮點數
    sa.Column("Margin_Short_Quota", sa.Float),  # Margin_Short_Quota，浮點數
    sa.Column("Margin_Balance_Amt", sa.Float),  # Margin_Balance_Amt，浮點數
    sa.Column("SBL_Short_Quota", sa.Float),  # SBL_Short_Quota，浮點數
    sa.Column("Margin_Short_Sales", sa.Float),  # Margin_Short_Sales，浮點數
    sa.Column("Dealer_Hedge_Diff_Amt", sa.Float),  # Dealer_Hedge_Diff_Amt，浮點數
    sa.Column("Total_Sell_Amt", sa.Float),  # Total_Sell_Amt，浮點數
    sa.Column("Fund_Buy_Amt", sa.Float),  # Fund_Buy_Amt，浮點數
    sa.Column("Dealer_Stock_Holding_Pct", sa.Float),  # Dealer_Stock_Holding_Pct，浮點數
    sa.Column("Qfii_Buy_Vol", sa.Float),  # Qfii_Buy_Vol，浮點數
    sa.Column("Dealer_Proprietary_Sell_Amt", sa.Float),  # Dealer_Proprietary_Sell_Amt，浮點數
    sa.Column("Margin_Short_Coverting", sa.Float),  # Margin_Short_Coverting，浮點數
    sa.Column("Dealer_Proprietary_Diff_Amt", sa.Float),  # Dealer_Proprietary_Diff_Amt，浮點數
    sa.Column("Total_Diff_Vol", sa.Float),  # Total_Diff_Vol，浮點數
    sa.Column("Day_Trading_Pct", sa.Float),  # Day_Trading_Pct，浮點數
    sa.Column("Security_Type_Chinese", sa.String),  # Security_Type_Chinese，字串
    sa.Column("Component_Stock_of_High_Dividend_Fg", sa.String),  # Component_Stock_of_High_Dividend_Fg，字串
    sa.Column("Matching_Period", sa.Float),  # Matching_Period，浮點數
    sa.Column("Suspended_Trading_Stock_Fg", sa.String),  # Suspended_Trading_Stock_Fg，字串
    sa.Column("Disposition_Stock_Fg", sa.String),  # Disposition_Stock_Fg，字串
    sa.Column("Limit_Up_or_Down_in_Opening_Fg", sa.String),  # Limit_Up_or_Down_in_Opening_Fg，字串
    sa.Column("Suspension_of_Buy_After_Day_Trading_Fg", sa.String),  # Suspension_of_Buy_After_Day_Trading_Fg，字串
    sa.Column("Market_Board", sa.String),  # Market_Board，字串
    sa.Column("Industry", sa.String),  # Industry，字串
    sa.Column("Component_Stock_of_TPEx50_Fg", sa.String),  # Component_Stock_of_TPEx50_Fg，字串
    sa.Column("Component_Stock_of_MidCap100_Fg", sa.String),  # Component_Stock_of_MidCap100_Fg，字串
    sa.Column("Limit_Up_or_Down", sa.String),  # Limit_Up_or_Down，字串
    sa.Column("Component_Stock_of_MSCI_TW_Fg", sa.String),  # Component_Stock_of_MSCI_TW_Fg，字串
    sa.Column("Component_Stock_of_TWN50_Fg", sa.String),  # Component_Stock_of_TWN50_Fg，字串
    sa.Column("Attention_Stock_Fg", sa.String),  # Attention_Stock_Fg，字串
    sa.Column("Security_Type_English", sa.String),  # Security_Type_English，字串
    sa.Column("Industry_Eng", sa.String),  # Industry_Eng，字串
    sa.Column("Full_Delivery_Stock_Fg", sa.String),  # Full_Delivery_Stock_Fg，字串
    sa.Column("Component_Stock_of_TPEx200_Fg", sa.String),  # Component_Stock_of_TPEx200_Fg，字串
    sa.Column("Sales_Monthly_LastYear", sa.Float),  # Sales_Monthly_LastYear，浮點數
    sa.Column("Sales_Accu_12M", sa.Float),  # Sales_Accu_12M，浮點數
    sa.Column("YoY_AccuSales_12M", sa.Float),  # YoY_AccuSales_12M，浮點數
    sa.Column("YoY_Monthly_Sales", sa.Float),  # YoY_Monthly_Sales，浮點數
    sa.Column("MoM_Monthly_Sales", sa.Float),  # MoM_Monthly_Sales，浮點數
    sa.Column("YoY_Accu_Sales_3M", sa.Float),  # YoY_Accu_Sales_3M，浮點數
    sa.Column("YoY_Accu_Sales", sa.Float),  # YoY_Accu_Sales，浮點數
    sa.Column("Lowest_Monthly_Sales", sa.Float),  # Lowest_Monthly_Sales，浮點數
    sa.Column("Sales_Monthly", sa.Float),  # Sales_Monthly，浮點數
    sa.Column("Sales_Accu_LastYear", sa.Float),  # Sales_Accu_LastYear，浮點數
    sa.Column("Highest_Monthly_Sales", sa.Float),  # Highest_Monthly_Sales，浮點數
    sa.Column("Outstanding_Shares_1000_Shares", sa.Float),  # Outstanding_Shares_1000_Shares，浮點數
    sa.Column("Sales_Per_Share_Accu_3M", sa.Float),  # Sales_Per_Share_Accu_3M，浮點數
    sa.Column("Highest_Monthly_Sales_YM", sa.DateTime),  # Highest_Monthly_Sales_YM，日期時間
    sa.Column("MoM_Accu_Sales_3M", sa.Float),  # MoM_Accu_Sales_3M，浮點數
    sa.Column("Highest_Or_Lowest_In_12M", sa.String),  # Highest_Or_Lowest_In_12M，字串
    sa.Column("Month_Sales_Compared_To_Lowest_Month_Sales_MoM", sa.Float),  # Month_Sales_Compared_To_Lowest_Month_Sales_MoM，浮點數
    sa.Column("Sales_Per_Share_Accu", sa.Float),  # Sales_Per_Share_Accu，浮點數
    sa.Column("Highest_Or_Lowest_All_Time", sa.String),  # Highest_Or_Lowest_All_Time，字串
    sa.Column("Highest_Or_Lowest_Same_Month", sa.String),  # Highest_Or_Lowest_Same_Month，字串
    sa.Column("QoQ_Accu_Sales_3M", sa.Float),  # QoQ_Accu_Sales_3M，浮點數
    sa.Column("Sales_Per_Share_Accu_12M", sa.Float),  # Sales_Per_Share_Accu_12M，浮點數
    sa.Column("Sales_Accu_12M_Last_Year", sa.Float),  # Sales_Accu_12M_Last_Year，浮點數
    sa.Column("Sales_Accumulated", sa.Float),  # Sales_Accumulated，浮點數
    sa.Column("Sales_Accu_3M_LastYear", sa.Float),  # Sales_Accu_3M_LastYear，浮點數
    sa.Column("Month_Sales_Compared_To_High_Month_Sales_MoM", sa.Float),  # Month_Sales_Compared_To_High_Month_Sales_MoM，浮點數
    sa.Column("Sales_Accu_3M", sa.Float),  # Sales_Accu_3M，浮點數
    sa.Column("Lowest_Monthly_Sales_YM", sa.DateTime),  # Lowest_Monthly_Sales_YM，日期時間
    sa.Column("Sales_Per_Share_Single_Month", sa.Float),  # Sales_Per_Share_Single_Month，浮點數
    sa.Column("Hit_New_High_Or_Low_In_N_Months", sa.Float),  # Hit_New_High_Or_Low_In_N_Months，浮點數
    sa.Column("Custodied_Lots_Between_400_600_Total_Holders", sa.Float),  # Custodied_Lots_Between_400_600_Total_Holders，浮點數
    sa.Column("Custodied_Lots_Between_600_800_Total_Holders", sa.Float),  # Custodied_Lots_Between_600_800_Total_Holders，浮點數
    sa.Column("Custodied_Larger_Than_400_Lots_Pct", sa.Float),  # Custodied_Larger_Than_400_Lots_Pct，浮點數
    sa.Column("Custodied_Lots_Between_800_1000_Pct", sa.Float),  # Custodied_Lots_Between_800_1000_Pct，浮點數
    sa.Column("Pledged_Stock_Shares_1000_Lots", sa.Float),  # Pledged_Stock_Shares_1000_Lots，浮點數
    sa.Column("Custodied_Under_400_Lots_Total_Holders", sa.Float),  # Custodied_Under_400_Lots_Total_Holders，浮點數
    sa.Column("Custodied_Lots_Between_400_600_Pct", sa.Float),  # Custodied_Lots_Between_400_600_Pct，浮點數
    sa.Column("Custodied_Lots_Between_800_1000_Total_Lots", sa.Float),  # Custodied_Lots_Between_800_1000_Total_Lots，浮點數
    sa.Column("Custodied_Greater_Than_1000_Lots_Pct", sa.Float),  # Custodied_Greater_Than_1000_Lots_Pct，浮點數
    sa.Column("Custodied_Lots_Between_400_600_Total_Lots", sa.Float),  # Custodied_Lots_Between_400_600_Total_Lots，浮點數
    sa.Column("Custodied_Greater_Than_1000_Lots_Total_Lots", sa.Float),  # Custodied_Greater_Than_1000_Lots_Total_Lots，浮點數
    sa.Column("Total_Custodied_Shares_1000_Lots", sa.Float),  # Total_Custodied_Shares_1000_Lots，浮點數
    sa.Column("Custodied_Larger_Than_400_Lots_Total_Lots", sa.Float),  # Custodied_Larger_Than_400_Lots_Total_Lots，浮點數
    sa.Column("Custodied_Under_400_Lots_Pct", sa.Float),  # Custodied_Under_400_Lots_Pct，浮點數
    sa.Column("Custodied_Under_400_Lots_Total_Lots", sa.Float),  # Custodied_Under_400_Lots_Total_Lots，浮點數
    sa.Column("Custodied_Lots_Between_800_1000_Total_Holders", sa.Float),  # Custodied_Lots_Between_800_1000_Total_Holders，浮點數
    sa.Column("Custodied_Greater_Than_1000_Lots_Total_Holders", sa.Float),  # Custodied_Greater_Than_1000_Lots_Total_Holders，浮點數
    sa.Column("Custodied_Lots_Between_600_800_Total_Lots", sa.Float),  # Custodied_Lots_Between_600_800_Total_Lots，浮點數
    sa.Column("Custodied_Lots_Between_600_800_Pct", sa.Float),  # Custodied_Lots_Between_600_800_Pct，浮點數
    sa.Column("Custodied_Larger_Than_400_Lots_Total_Holders", sa.Float),  # Custodied_Larger_Than_400_Lots_Total_Holders，浮點數
    # 財務資料
    sa.Column("Total_Operating_Income_A", sa.Float),  # Total_Operating_Income_A，浮點數
    sa.Column("Total_Operating_Income_Q", sa.Float),  # Total_Operating_Income_Q，浮點數
    sa.Column("Total_Operating_Income_TTM", sa.Float),  # Total_Operating_Income_TTM，浮點數
    sa.Column("Net_Income_Rate_percent_A", sa.Float),  # Net_Income_Rate_percent_A，浮點數
    sa.Column("Net_Income_Rate_percent_Q", sa.Float),  # Net_Income_Rate_percent_Q，浮點數
    sa.Column("Net_Income_Rate_percent_TTM", sa.Float),  # Net_Income_Rate_percent_TTM，浮點數
    sa.Column("Sales_Growth_Rate_A", sa.Float),  # Sales_Growth_Rate_A，浮點數
    sa.Column("Sales_Growth_Rate_Q", sa.Float),  # Sales_Growth_Rate_Q，浮點數
    sa.Column("Sales_Growth_Rate_TTM", sa.Float),  # Sales_Growth_Rate_TTM，浮點數
    sa.Column("Pre_Tax_Income_Growth_Rate_A", sa.Float),  # Pre_Tax_Income_Growth_Rate_A，浮點數
    sa.Column("Pre_Tax_Income_Growth_Rate_Q", sa.Float),  # Pre_Tax_Income_Growth_Rate_Q，浮點數
    sa.Column("Pre_Tax_Income_Growth_Rate_TTM", sa.Float),  # Pre_Tax_Income_Growth_Rate_TTM，浮點數
    sa.Column("Gross_Margin_Growth_Rate_A", sa.Float),  # Gross_Margin_Growth_Rate_A，浮點數
    sa.Column("Gross_Margin_Growth_Rate_Q", sa.Float),  # Gross_Margin_Growth_Rate_Q，浮點數
    sa.Column("Gross_Margin_Growth_Rate_TTM", sa.Float),  # Gross_Margin_Growth_Rate_TTM，浮點數
    sa.Column("Profit_Before_Tax_A", sa.Float),  # Profit_Before_Tax_A，浮點數
    sa.Column("Profit_Before_Tax_Q", sa.Float),  # Profit_Before_Tax_Q，浮點數
    sa.Column("Profit_Before_Tax_TTM", sa.Float),  # Profit_Before_Tax_TTM，浮點數
    sa.Column("Operating_Income_Growth_Rate_A", sa.Float),  # Operating_Income_Growth_Rate_A，浮點數
    sa.Column("Operating_Income_Growth_Rate_Q", sa.Float),  # Operating_Income_Growth_Rate_Q，浮點數
    sa.Column("Operating_Income_Growth_Rate_TTM", sa.Float),  # Operating_Income_Growth_Rate_TTM，浮點數
    sa.Column("Pre_Tax_Income_Rate_percent_A", sa.Float),  # Pre_Tax_Income_Rate_percent_A，浮點數
    sa.Column("Pre_Tax_Income_Rate_percent_Q", sa.Float),  # Pre_Tax_Income_Rate_percent_Q，浮點數
    sa.Column("Pre_Tax_Income_Rate_percent_TTM", sa.Float),  # Pre_Tax_Income_Rate_percent_TTM，浮點數
    sa.Column("Gross_Profit_Loss_from_Operations_A", sa.Float),  # Gross_Profit_Loss_from_Operations_A，浮點數
    sa.Column("Gross_Profit_Loss_from_Operations_Q", sa.Float),  # Gross_Profit_Loss_from_Operations_Q，浮點數
    sa.Column("Gross_Profit_Loss_from_Operations_TTM", sa.Float),  # Gross_Profit_Loss_from_Operations_TTM，浮點數
    sa.Column("Pre_Tax_Income_Per_Share_A", sa.Float),  # Pre_Tax_Income_Per_Share_A，浮點數
    sa.Column("Pre_Tax_Income_Per_Share_Q", sa.Float),  # Pre_Tax_Income_Per_Share_Q，浮點數
    sa.Column("Pre_Tax_Income_Per_Share_TTM", sa.Float),  # Pre_Tax_Income_Per_Share_TTM，浮點數
    sa.Column("Operating_Income_Rate_percent_A", sa.Float),  # Operating_Income_Rate_percent_A，浮點數
    sa.Column("Operating_Income_Rate_percent_Q", sa.Float),  # Operating_Income_Rate_percent_Q，浮點數
    sa.Column("Operating_Income_Rate_percent_TTM", sa.Float),  # Operating_Income_Rate_percent_TTM，浮點數
    sa.Column("Gross_Margin_Rate_percent_A", sa.Float),  # Gross_Margin_Rate_percent_A，浮點數
    sa.Column("Gross_Margin_Rate_percent_Q", sa.Float),  # Gross_Margin_Rate_percent_Q，浮點數
    sa.Column("Gross_Margin_Rate_percent_TTM", sa.Float),  # Gross_Margin_Rate_percent_TTM，浮點數
    sa.Column("Net_Income_Loss_A", sa.Float),  # Net_Income_Loss_A，浮點數
    sa.Column("Net_Income_Loss_Q", sa.Float),  # Net_Income_Loss_Q，浮點數
    sa.Column("Net_Income_Loss_TTM", sa.Float),  # Net_Income_Loss_TTM，浮點數
    sa.Column("Net_Income_Attributable_to_Parent_A", sa.Float),  # Net_Income_Attributable_to_Parent_A，浮點數
    sa.Column("Net_Income_Attributable_to_Parent_Q", sa.Float),  # Net_Income_Attributable_to_Parent_Q，浮點數
    sa.Column("Net_Income_Attributable_to_Parent_TTM", sa.Float),  # Net_Income_Attributable_to_Parent_TTM，浮點數
    sa.Column("Net_Income_Growth_Rate_A", sa.Float),  # Net_Income_Growth_Rate_A，浮點數
    sa.Column("Net_Income_Growth_Rate_Q", sa.Float),  # Net_Income_Growth_Rate_Q，浮點數
    sa.Column("Net_Income_Growth_Rate_TTM", sa.Float),  # Net_Income_Growth_Rate_TTM，浮點數
    sa.Column("Basic_Earnings_Per_Share_A", sa.Float),  # Basic_Earnings_Per_Share_A，浮點數
    sa.Column("Basic_Earnings_Per_Share_Q", sa.Float),  # Basic_Earnings_Per_Share_Q，浮點數
    sa.Column("Basic_Earnings_Per_Share_TTM", sa.Float),  # Basic_Earnings_Per_Share_TTM，浮點數
    sa.Column("Current_Ratio_A", sa.Float),  # Current_Ratio_A，浮點數
    sa.Column("Current_Ratio_Q", sa.Float),  # Current_Ratio_Q，浮點數
    sa.Column("Current_Ratio_TTM", sa.Float),  # Current_Ratio_TTM，浮點數
    sa.Column("Total_Operating_Expenses_A", sa.Float),  # Total_Operating_Expenses_A，浮點數
    sa.Column("Total_Operating_Expenses_Q", sa.Float),  # Total_Operating_Expenses_Q，浮點數
    sa.Column("Total_Operating_Expenses_TTM", sa.Float),  # Total_Operating_Expenses_TTM，浮點數
    sa.Column("Cash_Flow_from_Investing_Activities_A", sa.Float),  # Cash_Flow_from_Investing_Activities_A，浮點數
    sa.Column("Cash_Flow_from_Investing_Activities_Q", sa.Float),  # Cash_Flow_from_Investing_Activities_Q，浮點數
    sa.Column("Cash_Flow_from_Investing_Activities_TTM", sa.Float),  # Cash_Flow_from_Investing_Activities_TTM，浮點數
    sa.Column("Total_Non_current_Liabilities_A", sa.Float),  # Total_Non_current_Liabilities_A，浮點數
    sa.Column("Total_Non_current_Liabilities_Q", sa.Float),  # Total_Non_current_Liabilities_Q，浮點數
    sa.Column("Total_Non_current_Liabilities_TTM", sa.Float),  # Total_Non_current_Liabilities_TTM，浮點數
    sa.Column("Return_on_Total_Assets_A_percent_A", sa.Float),  # Return_on_Total_Assets_A_percent_A，浮點數
    sa.Column("Return_on_Total_Assets_A_percent_Q", sa.Float),  # Return_on_Total_Assets_A_percent_Q，浮點數
    sa.Column("Return_on_Total_Assets_A_percent_TTM", sa.Float),  # Return_on_Total_Assets_A_percent_TTM，浮點數
    sa.Column("Preferred_Stocks_A", sa.Float),  # Preferred_Stocks_A，浮點數
    sa.Column("Preferred_Stocks_Q", sa.Float),  # Preferred_Stocks_Q，浮點數
    sa.Column("Preferred_Stocks_TTM", sa.Float),  # Preferred_Stocks_TTM，浮點數
    sa.Column("Total_Liabilities_A", sa.Float),  # Total_Liabilities_A，浮點數
    sa.Column("Total_Liabilities_Q", sa.Float),  # Total_Liabilities_Q，浮點數
    sa.Column("Total_Liabilities_TTM", sa.Float),  # Total_Liabilities_TTM，浮點數
    sa.Column("Borrowings_A", sa.Float),  # Borrowings_A，浮點數
    sa.Column("Borrowings_Q", sa.Float),  # Borrowings_Q，浮點數
    sa.Column("Borrowings_TTM", sa.Float),  # Borrowings_TTM，浮點數
    sa.Column("Interest_Expense_Rate_percent_A", sa.Float),  # Interest_Expense_Rate_percent_A，浮點數
    sa.Column("Interest_Expense_Rate_percent_Q", sa.Float),  # Interest_Expense_Rate_percent_Q，浮點數
    sa.Column("Interest_Expense_Rate_percent_TTM", sa.Float),  # Interest_Expense_Rate_percent_TTM，浮點數
    sa.Column("Depreciation_and_Amortisation_A", sa.Float),  # Depreciation_and_Amortisation_A，浮點數
    sa.Column("Depreciation_and_Amortisation_Q", sa.Float),  # Depreciation_and_Amortisation_Q，浮點數
    sa.Column("Depreciation_and_Amortisation_TTM", sa.Float),  # Depreciation_and_Amortisation_TTM，浮點數
    sa.Column("Cash_and_Cash_Equivalent_A", sa.Float),  # Cash_and_Cash_Equivalent_A，浮點數
    sa.Column("Cash_and_Cash_Equivalent_Q", sa.Float),  # Cash_and_Cash_Equivalent_Q，浮點數
    sa.Column("Cash_and_Cash_Equivalent_TTM", sa.Float),  # Cash_and_Cash_Equivalent_TTM，浮點數
    sa.Column("Accounts_Payable_A", sa.Float),  # Accounts_Payable_A，浮點數
    sa.Column("Accounts_Payable_Q", sa.Float),  # Accounts_Payable_Q，浮點數
    sa.Column("Accounts_Payable_TTM", sa.Float),  # Accounts_Payable_TTM，浮點數
    sa.Column("Days_Receivables_Outstanding_A", sa.Float),  # Days_Receivables_Outstanding_A，浮點數
    sa.Column("Days_Receivables_Outstanding_Q", sa.Float),  # Days_Receivables_Outstanding_Q，浮點數
    sa.Column("Days_Receivables_Outstanding_TTM", sa.Float),  # Days_Receivables_Outstanding_TTM，浮點數
    sa.Column("Long_Term_Borrowings_Non_Financial_Institutions_A", sa.Float),  # Long_Term_Borrowings_Non_Financial_Institutions_A，浮點數
    sa.Column("Long_Term_Borrowings_Non_Financial_Institutions_Q", sa.Float),  # Long_Term_Borrowings_Non_Financial_Institutions_Q，浮點數
    sa.Column("Long_Term_Borrowings_Non_Financial_Institutions_TTM", sa.Float),  # Long_Term_Borrowings_Non_Financial_Institutions_TTM，浮點數
    sa.Column("Taxrate_A", sa.Float),  # Taxrate_A，浮點數
    sa.Column("Taxrate_Q", sa.Float),  # Taxrate_Q，浮點數
    sa.Column("Taxrate_TTM", sa.Float),  # Taxrate_TTM，浮點數
    sa.Column("Earnings_Before_Interest_and_Tax_A", sa.Float),  # Earnings_Before_Interest_and_Tax_A，浮點數
    sa.Column("Earnings_Before_Interest_and_Tax_Q", sa.Float),  # Earnings_Before_Interest_and_Tax_Q，浮點數
    sa.Column("Earnings_Before_Interest_and_Tax_TTM", sa.Float),  # Earnings_Before_Interest_and_Tax_TTM，浮點數
    sa.Column("Non_Operating_Income_A", sa.Float),  # Non_Operating_Income_A，浮點數
    sa.Column("Non_Operating_Income_Q", sa.Float),  # Non_Operating_Income_Q，浮點數
    sa.Column("Non_Operating_Income_TTM", sa.Float),  # Non_Operating_Income_TTM，浮點數
    sa.Column("Interest_Expense_A", sa.Float),  # Interest_Expense_A，浮點數
    sa.Column("Interest_Expense_Q", sa.Float),  # Interest_Expense_Q，浮點數
    sa.Column("Interest_Expense_TTM", sa.Float),  # Interest_Expense_TTM，浮點數
    sa.Column("Total_Current_Liabilities_A", sa.Float),  # Total_Current_Liabilities_A，浮點數
    sa.Column("Total_Current_Liabilities_Q", sa.Float),  # Total_Current_Liabilities_Q，浮點數
    sa.Column("Total_Current_Liabilities_TTM", sa.Float),  # Total_Current_Liabilities_TTM，浮點數
    sa.Column("Net_Operating_Income_Loss_A", sa.Float),  # Net_Operating_Income_Loss_A，浮點數
    sa.Column("Net_Operating_Income_Loss_Q", sa.Float),  # Net_Operating_Income_Loss_Q，浮點數
    sa.Column("Net_Operating_Income_Loss_TTM", sa.Float),  # Net_Operating_Income_Loss_TTM，浮點數
    sa.Column("Non_Controlling_Interest_A", sa.Float),  # Non_Controlling_Interest_A，浮點數
    sa.Column("Non_Controlling_Interest_Q", sa.Float),  # Non_Controlling_Interest_Q，浮點數
    sa.Column("Non_Controlling_Interest_TTM", sa.Float),  # Non_Controlling_Interest_TTM，浮點數
    sa.Column("Total_Non_Current_Assets_A", sa.Float),  # Total_Non_Current_Assets_A，浮點數
    sa.Column("Total_Non_Current_Assets_Q", sa.Float),  # Total_Non_Current_Assets_Q，浮點數
    sa.Column("Total_Non_Current_Assets_TTM", sa.Float),  # Total_Non_Current_Assets_TTM，浮點數
    sa.Column("Number_of_Employees_A", sa.Float),  # Number_of_Employees_A，浮點數
    sa.Column("Number_of_Employees_Q", sa.Float),  # Number_of_Employees_Q，浮點數
    sa.Column("Number_of_Employees_TTM", sa.Float),  # Number_of_Employees_TTM，浮點數
    sa.Column("Total_Assets_Growth_Rate_A", sa.Float),  # Total_Assets_Growth_Rate_A，浮點數
    sa.Column("Total_Assets_Growth_Rate_Q", sa.Float),  # Total_Assets_Growth_Rate_Q，浮點數
    sa.Column("Total_Assets_Growth_Rate_TTM", sa.Float),  # Total_Assets_Growth_Rate_TTM，浮點數
    sa.Column("Prepayments_A", sa.Float),  # Prepayments_A，浮點數
    sa.Column("Prepayments_Q", sa.Float),  # Prepayments_Q，浮點數
    sa.Column("Prepayments_TTM", sa.Float),  # Prepayments_TTM，浮點數
    sa.Column("Times_Interest_Earned_A", sa.Float),  # Times_Interest_Earned_A，浮點數
    sa.Column("Times_Interest_Earned_Q", sa.Float),  # Times_Interest_Earned_Q，浮點數
    sa.Column("Times_Interest_Earned_TTM", sa.Float),  # Times_Interest_Earned_TTM，浮點數
    sa.Column("Total_Assets_A", sa.Float),  # Total_Assets_A，浮點數
    sa.Column("Total_Assets_Q", sa.Float),  # Total_Assets_Q，浮點數
    sa.Column("Total_Assets_TTM", sa.Float),  # Total_Assets_TTM，浮點數
    sa.Column("Total_Assets_Turnover_A", sa.Float),  # Total_Assets_Turnover_A，浮點數
    sa.Column("Total_Assets_Turnover_Q", sa.Float),  # Total_Assets_Turnover_Q，浮點數
    sa.Column("Total_Assets_Turnover_TTM", sa.Float),  # Total_Assets_Turnover_TTM，浮點數
    sa.Column("Common_Stocks_A", sa.Float),  # Common_Stocks_A，浮點數
    sa.Column("Common_Stocks_Q", sa.Float),  # Common_Stocks_Q，浮點數
    sa.Column("Common_Stocks_TTM", sa.Float),  # Common_Stocks_TTM，浮點數
    sa.Column("Cash_Flow_from_Operating_Ratio_A", sa.Float),  # Cash_Flow_from_Operating_Ratio_A，浮點數
    sa.Column("Cash_Flow_from_Operating_Ratio_Q", sa.Float),  # Cash_Flow_from_Operating_Ratio_Q，浮點數
    sa.Column("Cash_Flow_from_Operating_Ratio_TTM", sa.Float),  # Cash_Flow_from_Operating_Ratio_TTM，浮點數
    sa.Column("Acid_Test_A", sa.Float),  # Acid_Test_A，浮點數
    sa.Column("Acid_Test_Q", sa.Float),  # Acid_Test_Q，浮點數
    sa.Column("Acid_Test_TTM", sa.Float),  # Acid_Test_TTM，浮點數
    sa.Column("Net_Non_operating_Income_Ratio_A", sa.Float),  # Net_Non_operating_Income_Ratio_A，浮點數
    sa.Column("Net_Non_operating_Income_Ratio_Q", sa.Float),  # Net_Non_operating_Income_Ratio_Q，浮點數
    sa.Column("Net_Non_operating_Income_Ratio_TTM", sa.Float),  # Net_Non_operating_Income_Ratio_TTM，浮點數
    sa.Column("Sales_Per_Employee_A", sa.Float),
    sa.Column("Sales_Per_Employee_Q", sa.Float),
    sa.Column("Sales_Per_Employee_TTM", sa.Float),
    sa.Column("Accounts_Receivable_Turnover_A", sa.Float),
    sa.Column("Accounts_Receivable_Turnover_Q", sa.Float),
    sa.Column("Accounts_Receivable_Turnover_TTM", sa.Float),
    sa.Column("Capital_Reserves_A", sa.Float),
    sa.Column("Capital_Reserves_Q", sa.Float),
    sa.Column("Capital_Reserves_TTM", sa.Float),
    sa.Column("Days_Inventory_Outstanding_A", sa.Float),
    sa.Column("Days_Inventory_Outstanding_Q", sa.Float),
    sa.Column("Days_Inventory_Outstanding_TTM", sa.Float),
    sa.Column("Non_Recurring_Net_Income_A", sa.Float),
    sa.Column("Non_Recurring_Net_Income_Q", sa.Float),
    sa.Column("Non_Recurring_Net_Income_TTM", sa.Float),
    sa.Column("Accounts_Payable_Turnover_A", sa.Float),
    sa.Column("Accounts_Payable_Turnover_Q", sa.Float),
    sa.Column("Accounts_Payable_Turnover_TTM", sa.Float),
    sa.Column("Total_Interest_Income_A", sa.Float),
    sa.Column("Total_Interest_Income_Q", sa.Float),
    sa.Column("Total_Interest_Income_TTM", sa.Float),
    sa.Column("Quick_Assets_A", sa.Float),
    sa.Column("Quick_Assets_Q", sa.Float),
    sa.Column("Quick_Assets_TTM", sa.Float),
    sa.Column("Preferred_Stock_Dividends_A", sa.Float),
    sa.Column("Preferred_Stock_Dividends_Q", sa.Float),
    sa.Column("Preferred_Stock_Dividends_TTM", sa.Float),
    sa.Column("Total_Current_Assets_A", sa.Float),
    sa.Column("Total_Current_Assets_Q", sa.Float),
    sa.Column("Total_Current_Assets_TTM", sa.Float),
    sa.Column("Intangible_Assets_A", sa.Float),
    sa.Column("Intangible_Assets_Q", sa.Float),
    sa.Column("Intangible_Assets_TTM", sa.Float),
    sa.Column("Short_Term_Borrowings_Financial_Institutions_A", sa.Float),
    sa.Column("Short_Term_Borrowings_Financial_Institutions_Q", sa.Float),
    sa.Column("Short_Term_Borrowings_Financial_Institutions_TTM", sa.Float),
    sa.Column("Short_Term_Borrowings_Non_Financial_Institutions_A", sa.Float),
    sa.Column("Short_Term_Borrowings_Non_Financial_Institutions_Q", sa.Float),
    sa.Column("Short_Term_Borrowings_Non_Financial_Institutions_TTM", sa.Float),
    sa.Column("Cash_Flow_from_Operating_Activities_A", sa.Float),
    sa.Column("Cash_Flow_from_Operating_Activities_Q", sa.Float),
    sa.Column("Cash_Flow_from_Operating_Activities_TTM", sa.Float),
    sa.Column("Inventory_Turnover_A", sa.Float),
    sa.Column("Inventory_Turnover_Q", sa.Float),
    sa.Column("Inventory_Turnover_TTM", sa.Float),
    sa.Column("Liabilities_Ratio_A", sa.Float),
    sa.Column("Liabilities_Ratio_Q", sa.Float),
    sa.Column("Liabilities_Ratio_TTM", sa.Float),
    sa.Column("Total_Fixed_Assets_A", sa.Float),
    sa.Column("Total_Fixed_Assets_Q", sa.Float),
    sa.Column("Total_Fixed_Assets_TTM", sa.Float),
    sa.Column("Fixed_Assets_A", sa.Float),
    sa.Column("Fixed_Assets_Q", sa.Float),
    sa.Column("Fixed_Assets_TTM", sa.Float),
    sa.Column("Advances_Receipts_Non_Current_A", sa.Float),
    sa.Column("Advances_Receipts_Non_Current_Q", sa.Float),
    sa.Column("Advances_Receipts_Non_Current_TTM", sa.Float),
    sa.Column("Fixed_Asset_Turnover_A", sa.Float),
    sa.Column("Fixed_Asset_Turnover_Q", sa.Float),
    sa.Column("Fixed_Asset_Turnover_TTM", sa.Float),
    sa.Column("Total_Liabilities_and_Equity_A", sa.Float),
    sa.Column("Total_Liabilities_and_Equity_Q", sa.Float),
    sa.Column("Total_Liabilities_and_Equity_TTM", sa.Float),
    sa.Column("Weighted_Average_Outstanding_Shares_Thousand_A", sa.Float),
    sa.Column("Weighted_Average_Outstanding_Shares_Thousand_Q", sa.Float),
    sa.Column("Weighted_Average_Outstanding_Shares_Thousand_TTM", sa.Float),
    sa.Column("Long_Term_Borrowings_Financial_Institutions_A", sa.Float),
    sa.Column("Long_Term_Borrowings_Financial_Institutions_Q", sa.Float),
    sa.Column("Long_Term_Borrowings_Financial_Institutions_TTM", sa.Float),
    sa.Column("Inventories_A", sa.Float),
    sa.Column("Inventories_Q", sa.Float),
    sa.Column("Inventories_TTM", sa.Float),
    sa.Column("Depreciable_Fixed_Assets_Growth_Rate_A", sa.Float),
    sa.Column("Depreciable_Fixed_Assets_Growth_Rate_Q", sa.Float),
    sa.Column("Depreciable_Fixed_Assets_Growth_Rate_TTM", sa.Float),
    sa.Column("Equity_Turnover_A", sa.Float),
    sa.Column("Equity_Turnover_Q", sa.Float),
    sa.Column("Equity_Turnover_TTM", sa.Float),
    sa.Column("Book_Value_Per_Share_A_A", sa.Float),
    sa.Column("Book_Value_Per_Share_A_Q", sa.Float),
    sa.Column("Book_Value_Per_Share_A_TTM", sa.Float),
    sa.Column("Days_Payables_Outstanding_A", sa.Float),
    sa.Column("Days_Payables_Outstanding_Q", sa.Float),
    sa.Column("Days_Payables_Outstanding_TTM", sa.Float),
    sa.Column("Other_Accounts_Payable_A", sa.Float),
    sa.Column("Other_Accounts_Payable_Q", sa.Float),
    sa.Column("Other_Accounts_Payable_TTM", sa.Float),
    sa.Column("Total_Equity_Growth_Rate_A", sa.Float),
    sa.Column("Total_Equity_Growth_Rate_Q", sa.Float),
    sa.Column("Total_Equity_Growth_Rate_TTM", sa.Float),
    sa.Column("Return_Rate_on_Equity_A_percent_A", sa.Float),
    sa.Column("Return_Rate_on_Equity_A_percent_Q", sa.Float),
    sa.Column("Return_Rate_on_Equity_A_percent_TTM", sa.Float),
    sa.Column("Total_Other_Equity_Interest_A", sa.Float),
    sa.Column("Total_Other_Equity_Interest_Q", sa.Float),
    sa.Column("Total_Other_Equity_Interest_TTM", sa.Float),
    sa.Column("Other_Receivables_A", sa.Float),
    sa.Column("Other_Receivables_Q", sa.Float),
    sa.Column("Other_Receivables_TTM", sa.Float),
    sa.Column("Income_Tax_Expense_A", sa.Float),
    sa.Column("Income_Tax_Expense_Q", sa.Float),
    sa.Column("Income_Tax_Expense_TTM", sa.Float),
    sa.Column("Total_Equity_A", sa.Float),
    sa.Column("Total_Equity_Q", sa.Float),
    sa.Column("Total_Equity_TTM", sa.Float),
    sa.Column("Accounts_Receivable_Current_and_Non_Current_A", sa.Float),
    sa.Column("Accounts_Receivable_Current_and_Non_Current_Q", sa.Float),
    sa.Column("Accounts_Receivable_Current_and_Non_Current_TTM", sa.Float),
    sa.Column("Sales_Per_Share_A", sa.Float),
    sa.Column("Sales_Per_Share_Q", sa.Float),
    sa.Column("Sales_Per_Share_TTM", sa.Float),
    sa.Column("Cash_Flow_from_Financing_Activities_A", sa.Float),
    sa.Column("Cash_Flow_from_Financing_Activities_Q", sa.Float),
    sa.Column("Cash_Flow_from_Financing_Activities_TTM", sa.Float),
    sa.Column("Debt_Equity_Ratio_A", sa.Float),
    sa.Column("Debt_Equity_Ratio_Q", sa.Float),
    sa.Column("Debt_Equity_Ratio_TTM", sa.Float),
    sa.Column("Operating_Expenses_Ratio_A", sa.Float),
    sa.Column("Operating_Expenses_Ratio_Q", sa.Float),
    sa.Column("Operating_Expenses_Ratio_TTM", sa.Float),
    sa.Column("Net_Income_Per_Share_A", sa.Float),
    sa.Column("Net_Income_Per_Share_Q", sa.Float),
    sa.Column("Net_Income_Per_Share_TTM", sa.Float),
    sa.Column("Advances_Receipts_Current_A", sa.Float),
    sa.Column("Advances_Receipts_Current_Q", sa.Float),
    sa.Column("Advances_Receipts_Current_TTM", sa.Float),
    sa.Column("Common_Stock_Shares_Issued_Thousand_Shares_A", sa.Float),
    sa.Column("Common_Stock_Shares_Issued_Thousand_Shares_Q", sa.Float),
    sa.Column("Common_Stock_Shares_Issued_Thousand_Shares_TTM", sa.Float),
    sa.Column("Long_Term_Accounts_Receivable_A", sa.Float),
    sa.Column("Long_Term_Accounts_Receivable_Q", sa.Float),
    sa.Column("Long_Term_Accounts_Receivable_TTM", sa.Float),
    sa.Column("Total_Retained_Earnings_A", sa.Float),
    sa.Column("Total_Retained_Earnings_Q", sa.Float),
    sa.Column("Total_Retained_Earnings_TTM", sa.Float),
    sa.Column("Recurring_Net_Income_A", sa.Float),
    sa.Column("Recurring_Net_Income_Q", sa.Float),
    sa.Column("Recurring_Net_Income_TTM", sa.Float),
    sa.Column("Accounts_Receivable_A", sa.Float),
    sa.Column("Accounts_Receivable_Q", sa.Float),
    sa.Column("Accounts_Receivable_TTM", sa.Float),
    sa.Column("Total_Operating_Cost_A", sa.Float),
    sa.Column("Total_Operating_Cost_Q", sa.Float),
    sa.Column("Total_Operating_Cost_TTM", sa.Float),
    sa.Column("Operating_Income_Per_Share_A", sa.Float),
    sa.Column("Operating_Income_Per_Share_Q", sa.Float),
    sa.Column("Operating_Income_Per_Share_TTM", sa.Float),
    sa.Column("Initial_REG_Listing_Date", sa.DateTime),
    sa.Column("Initial_OTC_Listing_Date", sa.DateTime),
    sa.Column("Security_Type", sa.String),  # You can adjust the length according to your data
    sa.Column("Latest_Listing_Date", sa.DateTime),
    sa.Column("Initial_TSE_Listing_Date", sa.DateTime),
    sa.Column("TEJ_Industry_Code", sa.String),  # You can adjust the length according to your data
    sa.Column("English_Full_Name", sa.String),  # You can adjust the length according to your data
    sa.Column("TEJ_Industry_Name", sa.String),  # You can adjust the length according to your data
    sa.Column("Exchange_Industry_Name", sa.String),  # You can adjust the length according to your data
    sa.Column("Security_Name", sa.String),  # You can adjust the length according to your data
    sa.Column("English_Abbreviation", sa.String),  # You can adjust the length according to your data
    sa.Column("Exchange_Industry_Code", sa.String),  # You can adjust the length according to your data
    sa.Column("Unified_Identification_Number", sa.String),  # You can adjust the length according to your data
    sa.Column("Security_Full_Name", sa.String),  # You can adjust the length according to your data
    sa.Column("Delisting_Date", sa.DateTime),

    # 董監持股
    sa.Column("Director_and_Supervisor_Holdings_Percentage", sa.Float),
    sa.Column("Manager_Holdings_Percentage", sa.Float),
    sa.Column("Major_Shareholder_Holdings_Percentage_TSE", sa.Float),
    sa.Column("Total_Director_and_Supervisor_Holdings_Percentage", sa.Float),
    sa.Column("Managers_Total_Holdings_Percentage", sa.Float),
    sa.Column("Major_Shareholder_Total_Holdings_Percentage_TSE", sa.Float),

    # 新增欄位
    sa.Column("Director_and_Supervisor_Holdings", sa.Float),
    sa.Column("Net_Cash_Flows_From_Used_In_Operating_Activities_Q", sa.Float),
    sa.Column("Buyback_Description_Chinese", sa.String),
    sa.Column("Stock_Dividend_Listing_Date", sa.DateTime),
    sa.Column("Difference_in_Top_10_Shareholder_Pledged_Shares_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Directors_Relatives_Total_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Specific_Rate_Percentage", sa.Float),
    sa.Column("Net_Service_Fee_Charge_Income_Losses_Banking_And_Financial_Q", sa.Float),
    sa.Column("Total_Stock_Capital_Thousand_TWD", sa.Float),
    sa.Column("Discounts_And_Loans_Net_Banking_And_Financial_Q", sa.Float),
    sa.Column("Net_Cash_Flows_From_Used_In_Financing_Activities_A", sa.Float),
    sa.Column("Market", sa.String),
    sa.Column("Short_Term_And_Long_Term_Liabilities_Q", sa.Float),
    sa.Column("Warrant_Release_Date", sa.DateTime),
    sa.Column("Preference_Share_Dividends_Q", sa.Float),
    sa.Column("Difference_in_Managers_Pledged_Shares_Percentage_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Rights_Issuance_Stock_Announcement_Date", sa.DateTime),
    sa.Column("Major_Shareholder_Total_Pledged_Shares_1", sa.Float),
    sa.Column("Securities_and_Futures_Bureau_Approval_Date", sa.DateTime),
    sa.Column("Audit_Committee_Established", sa.String),
    sa.Column("Dividend_Pay_Times", sa.Float),
    sa.Column("Total_Director_and_Supervisor_Holdings", sa.Float),
    sa.Column("Managers_Relatives_Total_Pledged_Shares_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Directors_and_Supervisors_Pledged_Shares", sa.Float),
    sa.Column("Total_Non_Operating_Income_And_Expenses_Securities_TTM", sa.Float),
    sa.Column("Total_Asset_Turnover_Q", sa.Float),
    sa.Column("Profit_Loss_Attributable_To_Owners_Of_Parent_A", sa.Float),
    sa.Column("Total_Shares_Thousand_Shares", sa.Float),
    sa.Column("Total_Liabilities_And_Equity_Q", sa.Float),
    sa.Column("Total_Non_Operating_Income_And_Expenses_Securities_A", sa.Float),
    sa.Column("Capital_Reserves_Rights_Issuance_Rate_Percentage", sa.Float),
    sa.Column("Profit_Loss_Attributable_To_Owners_Of_Parent_TTM", sa.Float),
    sa.Column("Other_Thousand_Shares", sa.Float),
    sa.Column("Basic_Earnings_Per_Share_MOPS_A", sa.Float),
    sa.Column("Profit_Loss_Before_Tax_A", sa.Float),
    sa.Column("Total_Operating_Revenue_Q", sa.Float),
    sa.Column("Total_Expenditure_And_Expense_Securities_Q", sa.Float),
    sa.Column("Managers_Pledged_Shares", sa.Float),
    sa.Column("Executive_Directors_Count", sa.Float),
    sa.Column("Difference_in_Top_10_Shareholder_Total_Pledged_Shares_Percentage_Including_Rollover_from_Previous_Period", sa.Float),
    sa.Column("Event_Type_English", sa.String),
    sa.Column("Private_Placement_Flag_Fg", sa.String),
    sa.Column("Capital_Reduction_Rate_Percentage", sa.Float),
    sa.Column("Difference_Major_Shareholder_Total_Pledged_Shares_1", sa.Float),
    sa.Column("Convertible_Bonds_Conversion_Price_Per_Unit", sa.Float),
    sa.Column("Total_Treasury_Shares_TTM", sa.Float),
    sa.Column("Position", sa.String),
    sa.Column("Total_Equity_Attributable_To_Owners_Of_Parent_A", sa.Float),
    sa.Column("Suspension_of_Sell_After_Day_Trading_Fg", sa.String),
    sa.Column("Gross_Profit_Loss_From_Operations_A", sa.Float),
    sa.Column("Major_Shareholder_Total_Holdings_TSE", sa.Float),
    sa.Column("Managers_Total_Holdings_Percentage_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Stock_Dividend_Release_Date", sa.DateTime),
    sa.Column("Top_10_Shareholder_Total_Pledge_Shares_Percentage_2", sa.Float),
    sa.Column("Rights_Issuance_Flag_Fg", sa.String),
    sa.Column("Difference_in_Top_10_Shareholder_Total_Holdings_Percentage_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Major_Shareholder_Holdings_Percentage_TSE_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Manager_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Managers_Relatives_Total_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Buyback_Number", sa.Float),
    sa.Column("Total_CI_TTM", sa.Float),
    sa.Column("Next_Election_Date", sa.DateTime),
    sa.Column("Net_Cash_Flows_From_Used_In_Financing_Activities_TTM", sa.Float),
    sa.Column("Foreign_Independent_Directors_Count", sa.Float),
    sa.Column("General_Directors_Count", sa.Float),
    sa.Column("Board_of_Directors_Decision_Cancellation_Date", sa.DateTime),
    sa.Column("Difference_in_Supervisor_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Days_Sales_Outstanding_TTM", sa.Float),
    sa.Column("Difference_in_Directors_and_Supervisors_Relatives_Total_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Total_Treasury_Shares_Q", sa.Float),
    sa.Column("Debt_To_Equity_Ratio_A", sa.Float),
    sa.Column("BV_Per_Share_A_Reserve_For_Capital_Increase_And_Preference_Shares_Adjusted_A", sa.Float),
    sa.Column("Difference_in_Managers_Total_Holdings_Percentage_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Suspension_Transfer_Start_Date", sa.DateTime),
    sa.Column("Total_Revenue_Securities_A", sa.Float),
    sa.Column("Finance_Costs_Net_TTM", sa.Float),
    sa.Column("Managers_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Days_Payable_Outstanding_Q", sa.Float),
    sa.Column("Net_Cash_Flows_From_Used_In_Operating_Activities_A", sa.Float),
    sa.Column("Total_Non_Operating_Income_And_Expenses_Q", sa.Float),
    sa.Column("Difference_in_Managers_Relatives_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Stock_Div_Earnings", sa.Float),
    sa.Column("Net_Service_Fee_Charge_Income_Losses_Banking_And_Financial_A", sa.Float),
    sa.Column("Suspension_Transfer_End_Date", sa.DateTime),
    sa.Column("Original_Shareholder_Payment_End_Date", sa.DateTime),
    sa.Column("Cumulative_Holding_Percentage", sa.Float),
    sa.Column("Managers_Relatives_Total_Pledged_Shares", sa.Float),
    sa.Column("Net_Profit_Margin_Before_Interest_Taxes_Depreciation_And_Amortization_A", sa.Float),
    sa.Column("Merger_Thousand_Shares", sa.Float),
    sa.Column("Total_Non_Operating_Income_And_Expenses_Insurance_A", sa.Float),
    sa.Column("Total_Operating_Revenue_A", sa.Float),
    sa.Column("Earnings_Before_Interest_Tax_Depreciation_And_Amortization_A", sa.Float),
    sa.Column("Difference_in_Managers_Pledged_Shares_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Directors_and_Supervisors_Relatives_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Resignation_Date", sa.DateTime),
    sa.Column("Major_Shareholder_Relatives_Total_Holdings_1_Excluding_Directors_and_Supervisors", sa.Float),
    sa.Column("ROA_A_Income_From_Continuing_Operation_Before_Interest_A", sa.Float),
    sa.Column("Total_Non_Current_Liabilities_TTM", sa.Float),
    sa.Column("Meeting_Date", sa.DateTime),
    sa.Column("Managers_Relatives_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Net_Income_Loss_Of_Interest_Banking_And_Financial__TTM", sa.Float),
    sa.Column("Major_Shareholder_Total_Pledged_Shares_TSE", sa.Float),
    sa.Column("Supervisors_Total_Pledged_Shares", sa.Float),
    sa.Column("Directors_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Earnings_Before_Interest_And_Tax_Q", sa.Float),
    sa.Column("Difference_in_Top_10_Shareholder_Holdings_Percentage_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Outstanding_Shares_1000_Shares_Monthly_Frequency", sa.Float),
    sa.Column("Total_Non_Operating_Income_And_Expenses_Insurance_Q", sa.Float),
    sa.Column("Preferred_Stock_Shares_Q", sa.Float),
    sa.Column("Difference_in_Manager_Holdings_Percentage_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Event_Type_Chinese", sa.String),
    sa.Column("Top_10_Shareholder_Total_Holdings_Excluding_Directors_and_Supervisors", sa.Float),
    sa.Column("Convertible_Bonds_Securities_Conversion_Thousand_Shares", sa.Float),
    sa.Column("Days_Payable_Outstanding_A", sa.Float),
    sa.Column("NCI_A", sa.Float),
    sa.Column("Total_Assets_Growth_Rate_YOY_Q", sa.Float),
    sa.Column("ROE_After_Tax_A", sa.Float),
    sa.Column("Difference_in_Directors_and_Supervisors_Relatives_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Supervisors_Total_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Directors_Relatives_Total_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Market_Board_Eng", sa.String),
    sa.Column("Difference_in_Directors_and_Supervisors_Relatives_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Profit_Loss_Before_Tax_TTM", sa.Float),
    sa.Column("Transfer_Thousand_Shares", sa.Float),
    sa.Column("Directors_and_Supervisors_Relatives_Holdings_Percentage", sa.Float),
    sa.Column("Total_Expenditure_And_Expense_Securities_TTM", sa.Float),
    sa.Column("Net_Profit_Margin_Before_Interest_Taxes_Depreciation_And_Amortization_Q", sa.Float),
    sa.Column("Total_Non_Operating_Income_And_Expenses_Securities_Q", sa.Float),
    sa.Column("Net_Income_Banking_And_Financial_Q", sa.Float),
    sa.Column("Foreign_Independent_Supervisors_Count", sa.Float),
    sa.Column("Total_Liabilities_And_Equity_TTM", sa.Float),
    sa.Column("Total_Equity_Attributable_To_Owners_Of_Parent_Q", sa.Float),
    sa.Column("Par_Value", sa.Float),
    sa.Column("Managers_Total_Shares_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Total_Buyback_Price", sa.Float),
    sa.Column("Preference_Share_Dividends_A", sa.Float),
    sa.Column("Director_Holdings", sa.Float),
    sa.Column("Currency", sa.String),
    sa.Column("Total_Interest_Expenses_Banking_And_Financial__A", sa.Float),
    sa.Column("Debt_To_Equity_Ratio_TTM", sa.Float),
    sa.Column("Stock_Dividend_Pay_Date", sa.DateTime),
    sa.Column("Election_Status", sa.String),
    sa.Column("Short_Term_And_Long_Term_Liabilities_TTM", sa.Float),
    sa.Column("Board_of_Directors_Decision_Date", sa.DateTime),
    sa.Column("Debt_To_Equity_Ratio_Q", sa.Float),
    sa.Column("Managers_Pledged_Shares_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Total_Interest_Expenses_Banking_And_Financial__Q", sa.Float),
    sa.Column("Managers_Total_Pledged_Shares", sa.Float),
    sa.Column("Rights_Issuance_Stock_Release_Date", sa.DateTime),
    sa.Column("Stock_Div_Capital", sa.Float),
    sa.Column("Difference_in_Total_Director_Holdings_from_Previous_Period", sa.Float),
    sa.Column("R&D_Expenses_Ratio_Q", sa.Float),
    sa.Column("Total_Assets_Growth_Rate_YOY_TTM", sa.Float),
    sa.Column("Supervisors_Relatives_Holdings_Percentage", sa.Float),
    sa.Column("Difference_in_Managers_Relatives_Total_Pledged_Shares_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Profit_Loss_Attributable_To_Owners_Of_Parent_Q", sa.Float),
    sa.Column("Total_Operating_Costs_A", sa.Float),
    sa.Column("Adjust_Factor_Exclude_CashDiv", sa.Float),
    sa.Column("Difference_Top_10_Shareholder_Total_Pledge_Shares_Percentage_2", sa.Float),
    sa.Column("Payout_Ratio", sa.Float),
    sa.Column("Difference_in_Managers_Total_Pledged_Shares_Percentage_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Total_Operating_Costs_Q", sa.Float),
    sa.Column("Total_Asset_Turnover_TTM", sa.Float),
    sa.Column("Profit_Loss_From_Continuing_Operations_A", sa.Float),
    sa.Column("Difference_in_Manager_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Supervisors_Total_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Directors_and_Supervisors_Total_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Major_Shareholder_Total_Pledged_Shares_Percentage_TSE_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Supervisors_Relatives_Total_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Basic_Earnings_Per_Share_MOPS_Q", sa.Float),
    sa.Column("Cash_Back_Per_Share_After_Reduction", sa.Float),
    sa.Column("Difference_in_Total_Supervisor_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Major_Shareholder_Relatives_Holdings_Percentage_1_Excluding_Directors_and_Supervisors", sa.Float),
    sa.Column("Name", sa.String),
    sa.Column("Difference_in_Major_Shareholder_Total_Holdings_TSE_from_Previous_Period", sa.Float),
    sa.Column("Supervisors_Relatives_Total_Holdings", sa.Float),
    sa.Column("Total_Expenditure_And_Expense_Securities_A", sa.Float),
    sa.Column("Top_10_Shareholder_Relatives_Total_Holdings_2_Excluding_Directors_and_Supervisors", sa.Float),
    sa.Column("Weighted_Average_Number_Of_Ordinary_Shares_In_Thousands_Q", sa.Float),
    sa.Column("Short_Term_And_Long_Term_Liabilities_A", sa.Float),
    sa.Column("Difference_in_Supervisors_Relatives_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Issue_Shares_1000_Shares", sa.Float),
    sa.Column("Original_Shareholder_Payment_Start_Date", sa.DateTime),
    sa.Column("Supervisors_Relatives_Total_Pledged_Shares", sa.Float),
    sa.Column("Difference_in_Directors_and_Supervisors_Total_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Warrant_Listing_Date", sa.DateTime),
    sa.Column("Directors_Relatives_Total_Pledged_Shares", sa.Float),
    sa.Column("Difference_in_Managers_Relatives_Pledged_Shares_Percentage_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Managers_Relatives_Total_Holdings_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Managers_Relatives_Holdings_Percentage_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Difference_in_Supervisors_Relatives_Total_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Employee_Stock_Options_Thousand_Shares", sa.Float),
    sa.Column("Margin_Trading_Suspension_Start_Date", sa.DateTime),
    sa.Column("Discounts_And_Loans_Net_Banking_And_Financial_A", sa.Float),
    sa.Column("Difference_in_Major_Shareholder_Pledged_Shares_Percentage_TSE_from_Previous_Period", sa.Float),
    sa.Column("NCI_Q", sa.Float),
    sa.Column("Buyback_Period_End_Date", sa.DateTime),
    sa.Column("Difference_in_Managers_Total_Shares_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Earnings_Capital_Increase_Thousand_Shares", sa.Float),
    sa.Column("Preference_Share_Dividends_TTM", sa.Float),
    sa.Column("Stock_Dividend_Announcement_Date", sa.DateTime),
    sa.Column("Difference_in_Manager_Holdings_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Major_Shareholder_Holdings_TSE", sa.Float),
    sa.Column("Earnings_Before_Interest_Tax_Depreciation_And_Amortization_TTM", sa.Float),
    sa.Column("Difference_in_Managers_Total_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Gross_Profit_Loss_From_Operations_TTM", sa.Float),
    sa.Column("Difference_in_Major_Shareholder_Relatives_Holdings_Percentage_1_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Manager_Holdings_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Supervisors_Total_Holdings_Percentage", sa.Float),
    sa.Column("Total_Asset_Turnover_A", sa.Float),
    sa.Column("Difference_in_Directors_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Dividends_Type", sa.String),
    sa.Column("Dividends_Distri_End", sa.DateTime),
    sa.Column("Cash_Subscription_Rate_Percentage", sa.Float),
    sa.Column("Difference_in_Total_Director_and_Supervisor_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Director_and_Supervisor_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Top_10_Shareholder_Holdings_Percentage_Excluding_Directors_and_Supervisors", sa.Float),
    sa.Column("Average_Buyback_Price", sa.Float),
    sa.Column("Days_Sales_Outstanding_A", sa.Float),
    sa.Column("Directors_and_Supervisors_Relatives_Total_Pledged_Shares", sa.Float),
    sa.Column("Total_Interest_Expenses_Banking_And_Financial__TTM", sa.Float),
    sa.Column("Treasury_Stock_Cancellation_Thousand_Shares", sa.Float),
    sa.Column("Foreign_Directors_Count", sa.Float),
    sa.Column("Difference_in_Managers_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Directors_Total_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Subtotal_Stock_Div", sa.Float),
    sa.Column("Difference_in_Directors_Total_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Profit_Loss_From_Continuing_Operations_Q", sa.Float),
    sa.Column("Directors_and_Supervisors_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Finance_Costs_Net_Q", sa.Float),
    sa.Column("Payment_Certificate_Announcement_Date", sa.DateTime),
    sa.Column("Total_Operating_Costs_Insurance_A", sa.Float),
    sa.Column("ROE_After_Tax_TTM", sa.Float),
    sa.Column("Restricted_Employee_Rights_New_Stock_Thousand_Shares", sa.Float),
    sa.Column("Difference_in_Major_Shareholder_Total_Pledged_Shares_TSE_from_Previous_Period", sa.Float),
    sa.Column("Warrant_Announcement_Date", sa.DateTime),
    sa.Column("Difference_Major_Shareholder_Total_Pledge_Shares_Percentage_1", sa.Float),
    sa.Column("Employee_Bonus_Capital_Increase_Stock_Announcement_Date", sa.DateTime),
    sa.Column("Top_10_Shareholder_Pledged_Shares_Excluding_Directors_and_Supervisors", sa.Float),
    sa.Column("Independent_Directors_Count", sa.Float),
    sa.Column("Difference_in_Supervisors_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Announcement_Date", sa.DateTime),
    sa.Column("R&D_Expenses_Ratio_A", sa.Float),
    sa.Column("Cash_Dividend_Pay_Date", sa.DateTime),
    sa.Column("Net_Income_Banking_And_Financial_A", sa.Float),
    sa.Column("Major_Shareholder_Pledged_Shares_Percentage_TSE", sa.Float),
    sa.Column("Equivalent_Shares_Of_Stock_Dividend_To_Be_Distributed_TTM", sa.Float),
    sa.Column("Buyback_Price_Range_Upper_Limit", sa.Float),
    sa.Column("Total_Operating_Costs_Insurance_TTM", sa.Float),
    sa.Column("Supervisors_Pledged_Shares", sa.Float),
    sa.Column("Days_Inventory_On_Hand_A", sa.Float),
    sa.Column("Directors_Total_Pledged_Shares", sa.Float),
    sa.Column("Acid_Test_Ratio_TTM", sa.Float),
    sa.Column("Private_Placement_Pricing_Date", sa.DateTime),
    sa.Column("Weighted_Average_Number_Of_Ordinary_Shares_In_Thousands_TTM", sa.Float),
    sa.Column("Total_Supervisors_Count", sa.Float),
    sa.Column("Ex_Date_Cash_Div", sa.DateTime),
    sa.Column("Capital_Increase_Failure_Reason", sa.String),
    sa.Column("Net_Cash_Flows_From_Used_In_Financing_Activities_Q", sa.Float),
    sa.Column("Total_Operating_Costs_Insurance_Q", sa.Float),
    sa.Column("Previous_Day_Closing_Price_Per_Unit", sa.Float),
    sa.Column("Difference_in_Managers_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Director_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Total_Directors_Count", sa.Float),
    sa.Column("Difference_in_Top_10_Shareholder_Relatives_Total_Holdings_2_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Capital_Reduction_Thousand_Shares", sa.Float),
    sa.Column("Total_Non_Operating_Income_And_Expenses_A", sa.Float),
    sa.Column("Employee_Rights_Issuance_Rate_Percentage", sa.Float),
    sa.Column("Directors_and_Supervisors_Relatives_Total_Holdings", sa.Float),
    sa.Column("Short_Selling_Suspension_Start_Date", sa.DateTime),
    sa.Column("Net_Profit_Margin_Before_Interest_Taxes_Depreciation_And_Amortization_TTM", sa.Float),
    sa.Column("Supervisor_Holdings", sa.Float),
    sa.Column("Cumulative_Holding_Shares", sa.Float),
    sa.Column("ROE_After_Tax_Q", sa.Float),
    sa.Column("Days_Inventory_On_Hand_TTM", sa.Float),
    sa.Column("Convertible_Shares_Thousand_Shares", sa.Float),
    sa.Column("Difference_in_Director_and_Supervisor_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Supervisors_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Net_Cash_Flows_From_Used_In_Investing_Activities_Q", sa.Float),
    sa.Column("Common_Stock_Shares_TTM", sa.Float),
    sa.Column("Difference_in_Top_10_Shareholder_Pledged_Shares_Percentage_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("IPO_Flag_Fg", sa.String),
    sa.Column("Revenue_Remark", sa.String),
    sa.Column("Equivalent_Shares_Of_Stock_Dividend_To_Be_Distributed_Q", sa.Float),
    sa.Column("Total_Revenue_Securities_TTM", sa.Float),
    sa.Column("Managers_Relatives_Total_Holdings", sa.Float),
    sa.Column("Acid_Test_Ratio_A", sa.Float),
    sa.Column("Managers_Total_Shares", sa.Float),
    sa.Column("Discounts_And_Loans_Net_Banking_And_Financial_TTM", sa.Float),
    sa.Column("Net_Cash_Flows_From_Used_In_Investing_Activities_TTM", sa.Float),
    sa.Column("Total_Assets_Growth_Rate_YOY_A", sa.Float),
    sa.Column("Cash_Subscription_Price_Per_Unit", sa.Float),
    sa.Column("Directors_Relatives_Total_Holdings", sa.Float),
    sa.Column("Total_Deposits_And_Remittances_Banking_And_Financial_TTM", sa.Float),
    sa.Column("Total_Deposits_And_Remittances_Banking_And_Financial_Q", sa.Float),
    sa.Column("ROA_A_Income_From_Continuing_Operation_Before_Interest_TTM", sa.Float),
    sa.Column("Difference_in_Directors_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Total_Supervisor_Holdings", sa.Float),
    sa.Column("Buyback_Short", sa.String),
    sa.Column("Rights_Issuance_Accelerated_Payment_Start_Date", sa.DateTime),
    sa.Column("Cash_Flow_Ratio_Q", sa.Float),
    sa.Column("Net_Income_Banking_And_Financial_TTM", sa.Float),
    sa.Column("Insufficient_Director_Shares", sa.Float),
    sa.Column("Payment_Certificate_Listing_Date", sa.DateTime),
    sa.Column("Chairman_Count", sa.Float),
    sa.Column("Difference_in_Total_Director_and_Supervisor_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Executive_Supervisors_Count", sa.Float),
    sa.Column("Directors_Relatives_Holdings_Percentage", sa.Float),
    sa.Column("Managers_Relatives_Pledged_Shares_Percentage_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Difference_Top_10_Shareholder_Total_Pledge_Shares_2", sa.Float),
    sa.Column("Total_Deposits_And_Remittances_Banking_And_Financial_A", sa.Float),
    sa.Column("Difference_in_Managers_Total_Shares_from_Previous_Period", sa.Float),
    sa.Column("Capital_Reserves_Capital_Increase_Thousand_Shares", sa.Float),
    sa.Column("Managers_Total_Pledged_Shares_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Shareholders_Meeting_Decision_Date", sa.DateTime),
    sa.Column("Profit_Loss_Before_Tax_Q", sa.Float),
    sa.Column("Total_Equity_Attributable_To_Owners_Of_Parent_TTM", sa.Float),
    sa.Column("BV_Per_Share_A_Reserve_For_Capital_Increase_And_Preference_Shares_Adjusted_Q", sa.Float),
    sa.Column("Difference_in_Managers_Relatives_Holdings_Percentage_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Profit_Loss_From_Continuing_Operations_TTM", sa.Float),
    sa.Column("Difference_in_Managers_Total_Pledged_Shares_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Managers_Relatives_Total_Holdings_Including_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Earnings_Rights_Issuance_Rate_Percentage", sa.Float),
    sa.Column("Difference_in_Major_Shareholder_Total_Holdings_Percentage_TSE_from_Previous_Period", sa.Float),
    sa.Column("Director_Meeting_Date", sa.DateTime),
    sa.Column("Directors_and_Supervisors_Total_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Difference_in_Major_Shareholder_Holdings_TSE_from_Previous_Period", sa.Float),
    sa.Column("Rights_Issuance_Stock_Listing_Date", sa.DateTime),
    sa.Column("Employee_Subscription_Rate_Percentage", sa.Float),
    sa.Column("Preferred_Stock_Shares_TTM", sa.Float),
    sa.Column("Sub_Industry_Eng", sa.String),
    sa.Column("Difference_in_Major_Shareholder_Relatives_Total_Holdings_1_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Managers_Relatives_Total_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Total_CI_A", sa.Float),
    sa.Column("Independent_Supervisors_Count", sa.Float),
    sa.Column("Days_In_Position", sa.Float),
    sa.Column("Top_10_Shareholder_Relatives_Holdings_Percentage_1_Excluding_Directors_and_Supervisors", sa.Float),
    sa.Column("Convertible_Shares_Total_Thousand_Shares", sa.Float),
    sa.Column("Difference_in_Directors_Total_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Directors_Pledged_Shares", sa.Float),
    sa.Column("Difference_in_Managers_Total_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Directors_Relatives_Total_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Total_Shares", sa.Float),
    sa.Column("Rights_Issuance_Accelerated_Payment_End_Date", sa.DateTime),
    sa.Column("Days_Sales_Outstanding_Q", sa.Float),
    sa.Column("Margin_Trading_Suspension_End_Date", sa.DateTime),
    sa.Column("Difference_in_Directors_Relatives_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Director_Holdings_Percentage", sa.Float),
    sa.Column("Manager_Holdings", sa.Float),
    sa.Column("Managers_Pledged_Shares_Percentage_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Purchase_Rate_Percentage", sa.Float),
    sa.Column("Capital_Use", sa.String),
    sa.Column("ROA_A_Income_From_Continuing_Operation_Before_Interest_Q", sa.Float),
    sa.Column("Vice_Chairman_Count", sa.Float),
    sa.Column("Dividends_Distri_Year", sa.Float),
    sa.Column("Finance_Costs_Net_A", sa.Float),
    sa.Column("Difference_in_Top_10_Shareholder_Total_Pledged_Shares_Including_Rollover_from_Previous_Period", sa.Float),
    sa.Column("NCI_TTM", sa.Float),
    sa.Column("Cash_Dividend", sa.Float),
    sa.Column("Top_10_Shareholder_Total_Pledge_Shares_2", sa.Float),
    sa.Column("Manager_Holdings_Percentage_Including_Directors_and_Supervisors", sa.Float),
    sa.Column("Net_Service_Fee_Charge_Income_Losses_Banking_And_Financial_TTM", sa.Float),
    sa.Column("Total_Revenue_Securities_Q", sa.Float),
    sa.Column("Top_10_Shareholder_Holdings_Excluding_Directors_and_Supervisors", sa.Float),
    sa.Column("Difference_in_Supervisors_Relatives_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Total_Non_Current_Liabilities_A", sa.Float),
    sa.Column("Difference_in_Major_Shareholder_Pledged_Shares_TSE_from_Previous_Period", sa.Float),
    sa.Column("Total_Equity_Growth_Rate_YOY_TTM", sa.Float),
    sa.Column("Cash_Capital_Increase_Failure_Flag", sa.String),
    sa.Column("Difference_in_Top_10_Shareholder_Total_Holdings_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Common_Stock_Shares_Q", sa.Float),
    sa.Column("Recent_Election_Date", sa.DateTime),
    sa.Column("Supervisors_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Major_Shareholder_Pledged_Shares_TSE", sa.Float),
    sa.Column("Managers_Total_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Major_Shareholder_Total_Pledged_Shares_Percentage_1", sa.Float),
    sa.Column("Last_Transfer_Date", sa.DateTime),
    sa.Column("Employee_Bonus_Capital_Increase_Stock_Listing_Date", sa.DateTime),
    sa.Column("Buyback_Period_Start_Date", sa.DateTime),
    sa.Column("Total_Non_Current_Liabilities_Q", sa.Float),
    sa.Column("Other_Description", sa.String),
    sa.Column("Preferred_Stock_Shares_A", sa.Float),
    sa.Column("Total_Director_Holdings_Percentage", sa.Float),
    sa.Column("General_Supervisors_Count", sa.Float),
    sa.Column("Capital_Increase_Announcement_Date", sa.DateTime),
    sa.Column("Difference_in_Director_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Top_10_Shareholder_Total_Pledged_Including_Rollover", sa.Float),
    sa.Column("Total_Equity_Growth_Rate_YOY_Q", sa.Float),
    sa.Column("Employee_Bonus_Thousand_Shares", sa.Float),
    sa.Column("Cash_Flow_Ratio_A", sa.Float),
    sa.Column("GDR_Rate_Percentage", sa.Float),
    sa.Column("Difference_in_Total_Director_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Top_10_Shareholder_Relatives_Holdings_Percentage_2_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Ex_Date_Stock", sa.DateTime),
    sa.Column("Preferred_Shares_Securities_Conversion_Thousand_Shares", sa.Float),
    sa.Column("Net_Income_Loss_Of_Interest_Banking_And_Financial__Q", sa.Float),
    sa.Column("R&D_Expenses_Ratio_TTM", sa.Float),
    sa.Column("Difference_in_Managers_Relatives_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Top_10_Shareholder_Total_Pledged_Shares_Percentage_Including_Rollover", sa.Float),
    sa.Column("Days_Inventory_On_Hand_Q", sa.Float),
    sa.Column("Restricted_Employee_Rights_New_Stock_Cancellation_Thousand_Shares", sa.Float),
    sa.Column("Difference_in_Directors_and_Supervisors_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Directors_Relatives_Total_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Directors_and_Supervisors_Pledged_Shares_from_Previous_Period", sa.Float),
    sa.Column("Net_Cash_Flows_From_Used_In_Operating_Activities_TTM", sa.Float),
    sa.Column("Total_Director_Holdings", sa.Float),
    sa.Column("Acid_Test_Ratio_Q", sa.Float),
    sa.Column("Margin_Trading_Last_Return_Date", sa.DateTime),
    sa.Column("Total_CI_Q", sa.Float),
    sa.Column("Difference_in_Managers_Total_Pledged_Shares_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Top_10_Shareholder_Holdings_Excluding_Directors_and_Supervisors_from_Previous_Period", sa.Float),
    sa.Column("Foreign_Supervisors_Count", sa.Float),
    sa.Column("Basic_Earnings_Per_Share_MOPS_TTM", sa.Float),
    sa.Column("Insufficient_Supervisor_Shares", sa.Float),
    sa.Column("Total_Non_Operating_Income_And_Expenses_Insurance_TTM", sa.Float),
    sa.Column("Employee_Bonus_Capital_Increase_Stock_Release_Date", sa.DateTime),
    sa.Column("Buyback_Price_Range_Lower_Limit", sa.Float),
    sa.Column("Ex_Rights_Reference_Price_Per_Unit", sa.Float),
    sa.Column("Total_Treasury_Shares_A", sa.Float),
    sa.Column("Difference_in_Supervisors_Total_Holdings_Percentage_from_Previous_Period", sa.Float),
    sa.Column("Difference_in_Supervisor_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Buyback_Description_English", sa.String),
    sa.Column("BV_Per_Share_A_Reserve_For_Capital_Increase_And_Preference_Shares_Adjusted_TTM", sa.Float),
    sa.Column("Total_Operating_Revenue_TTM", sa.Float),
    sa.Column("Payment_Certificate_Release_Date", sa.DateTime),
    sa.Column("Public_Underwriting_Rate_Percentage", sa.Float),
    sa.Column("Dividends_Distri_Begin", sa.DateTime),
    sa.Column("Total_Non_Operating_Income_And_Expenses_TTM", sa.Float),
    sa.Column("Common_Stock_Shares_A", sa.Float),
    sa.Column("Supervisor_Holdings_Percentage", sa.Float),
    sa.Column("Supervisors_Total_Pledged_Shares_Percentage", sa.Float),
    sa.Column("Equivalent_Shares_Of_Stock_Dividend_To_Be_Distributed_A", sa.Float),
    sa.Column("buyback_no", sa.Float),
    sa.Column("Cash_Flow_Ratio_TTM", sa.Float),
    sa.Column("Difference_in_Directors_and_Supervisors_Relatives_Total_Holdings_from_Previous_Period", sa.Float),
    sa.Column("Sub_Industry", sa.String),
    sa.Column("Weighted_Average_Number_Of_Ordinary_Shares_In_Thousands_A", sa.Float),
    sa.Column("Total_Equity_Growth_Rate_YOY_A", sa.Float),
    sa.Column("Total_Operating_Costs_TTM", sa.Float),
    sa.Column("Supervisors_Relatives_Pledged_Shares_Percentage_Percentage", sa.Float)

)


register(
name = 'fundamentals',
f = tej_bundle,
calendar_name='TEJ',
start_session = None,
end_session = None,
minutes_per_day = 390,
create_writers = True,
)


register_calendar_alias("fundamentals", "TEJ")

