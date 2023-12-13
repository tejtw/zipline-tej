"""
Module for building a complete daily dataset from Quandl's WIKI dataset.
"""
from io import BytesIO
import os

from click import progressbar
import logging
import pandas as pd
import requests
from zipline.utils.calendar_utils import register_calendar_alias
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

    import TejToolAPI


    try :
        data = TejToolAPI.get_history_data(ticker = coid, 
                                           columns = columns, 
                                           start = mdate['gte'], 
                                           end = mdate['lte'],
                                           include_self_acc = self_acc,
                                           require_annd = True,
                                           )

        if data.size == 0:
            raise ValueError("Did not fetch any fundamental data. Please check the correctness of your ticker, mdate and fields.")
        
        data = data.rename({'coid':'symbol', 'mdate':'date'}, axis =1)

    
    except Exception as e  :
        raise ValueError(f'Error occurs while downloading metadata due to {e} .')
    
    
    return data



def gen_asset_metadata(data, show_progress):
    if show_progress:
        log.info("Generating asset metadata.")

    data = data.groupby(by="symbol").agg({"date": [np.min, np.max]})
    data.reset_index(inplace=True)
    data["start_date"] = data.date.amin
    data["end_date"] = data.date.amax
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
        
        if isinstance( mdate , pd.Series) :
            mdate = mdate[0]
            
        new_symbol = environ.get('ticker')
        symbol = pd.read_sql('SELECT DISTINCT symbol FROM factor_table;', con).symbol.tolist()
        if new_symbol :
            new_symbol = re.split('[,; ]',new_symbol)
            symbol = symbol + new_symbol
        symbol = set(symbol)    
        symbol = ';'.join(symbol)
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
        raw_data = raw_data.reset_index(drop = True)
        raw_data = raw_data.sort_values(by =['symbol','date'])
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
    raw_data.to_sql(name = 'factor_table',
                    con = engine,
                    index=False,
                    )







import sqlalchemy as sa

metadata = sa.MetaData()

fundamental_schema = sa.Table(

    "factor_table",  # 資料表名稱
    metadata,  # 元資料
    sa.Column("symbol", sa.String, primary_key=True, nullable=False),  # 公司 ID，字串，主鍵
    sa.Column("date", sa.DateTime, primary_key=True, nullable=False),  # 年/月，日期時間，主鍵
    sa.Column("fin_date", sa.DateTime),  # 年/月，日期時間，主鍵
    sa.Column("share_date", sa.DateTime),  # 年/月，日期時間，主鍵
    sa.Column("mon_sales_date", sa.DateTime),  # 年/月，日期時間，主鍵
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
    sa.Column("Delisting_Date", sa.DateTime)
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