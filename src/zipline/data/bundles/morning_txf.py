
from io import BytesIO
import os
from click import progressbar
import logging
import pandas as pd
import requests
from zipline.utils.calendar_utils import register_calendar_alias
from dateutil.relativedelta import relativedelta
from datetime import datetime
import numpy as np
import re
import tejapi
from .core import register
from time import sleep , mktime
import warnings 
warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

tejapi.ApiConfig.page_limit = 10000
ONE_MEGABYTE = 1024 * 1024



def fetch_data_table(api_key, show_progress, mdate):
    """Fetch Prices data table from TEJ"""
    tejapi.ApiConfig.api_key = api_key
    if show_progress:
        log.info("Downloading TEJ metadata.")
        
    try :
        products = []
        contract_periods = pd.date_range(mdate["gte"],mdate["lte"],freq = "MS")
        for year , month in zip(contract_periods.year , contract_periods.month) :
            if month < 10 :
                month = "0"+str(month)
            products.append(f"TX{year}{month}")
        metadata = tejapi.fastget('TWN/AFUTR', coid = products ,mdate = mdate,opts = {'columns': ['mdate','coid', 'open_d', 'low_d', 'high_d', 'close_d', 'vol_d', 'underlying_name','last_tradedate']}, paginate=True)
        metadata = metadata.rename({'coid':'symbol',"mdate":"date",'open_d':'open','low_d':'low','high_d':'high','close_d':'close','vol_d':'volume','underlying_name':'asset_name','last_tradedate':'expiration_date'},axis =1)
        
    except Exception as e  :
        raise ValueError(f'Error occurs while downloading metadata due to {e} .')
    return metadata



def gen_asset_metadata(data, show_progress):
    if show_progress:
        log.info("Generating asset metadata.")
    
    data = data.groupby(by=["symbol","expiration_date","asset_name"]).agg({"date": [np.min, np.max]})
    
    data.reset_index(inplace=True)
    
    data['root_symbol'] = data['symbol']
    data["start_date"] = data.date.amin
    data["first_traded"] = data["start_date"]
    data["end_date"] = data.date.amax
    data["notice_date"] = pd.NaT
    data["tick_size"] = 1.0
    data["multiplier"] = 200.0
    
    
    del data["date"]
    data.columns = data.columns.get_level_values(0)

    data["exchange"] = "XTAI"
    data["auto_close_date"] = data["end_date"].values + pd.Timedelta(days=1)
    return data

def gen_root_symbols(data) :

    data = data.drop_duplicates(subset = "root_symbol",keep = "first")
    data["root_symbol"] = data["root_symbol"].astype("category")
    data["root_symbol_id"] = data.root_symbol.cat.codes
    
    return data[["root_symbol","root_symbol_id","exchange"]]


def parse_pricing_and_vol(data, sessions, symbol_map):
    data["annotation"] = "1"
    for asset_id, symbol in symbol_map.items():
        asset_data = (
            data.xs(symbol, level=1).reindex(sessions.tz_localize(None)).fillna(0.0)
        )
        yield asset_id, asset_data


def tej_morning_future_bundle(
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
):
    """
    quandl_bundle builds a daily dataset using Quandl's WIKI Prices dataset.

    For more information on Quandl's API and how to obtain an API key,
    please visit https://docs.quandl.com/docs#section-authentication
    """
    
    api_key = environ.get('TEJAPI_KEY')
    
    mdate = environ.get('mdate')
    if mdate :
        mdate = re.split('[,; ]',mdate)
        if len(mdate) == 1 :
            mdate.append(pd.to_datetime(datetime.today()).strftime('%Y%m%d'))
        elif len(mdate) > 2 :
            raise IndexError(
            "mdate must less than or equal to 2 parameters, please reset mdate."
            )
        mdate[1] = (pd.to_datetime(mdate[1]) + relativedelta(years = 1)).strftime('%Y%m%d')
        mdate = {'gte':mdate[0],'lte':mdate[1]}
    
    if api_key is None:
        raise ValueError(
            "Please set your TEJAPI_KEY environment variable and retry."
        )
    source_csv = os.environ.get('raw_source')
    csv_output_path = os.path.join(output_dir,'raw.csv')
    raw_data = fetch_data_table(
        api_key, show_progress , mdate
    )
    if source_csv :
        source_csv = os.path.join(source_csv,'raw.csv')
        origin_raw = pd.read_csv(source_csv,dtype = {'symbol':str,})
        raw_data = pd.concat([raw_data,origin_raw])
        raw_data['date'] = raw_data['date'].apply(pd.to_datetime)
        raw_data = raw_data.drop_duplicates(subset = ['symbol','date'])
        raw_data = raw_data.reset_index(drop = True)

    raw_data.to_csv(csv_output_path,index = False)
    
    asset_metadata = gen_asset_metadata(raw_data, show_progress)
    
    root_symobl_data = gen_root_symbols(asset_metadata)
    
    exchanges = pd.DataFrame(
        data=[["XTAI", "XTAI", "TW"]],
        columns=["exchange", "canonical_name", "country_code"],
    )
    
    asset_db_writer.write(futures=asset_metadata, exchanges=exchanges , root_symbols = root_symobl_data)

    symbol_map = asset_metadata.symbol
    
    sessions = calendar.sessions_in_range(start_session, end_session)
    
    
    raw_data.set_index(["date", "symbol"], inplace=True)
    clean_raw_data = raw_data[['open','high','low','close','volume',]].copy()
    daily_bar_writer.write(
        parse_pricing_and_vol(clean_raw_data, sessions, symbol_map),
        show_progress=show_progress,
    )
    
    raw_data.reset_index(inplace=True)
    raw_data["symbol"] = raw_data["symbol"].astype("category")
    raw_data["sid"] = raw_data.symbol.cat.codes

    adjustment_writer.write(
    #     splits=parse_splits(
    #         raw_data[
    #             [
    #                 "sid",
    #                 "date",
    #                 "split_ratio",
    #             ]
    #         ].loc[raw_data.split_ratio != 1],
    #         show_progress=show_progress,
    #     ),
    #     dividends=parse_dividends(
    #         raw_data[
    #             [
    #                 "sid",
    #                 "date",
    #                 "ex_dividend",
    #                 'out_pay',
    #                 'news_d',
    #                 'lastreg',
    #                 'div_percent',
    #             ]
    #         ].loc[raw_data.ex_dividend != 0],
    #         show_progress=show_progress,
    #     ),
        # stock_dividend_payouts = parse_stock_dividend_payouts(
        #     raw_data[
        #         [
        #             "sid",
        #             "date",
                    
        #         ]
        #     ].loc[raw_data.split_ratio != 1] ## 要改
        #     ,show_progress= show_progress)
    )




register(
name = 'morning_TXF',
f = tej_morning_future_bundle,
calendar_name='TEJ_morning_future',
start_session = None,
end_session = None,
minutes_per_day = 420,
create_writers = True,
)


register_calendar_alias("morning_TXF", "TEJ_morning_future")
