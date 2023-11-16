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
from time import sleep , mktime
import warnings 
warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

tejapi.ApiConfig.page_limit = 10000
ONE_MEGABYTE = 1024 * 1024


def load_data_table(file, index_col, show_progress=False):
    """Load data table from zip file provided by TEJ."""
    if show_progress:
        log.info("Parsing raw data.")
    data_table = file[
        ['ticker',
         'date',
         'open',
         'high',
         'low',
         'close',
         'volume',
         'ex-dividend',
         'split_ratio',
          'out_pay',
          'news_d',
          'lastreg',
          'div_percent',
          'annotation',
          'stk_name',
         ]
        ]
    data_table.rename(
        columns={
            "ticker": "symbol",
            "ex-dividend": "ex_dividend",
        },
        inplace=True,
        copy=False,
    )
    return data_table
def parse_annotation(data) :
    data['mch_prd'].fillna(0,inplace=  True)
    data['annotation'] = '1'
    data['annotation'] +=data.apply(lambda x : '1' if x['atten_fg'] == "Y" else "0",axis=1)
    data['annotation'] +=data.apply(lambda x : '1' if x['disp_fg'] == "Y" else "0",axis=1)
    data['annotation'] +=data.apply(lambda x : '1' if int(x['mch_prd']) == 5 else "0",axis=1)
    data['annotation'] +=data.apply(lambda x : '1' if int(x['mch_prd']) == 20 else "0",axis=1)
    data['annotation'] +=data.apply(lambda x : '1' if x['full_fg'] == "Y" else "0",axis=1)
    data['annotation'] +=data.apply(lambda x : '1' if x['limit_fg'] == "+" else "0",axis=1)
    data['annotation'] +=data.apply(lambda x : '1' if x['limit_fg'] == "-" else "0",axis=1)
    data['annotation'] +=data.apply(lambda x : "1" if x['limo_fg'] == "Y" else "0",axis=1)
    data['annotation'] +=data.apply(lambda x : "1" if x['sbadt_fg'] == "Y" else "0",axis=1)
    
    
    # data['annotation'] = data['annotation'].str.replace('\s+',' ',regex = True).str.strip().str.replace(' ','、',regex = True)
    
    return data[['coid','mdate','annotation']]
def fetch_data_table(api_key, show_progress,  coid, mdate):
    """Fetch Prices data table from TEJ"""
    tejapi.ApiConfig.api_key = api_key
    if show_progress:
        log.info("Downloading TEJ metadata.")
    try :
        metadata = tejapi.fastget('TWN/APIPRCD', coid =coid ,mdate = mdate,opts = {'columns': ['mdate','coid', 'open_d', 'low_d', 'high_d', 'close_d', 'vol', 'adjfac_a']}, paginate=True)
        if metadata.size == 0 :
            raise ValueError("Did not fetch any metadata. Please check the correctness of your ticker and mdate.")
        metadata['vol'] = metadata['vol'] * 1000
        cash_dividend = tejapi.fastget('TWN/ADIV', coid = coid ,mdate= mdate,opts = {'columns':['coid','div','mdate','out_pay','news_d','lastreg']},paginate=True)
        cash_back = tejapi.fastget('TWN/ASTK1',coid = coid,mdate = mdate , opts = {'columns':['coid','mdate','x_cap_date','x_lastreg', 'cc_pay_date', 'ashback']},paginate=True)
        cash_back = cash_back.loc[cash_back.ashback != 0,['coid','ashback','mdate','x_cap_date','x_lastreg','cc_pay_date']].rename({'x_cap_date':'news_d','x_lastreg':'lastreg','cc_pay_date':'out_pay','ashback':'div'},axis=1)
        
        cash_back['cash_back'] = True
        cash_dividend['div_percent'] = 0
        cash_dividend = pd.concat([cash_dividend,cash_back])
        cash_back['cash_back'] = cash_back['cash_back'].fillna(False)
        if cash_dividend.size > 0 :
            
            adjusted_cash_dividends = cash_dividend.groupby(['coid','mdate',])[['div']].sum().join(cash_dividend.set_index(['coid','mdate']),lsuffix = '_correct')
            
            dividend_percentage = adjusted_cash_dividends.loc[adjusted_cash_dividends['cash_back'] != True,['div','div_correct',]]
            
            dividend_percentage['div_percent'] = dividend_percentage.groupby(['coid','mdate'])['div'].sum() / dividend_percentage['div_correct']
            
            adjusted_cash_dividends = adjusted_cash_dividends.join(dividend_percentage[['div_percent']], how = 'left', lsuffix = '_ignore')
            
            adjusted_cash_dividends['div_percent'] = adjusted_cash_dividends['div_percent'].fillna(0)
            
            adjusted_cash_dividends['div'] = adjusted_cash_dividends['div_correct']
            
            del adjusted_cash_dividends['div_correct'] , adjusted_cash_dividends['div_percent_ignore']
            
            adjusted_cash_dividends = adjusted_cash_dividends.loc[(adjusted_cash_dividends['cash_back'] != True) | (adjusted_cash_dividends['div_percent'] == 0 ) ]
            
            adjusted_cash_dividends = adjusted_cash_dividends[~adjusted_cash_dividends.index.duplicated()]
            
            cash_dividend = adjusted_cash_dividends
        
            del adjusted_cash_dividends
        
        metadata = metadata.merge(cash_dividend, on = ['coid','mdate'],how = 'left')
        first_list = metadata.drop_duplicates(subset = ['coid'],keep = 'first').index.tolist()
        last_list = metadata.drop_duplicates(subset = ['coid'],keep = 'last').index.tolist()
        metadata['adjfac_a2'] = (metadata['adjfac_a'].copy().shift(1)).bfill()
        
        
        metadata['split_ratio'] = metadata['adjfac_a2'] / metadata['adjfac_a']
        
        metadata.loc[first_list,'split_ratio'] = 1
        
        metadata.loc[last_list,'split_ratio'] = 1
        
        metadata['split_ratio'] = 1 / metadata['split_ratio']
        
        del metadata['adjfac_a'],metadata['adjfac_a2']
        
        anno = tejapi.fastget("TWN/APISTKATTR",coid =coid ,mdate = mdate,opts ={"columns":["coid","mdate","atten_fg","disp_fg","mch_prd","full_fg","limit_fg","limo_fg","sbadt_fg"]},paginate=True)
        
        anno = parse_annotation(anno)
        
        metadata = metadata.merge(anno,on = ['coid','mdate'],how = 'left')
        
        metadata = metadata.rename({'coid':'ticker','open_d':'open','low_d':'low','high_d':'high','close_d':'close','vol':'volume','div':'ex-dividend'},axis =1)
        
        metadata["ex-dividend"].fillna(0,inplace= True)
        
        metadata.loc[first_list,'ex-dividend'] = 0
        
        metadata['date'] = metadata['mdate'].apply(lambda x : pd.Timestamp(x.strftime('%Y%m%d')) if not pd.isna(x) else pd.NaT)
        
        metadata['out_pay'] = pd.to_datetime(metadata['out_pay'])
        # if out_pay is NaT then set default out_pay day = T+21
        metadata.loc[(metadata['ex-dividend'] != 0)&(metadata['out_pay'].isna()),'out_pay'] = metadata.loc[(metadata['ex-dividend'] != 0)&(metadata['out_pay'].isna())].apply(lambda x : (x['mdate'] + pd.Timedelta(days = 21))  ,axis= 1)
        
        metadata['news_d'] = pd.to_datetime(metadata['news_d'])
        
        metadata['lastreg'] = pd.to_datetime(metadata['lastreg'])
        
        attr_df = tejapi.fastget('TWN/APISTOCK',coid = coid, opts = {'columns':['coid','stk_name']},paginate=True)
        
        attr_df = attr_df.rename({'coid':'ticker'},axis =1)
        
        metadata = metadata.merge(attr_df,on = 'ticker',how = 'left')
        
        del metadata['mdate'], metadata['cash_back']
        
    except Exception as e  :
        raise ValueError(f'Error occurs while downloading metadata due to {e} .')
    return load_data_table(
        file=metadata,
        index_col=None,
        show_progress=show_progress,
    )



def gen_asset_metadata(data, show_progress):
    if show_progress:
        log.info("Generating asset metadata.")

    data = data.groupby(by=["symbol","stk_name"]).agg({"date": [np.min, np.max]})
    data.reset_index(inplace=True)
    data["asset_name"] = data["stk_name"]
    data["start_date"] = data.date.amin
    data["end_date"] = data.date.amax
    del data["date"]
    data.columns = data.columns.get_level_values(0)

    data["exchange"] = "TEJ_XTAI"
    data["auto_close_date"] = data["end_date"].values + pd.Timedelta(days=1)
    return data


def parse_splits(data, show_progress):
    if show_progress:
        log.info("Parsing split data.")

    data["split_ratio"] = 1.0 / data.split_ratio
    data.rename(
        columns={
            "split_ratio": "ratio",
            "date": "effective_date",
        },
        inplace=True,
        copy=False,
    )
    return data

def parse_stock_dividend_payouts(data, show_progress) :
    if show_progress :
        log.info("Parsing stock dividend payouts.")
    data['payment_sid'] = data['sid']
    data['ex_date'] ,data['declare_date'],data['record_date'],data['payout_date'],= pd.NaT
    data['ratio'] = pd.NA
    return data


def parse_dividends(data, show_progress):
    if show_progress:
        log.info("Parsing dividend data.")
    data['pay_date'] = data['out_pay']
    data["record_date"] = data['lastreg']
    data["declared_date"] = data['news_d']
    # data["record_date"] = data["declared_date"] = data["pay_date"] = pd.NaT
    del data['out_pay'], data['news_d'] , data['lastreg']
    data.rename(
        columns={
            "ex_dividend": "amount",
            "date": "ex_date",
        },
        inplace=True,
        copy=False,
    )
    return data


def parse_pricing_and_vol(data, sessions, symbol_map):
    
    for asset_id, symbol in symbol_map.items():
        asset_data = (
            data.xs(symbol, level=1).reindex(sessions.tz_localize(None)).fillna(0.0)
        )
        yield asset_id, asset_data


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
):
    """
    quandl_bundle builds a daily dataset using Quandl's WIKI Prices dataset.

    For more information on Quandl's API and how to obtain an API key,
    please visit https://docs.quandl.com/docs#section-authentication
    """
    
    api_key = environ.get('TEJAPI_KEY')
    coid = environ.get('ticker')
    if coid.lower() == "all" :
        coid = None
        #confirm = input("Warning, you are trying to download all company, it may spend lots of time and api flow.\nPlease enter y to continue[Y/n].")
        #if confirm.lower() == "y" :
        #    coid = None
        #else :
        #    raise ValueError(
        #        "Please reset company id in your environment variable and retry."
        #    )
    elif coid :
        coid = re.split('[,; ]',coid)
    else :
        raise ValueError(
            "Please set company id in your environment variable and retry."
        )
    mdate = environ.get('mdate')
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
    source_csv = os.environ.get('raw_source')
    csv_output_path = os.path.join(output_dir,'raw.csv')
    raw_data = fetch_data_table(
        api_key, show_progress , coid , mdate
    )
    if source_csv :
        source_csv = os.path.join(source_csv,'raw.csv')
        origin_raw = pd.read_csv(source_csv,dtype = {'symbol':str,})
        origin_raw['out_pay'] = origin_raw['out_pay'].fillna(pd.NaT)
        origin_raw['news_d'] = origin_raw['news_d'].fillna(pd.NaT)
        origin_raw['lastreg'] = origin_raw['lastreg'].fillna(pd.NaT)
        raw_data = pd.concat([raw_data,origin_raw])
        raw_data['date'] = raw_data['date'].apply(pd.to_datetime)
        raw_data = raw_data.drop_duplicates(subset = ['symbol','date'])
        raw_data = raw_data.reset_index(drop = True)

    raw_data.to_csv(csv_output_path,index = False)
    
    asset_metadata = gen_asset_metadata(raw_data[["symbol","stk_name", "date"]], show_progress)
    
    exchanges = pd.DataFrame(
        data=[["TEJ_XTAI", "TEJ_XTAI", "TW"]],
        columns=["exchange", "canonical_name", "country_code"],
    )
    asset_db_writer.write(equities=asset_metadata, exchanges=exchanges)

    symbol_map = asset_metadata.symbol
    
    sessions = calendar.sessions_in_range(start_session, end_session)
    
    
    raw_data.set_index(["date", "symbol"], inplace=True)
    clean_raw_data = raw_data[['open','high','low','close','volume','split_ratio','ex_dividend','annotation']].copy()
    daily_bar_writer.write(
        parse_pricing_and_vol(clean_raw_data, sessions, symbol_map),
        show_progress=show_progress,
    )
    
    raw_data.reset_index(inplace=True)
    raw_data["symbol"] = raw_data["symbol"].astype("category")
    raw_data["sid"] = raw_data.symbol.cat.codes

    adjustment_writer.write(
        splits=parse_splits(
            raw_data[
                [
                    "sid",
                    "date",
                    "split_ratio",
                ]
            ].loc[raw_data.split_ratio != 1],
            show_progress=show_progress,
        ),
        dividends=parse_dividends(
            raw_data[
                [
                    "sid",
                    "date",
                    "ex_dividend",
                    'out_pay',
                    'news_d',
                    'lastreg',
                    'div_percent',
                ]
            ].loc[raw_data.ex_dividend != 0],
            show_progress=show_progress,
        ),
        # stock_dividend_payouts = parse_stock_dividend_payouts(
        #     raw_data[
        #         [
        #             "sid",
        #             "date",
                    
        #         ]
        #     ].loc[raw_data.split_ratio != 1] ## 要改
        #     ,show_progress= show_progress)
    )


def download_with_progress(url, chunk_size, **progress_kwargs):
    """
    Download streaming data from a URL, printing progress information to the
    terminal.

    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.
    chunk_size : int
        Number of bytes to read at a time from requests.
    **progress_kwargs
        Forwarded to click.progressbar.

    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    total_size = int(resp.headers["content-length"])
    data = BytesIO()
    with progressbar(length=total_size, **progress_kwargs) as pbar:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            data.write(chunk)
            pbar.update(len(chunk))

    data.seek(0)
    return data


def download_without_progress(url):
    """
    Download data from a URL, returning a BytesIO containing the loaded data.

    Parameters
    ----------
    url : str
        A URL that can be understood by ``requests.get``.

    Returns
    -------
    data : BytesIO
        A BytesIO containing the downloaded data.
    """
    resp = requests.get(url)
    resp.raise_for_status()
    return BytesIO(resp.content)

register(
name = 'tquant',
f = tej_bundle,
calendar_name='TEJ',
start_session = None,
end_session = None,
minutes_per_day = 390,
create_writers = True,
)


register_calendar_alias("tquant", "TEJ")
