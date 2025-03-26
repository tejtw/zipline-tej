import os
import pandas as pd
from zipline.data import bundles
from zipline.data.data_portal import DataPortal
from zipline.utils.calendar_utils import get_calendar
from zipline.assets._assets import Future
from zipline.utils.run_algo import load_extensions

def get_bundle(bundle):    
    bundle_data = bundles.load(bundle)
    calendar_name = bundle_data.equity_daily_bar_reader.trading_calendar.name 
    trading_calendar = get_calendar(calendar_name)     
    return bundle_data, trading_calendar 
    
def get_dataportal(bundle_data, trading_calendar):    
    data_portal = DataPortal(
                    bundle_data.asset_finder,
                    trading_calendar=trading_calendar,
                    first_trading_day=bundle_data.equity_daily_bar_reader.first_trading_day,
                    equity_minute_reader=None,
                    equity_daily_reader=bundle_data.equity_daily_bar_reader,
                    future_daily_reader=bundle_data.equity_daily_bar_reader,
                    adjustment_reader=bundle_data.adjustment_reader,
                    )
    return data_portal
    
def get_futures_prices(start_dt:str, end_dt:str, bundle:str ='tquant_future'):    
    
    bundle_data, trading_calendar = get_bundle(bundle)
    data = get_dataportal(bundle_data, trading_calendar)
    
    end_loc = trading_calendar.all_sessions.searchsorted(end_dt)
    start_loc = trading_calendar.all_sessions.searchsorted(start_dt)
    bar_count = end_loc - start_loc + 1
    
    prices =  pd.concat([data.get_history_window(assets = bundle_data.asset_finder.futures_sids,
                                          end_dt = trading_calendar.all_sessions[end_loc],
                                          bar_count = bar_count,
                                          frequency = '1d',
                                          field = field,
                                          data_frequency = 'daily').assign(field = field)
                         for field in ['open','high','low','close','volume']],axis=0)
    
    prices = prices.set_index("field", append=True).swaplevel(0, 1, axis=0)
    prices.index.set_names(["Field", "Date"], inplace=True)

    if prices.index.duplicated().any():
        print('The prices index appears to be duplicated and needs to be grouped.')
        prices = prices.groupby(prices.index).first()
        prices.index = pd.MultiIndex.from_tuples(prices.index)
        prices.index.set_names(["Field", "Date"], inplace=True)

    sids = bundle_data.asset_finder.futures_sids
    assets = bundle_data.asset_finder.retrieve_futures_contracts(sids)
    root_symbols = {k:v.root_symbol for k,v in assets.items()}
    
    columns_tuple = zip(prices.columns,prices.columns.map(assets),prices.columns.map(root_symbols))
    prices.columns = pd.MultiIndex.from_tuples(columns_tuple)
    
    return prices.droplevel(0,axis=1).swaplevel(axis=1)

def get_root_symbol_ohlcv(df,get_field='close', get_root_symbol='TX'):
    return df.loc[pd.IndexSlice[get_field],pd.IndexSlice[get_root_symbol]] 

def get_root_symbol_latest_contract_prices(futures):    
    return futures.reset_index(level=1).groupby(level=0).last().set_index("Date", append=True).T.stack().reset_index()\
                                                .rename(columns={'level_0':'root_symbol','level_1':'contract'}).dropna()

def get_continues_futures_price(root_symbol='TX',offset=0,roll_style='calendar',adjustment='add',field='close',start_dt:str=None, end_dt:str=None,bundle:str ='tquant_future'):

    if not start_dt or not end_dt:
        raise ValueError()
        
    bundle_data, trading_calendar = get_bundle(bundle)
    data = get_dataportal(bundle_data, trading_calendar)
    
    kwargs_cf = dict(root_symbol = root_symbol,  
                     offset = offset,
                     roll_style = roll_style,
                     adjustment = adjustment
                    )
    continue_fut = bundle_data.asset_finder.create_continuous_future(**kwargs_cf)
    
    start_loc = trading_calendar.all_sessions.searchsorted(start_dt)
    end_loc = trading_calendar.all_sessions.searchsorted(end_dt)
    bar_count = end_loc - start_loc + 1
    
    data = get_dataportal(bundle_data,trading_calendar)
   
    return data.get_history_window(assets =[continue_fut], 
                                    end_dt = trading_calendar.all_sessions[end_loc],
                                    bar_count = bar_count+5,
                                    frequency = '1d',
                                    field = 'close',
                                    data_frequency = 'daily')

def get_futures_information(futures, rt =None):

    if not isinstance(rt, list):
        rt = [rt]
        
    all_rt = set(futures.columns.get_level_values(0))

    for _rt in rt :
        if _rt not in all_rt:
            raise ValueError(f"Invalid root symbol entered. Available root symbols are: {', '.join(all_rt)}.")
        
    cc=[];a=[]
    for j, rt_symbol in enumerate(rt,1):        
        for i,c in enumerate(futures[rt_symbol].columns,1):
            a.append(pd.DataFrame(c.to_dict(),index=[i]))
    return pd.concat(a).drop(['exchange_full','notice_date','first_traded'],axis=1)


