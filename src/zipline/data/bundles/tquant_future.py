import os
import logging
import pandas as pd
from zipline.utils.calendar_utils import register_calendar_alias
from dateutil.relativedelta import relativedelta
from datetime import datetime
import numpy as np
import re
import tejapi
from .core import register
import warnings 
warnings.filterwarnings("ignore")
log = logging.getLogger(__name__)

tejapi.ApiConfig.page_limit = 10000
ONE_MEGABYTE = 1024 * 1024

def load_data_table(file, index_col, ):
    """Load data table from zip file provided by TEJ."""
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
    data.fillna({'mch_prd':0},inplace=  True)
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
    
    return data[['coid','mdate','annotation']]

def fetch_equity_data(coid, mdate):
    """Fetch Prices data table from TEJ"""
    try :
        metadata = tejapi.fastget('TWN/APIPRCD', coid =coid ,mdate = mdate,opts = {'columns': ['mdate','coid', 'open_d', 'low_d', 'high_d', 'close_d', 'vol', 'adjfac_a']}, paginate=True)
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
        
        metadata.fillna({"ex-dividend":0}, inplace= True)
        
        metadata.loc[first_list,'ex-dividend'] = 0
        
        metadata['date'] = metadata['mdate'].apply(lambda x : pd.Timestamp(x.strftime('%Y%m%d')) if not pd.isna(x) else pd.NaT)
        
        # if out_pay is NaT then set default out_pay day = T+21
        metadata.loc[(metadata['ex-dividend'] != 0)&(metadata['out_pay'].isna()),'out_pay'] = metadata.loc[(metadata['ex-dividend'] != 0)&(metadata['out_pay'].isna())].apply(lambda x : (x['mdate'] + pd.Timedelta(days = 21))  ,axis= 1)
        
        metadata['out_pay'] = pd.to_datetime(metadata['out_pay'])

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
    )

def fetch_future_data(futures, mdate):
    """Fetch Prices data table from TEJ"""
    products = []
    code_dict = {}
    if futures :
        contract_periods = pd.date_range(mdate["gte"],mdate["lte"],freq = "MS")
        for year , month in zip(contract_periods.year , contract_periods.month) :
            if month < 10 :
                month = '0'+str(month)
            for company in futures :
                products.append(f"{company}{year}{month}")
                code_dict[f"{company}{year}{month}"] = company
    try :
        metadata = tejapi.fastget('TWN/AFUTR', coid = products ,mdate = mdate,opts = {'columns': ['mdate','coid', 'open_d', 'low_d', 'high_d', 'close_d', 'settle', 'wclose_d', 'vol_d', 'underlying_id' ,'underlying_name','last_tradedate']}, paginate=True)
        metadata['YM'] = metadata['coid'].str.extract('(\d+)')
        metadata = metadata.loc[(metadata['YM'].notna()) & (metadata['YM'].str.len() == 6)]
        coid = metadata['underlying_id'].unique().tolist()
        # join dividends
        dividends = tejapi.fastget('TWN/APIMT1' , coid = coid , opts = {'columns' : ['coid' , 'q1ex_date' ,  'q1mt_div' , 'q2ex_date' ,  'q2mt_div' , 'q3ex_date' ,  'q3mt_div' , 'q4ex_date' ,  'q4mt_div', 'mt_mer' , 'mex_date']} , paginate=True)
        cols = {'q1ex_date' : 'q1mt_div' , 'q2ex_date' : 'q2mt_div' , 'q3ex_date' : 'q3mt_div' , 'q4ex_date' : 'q4mt_div' , 'mex_date' :'mt_mer'}
        res = []

        for key , value in cols.items() :
            if key == 'mex_date' :
                temp = dividends[['coid' , key , value]].rename({key : 'mdate' , value : 'stock_dividends'} ,axis= 1)
            else :
                temp = dividends[['coid' , key , value]].rename({key : 'mdate' , value : 'ex_dividend'} ,axis= 1)
            res.append(temp)
        temp = pd.concat(res).dropna(subset = ['mdate']).groupby(['coid', 'mdate']).sum().reset_index().rename({'coid' : 'underlying_id'} , axis= 1)
        metadata = metadata.set_index(['mdate' , 'underlying_id']).join(temp.set_index(['mdate' , 'underlying_id']) , how = 'left').reset_index()
        del temp
        metadata.fillna({'ex_dividend' : 0  , 'stock_dividends' : 0 } , inplace= True)
        metadata = metadata.sort_values(by = ['coid' , 'mdate']).reset_index(drop =  True)

        # join cash return or cash add
        cash = tejapi.fastget('TWN/APISTK1' , coid = coid , mdate = mdate ,opts = {'columns' :  ['coid', 'mdate' ,'x_sub_l','x_issue2' ,'pct_cash1' ,'buy_price','ashback' , 'pct_dec1']} ).rename({'coid' : 'underlying_id'} , axis= 1)
        cash.fillna({'x_sub_l':pd.NaT  , 'pct_cash1' : 0 , 'buy_price' : 0 , 'ashback' : 0 , 'pct_dec1' :0 } , inplace=True)
        cash_return = cash[['underlying_id' , 'mdate' , 'ashback' , 'pct_dec1','x_issue2']]
        cash_return = cash_return[cash_return['x_issue2'].notna()]
        # handle with no trade on cash return or cash add day
        _range = metadata.groupby(['underlying_id','coid']).agg({'mdate' : [min] , 'last_tradedate' : [max]}).reset_index()

        _range.columns = ['underlying_id' , 'coid' , 'min' , 'max']

        def between(source , target) :
            return set(target.loc[(target['mdate'] >= source['min']) & (target['mdate'] <= source['max'])]['mdate'].values)
        
        _range['mdate'] = _range.apply(lambda x : between(x[['min' , 'max']] , cash_return) , axis= 1)

        _range = _range.explode(column='mdate').dropna(subset='mdate')
        del _range['min'] , _range['max']
        metadata = metadata.set_index(['underlying_id','coid','mdate']).join(_range.set_index(['underlying_id','coid','mdate']) , how = 'outer').reset_index()

        # join back cash return data
        metadata = metadata.set_index(['underlying_id' , 'mdate']).join(cash_return.set_index(['underlying_id', 'mdate']) , how = 'left').reset_index()
        metadata.fillna({'ex_dividend' : 0  , 'stock_dividends' : 0 , 'ashback' : 0 , 'pct_dec1' : 0 } , inplace= True)
        metadata['ex_dividend'] += metadata['ashback']
        
        metadata['stock_dividends'] -= (metadata['pct_dec1'] /10)
        del metadata['ashback'] , metadata['pct_dec1']

        # join back cash add data
        cash_add = cash[['underlying_id' , 'mdate' , 'x_sub_l' , 'pct_cash1' , 'buy_price']]
        cash_add = cash_add[cash_add['buy_price'] != 0]
        cash_add = cash_add.set_index(['underlying_id' , 'mdate']).join(metadata[['underlying_id' , 'coid' , 'mdate' , 'last_tradedate']].set_index(['underlying_id' , 'mdate'])).reset_index()
        cash_add['out_pay'] = cash_add[['last_tradedate','x_sub_l']].apply(lambda x : x['last_tradedate'] if x['last_tradedate'] <= x['x_sub_l'] else x['x_sub_l'] , axis =1)
        del cash_add['underlying_id'] , cash_add['last_tradedate'] , cash_add['x_sub_l']
        cash_add = cash_add.dropna()
        cash_add['mdate'] = cash_add['out_pay']
        metadata = metadata.set_index(['coid' , 'mdate']).join(cash_add.set_index(['coid' , 'mdate']) , how= 'left').reset_index()
        metadata.fillna({'ex_dividend' : 0 } , inplace= True)
        metadata['out_pay'] = metadata['out_pay'].fillna(metadata['mdate'])
        metadata = metadata.sort_values(by = ['coid' , 'mdate'])
        
        metadata['earn'] = np.floor(10 * metadata['pct_cash1'] * (metadata['wclose_d'] - metadata['buy_price'])) / 1000
        metadata.fillna({'earn' : 0 } , inplace= True)
        metadata.loc[metadata['earn'] < 0 ,'earn'] = 0
        metadata[metadata['ex_dividend'] != 0 ]

        
        metadata['ex_dividend'] += metadata['earn']
        del metadata['earn'] , metadata['pct_cash1'] , metadata['buy_price'] , 
        metadata = metadata.sort_values(by = ['coid' , 'mdate'])

        metadata['settle'] = metadata.groupby('coid')['settle'].ffill().bfill()
        metadata['close_d'] = metadata.groupby('coid')['close_d'].ffill().bfill()
        
        metadata['split_ratio'] =  1 / ((metadata['settle'] - metadata['ex_dividend'])/(1 + (metadata['stock_dividends'] / 10))  / metadata['settle'])
        metadata['split_ratio'] = metadata.groupby(['coid'])['split_ratio'].shift(-1).values
        
        metadata['temp_ex_dividend'] = metadata.groupby(['coid'])['ex_dividend'].shift(-1).fillna(0)
        metadata['temp_stock_dividends'] = metadata.groupby(['coid'])['stock_dividends'].shift(-1).fillna(0)
        metadata['next_open_price'] = metadata.groupby(['coid']).apply(lambda x : (x['settle'] - x['temp_ex_dividend'])/(1 + (x['temp_stock_dividends'] / 10))).shift(1).values
        
        del metadata['temp_ex_dividend'] , metadata['temp_stock_dividends']
        
        metadata['sell_stock_revenue'] = metadata['next_open_price'] * (metadata['stock_dividends'] / 10)
        # cash return need to pay money to stay same stock future quantity, so sell stock_revenue may be negative.
        # metadata.loc[metadata['sell_stock_revenue'] < 0 , 'sell_stock_revenue'] = 0 
        metadata['ex_dividend'] += metadata['sell_stock_revenue']
        
        del metadata['sell_stock_revenue']
        metadata.fillna({'split_ratio' : 1} , inplace= True)
        metadata['div_percent'] = 0
        del metadata['YM'] , metadata['next_open_price']
        metadata['code'] = metadata['coid'].map(code_dict)
        metadata['news_d'] = metadata['lastreg'] = metadata['mdate'] = metadata['mdate'].astype('datetime64[ns]')        
        metadata['out_pay'].fillna(metadata['mdate'] , inplace = True)
        metadata['last_tradedate'] = metadata.groupby('coid')['last_tradedate'].ffill()
        metadata['last_tradedate'] = metadata['last_tradedate'].astype('datetime64[ns]')
        metadata.loc[metadata['vol_d'].isna() , 'close_d'] = pd.NA
        metadata.loc[metadata['x_issue2'].notna() , 'out_pay'] = metadata.loc[metadata['x_issue2'].notna() , 'x_issue2']
        
        metadata = metadata.rename({'coid':'symbol',"mdate":"date",'open_d':'open','low_d':'low','high_d':'high','close_d':'close','vol_d':'volume','underlying_name':'asset_name','last_tradedate':'expiration_date'},axis =1)
    except Exception as e  :
        raise ValueError(f'Error occurs while downloading metadata due to {e} .')
    return metadata




def gen_future_asset_metadata(data):
    multiplier_dict = {
        "TX" : 200 ,  # 臺股期貨
        "MTX" : 50 ,  # 小型臺指期貨
        "TMF" : 10 ,  # 微型臺指期貨
        "M1F" : 10 ,  # 臺灣中型100期貨
        "E4F" : 100 ,  # 臺灣永續期貨
        "TE" : 4000 ,  # 電子期貨
        "ZEF"  : 500 , # 小型電子期貨
        "TF" : 1000 , # 金融期貨
        "ZFF" : 250 , # 小型金融期貨
        "XIF" : 100 , # 非金電期貨
        "SHF" : 1000 , # 航運期貨
        "SOF" : 50 , # 半導體30期貨
        "BTF" : 50 , # 臺灣生技期貨
        "GTF" : 4000 , # 櫃買期貨
        "G2F" : 50 , # 富櫃200期貨
    }
    try :
        url = 'https://www.taifex.com.tw/cht/2/stockLists'
        df = pd.read_html(url)
        stock_multiplier_dict = dict(zip(df[1]['股票期貨、 選擇權 商品代碼']+'F' , df[1]['標準型證 券股數/ 受益權單位']))
        multiplier_dict.update(stock_multiplier_dict)
    except :
        pass
    temp = data.loc[data['asset_name'] != '' ,["symbol","asset_name"]].dropna()
    name_dict = dict(set(zip(temp["symbol"] ,temp["asset_name"])))
    
    data = data.groupby(by=["symbol","code"]).agg({"date": [np.min, np.max]})
    cols = data.date.columns
    data.reset_index(inplace=True)
    data["start_date"] = data.date[cols[0]]
    data["first_traded"] = data["start_date"]
    data["end_date"] = data.date[cols[1]]
    data["notice_date"] = pd.NaT
    data["tick_size"] = 1.0
    data["multiplier"] = data['code'].map(multiplier_dict).fillna(2000.0)
    data["asset_name"] = data["symbol"].map(name_dict)
    data['root_symbol'] = data['code']
    
    del data["date"]
    data.columns = data.columns.get_level_values(0)
    data["exchange"] = "TEJ_morning_future"
    data["auto_close_date"] = data["end_date"].values + pd.Timedelta(days=1)
    data["expiration_date"] = data["end_date"].values
    return data

def gen_equity_asset_metadata(data):

    data = data.groupby(by=["symbol","stk_name"]).agg([np.min, np.max])
    cols = data.columns.tolist()
    data.reset_index(inplace=True)
    
    data["asset_name"] = data["stk_name"]
    data["start_date"] = data[cols[0]]
    data["end_date"] = data[cols[1]]
    del data["date"]
    data.columns = data.columns.get_level_values(0)

    data["exchange"] = "TEJ_XTAI"
    data["auto_close_date"] = data["end_date"].values + pd.Timedelta(days=1)
    return data

def gen_root_symbols(data) :

    data = data.drop_duplicates(subset = "root_symbol",keep = "first")
    data["root_symbol"] = data["root_symbol"].astype("category")
    data["root_symbol_id"] = data.root_symbol.cat.codes
    
    return data[["root_symbol","root_symbol_id","exchange"]]


def parse_pricing_and_vol(data, sessions, symbol_map):
    data.fillna({"annotation":"1"} , inplace = True)
    for asset_id, symbol in symbol_map.items():
        asset_data = (
            data.xs(symbol, level=1).reindex(sessions.tz_localize(None)).fillna(0.0)
        )
        yield asset_id, asset_data

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

def parse_dividends(data, show_progress):
    if show_progress:
        log.info("Parsing dividend data.")
    data['pay_date'] = data['out_pay']
    data["record_date"] = data['lastreg']
    data["declared_date"] = data['news_d']
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
    api_key = environ.get('TEJAPI_KEY')
    coid = environ.get('ticker')
    future = environ.get('future')

    if api_key is None:
        raise ValueError(
            "Please set your TEJAPI_KEY environment variable and retry."
        )
    
    tejapi.ApiConfig.api_key = api_key

    if coid :
        coid = re.split('[,; ]',coid)
    
    if future :
        future = re.split('[,; ]',future)

    if coid is None and future is None :
        raise ValueError(
            "Please set company id or future id in your environment variable and retry."
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
        mdate[1] = (pd.to_datetime(mdate[1]) + relativedelta(years = 1)).strftime('%Y%m%d')
        mdate = {'gte':mdate[0],'lte':mdate[1]}
    
    
    source_db = os.environ.get('raw_source')
    future_raw_output_path = os.path.join(output_dir,'future_raw.parquet')
    equity_raw_output_path = os.path.join(output_dir,'equity_raw.parquet')

    columns= ['symbol','date','open','high','low','close','volume','split_ratio','ex_dividend','annotation','out_pay','news_d','lastreg','div_percent']
    if future :
        future_data = fetch_future_data(
            future , mdate
        )
    else :
        future_data = pd.DataFrame()
    if coid :
        equity_data = fetch_equity_data(
            coid , mdate
        )
    else :
        equity_data = pd.DataFrame()
    if source_db :
        equity_source_db = os.path.join(source_db,'equity_raw.parquet')
        equity_origin_raw = pd.read_parquet(equity_source_db)
        
        equity_data = pd.concat([equity_data,equity_origin_raw])
        equity_data['date'] = equity_data['date'].apply(pd.to_datetime)
        equity_data = equity_data.drop_duplicates(subset = ['symbol','date'])
        equity_data = equity_data.reset_index(drop = True)

        future_source_db = os.path.join(source_db,'future_raw.parquet')
        future_origin_raw = pd.read_parquet(future_source_db)
        future_data = pd.concat([future_data,future_origin_raw])
        future_data['date'] = future_data['date'].apply(pd.to_datetime)
        future_data = future_data.drop_duplicates(subset = ['symbol','date'])
        future_data = future_data.reset_index(drop = True)
    
    future_data.to_parquet(future_raw_output_path , index = False )
    equity_data.to_parquet(equity_raw_output_path , index = False )

    if coid :
        equity_asset_metadata = gen_equity_asset_metadata(equity_data[['symbol','stk_name','date']])
    else :
        equity_asset_metadata = equity_data = pd.DataFrame(columns=columns)

    if future :
        future_asset_metadata = gen_future_asset_metadata(future_data)
        root_symobl_data = gen_root_symbols(future_asset_metadata)
    else :
        root_symobl_data = future_asset_metadata = future_data = pd.DataFrame(columns=columns)
        
    future_asset_metadata.index = future_asset_metadata.index + len(equity_asset_metadata)
    
    exchanges = pd.DataFrame(
        data=[["TEJ_XTAI", "TEJ_XTAI", "TW"] , ["TEJ_morning_future", "TEJ_morning_future", "TW"]],
        columns=["exchange", "canonical_name", "country_code"],
    )

    asset_db_writer.write(equities = equity_asset_metadata ,futures = future_asset_metadata ,  exchanges=exchanges , root_symbols = root_symobl_data)

    if future and coid :
        symbol_map = pd.concat([equity_data.symbol , future_data.symbol])
    elif future :
        symbol_map = future_asset_metadata.symbol
    elif coid :
        symbol_map = equity_asset_metadata.symbol
    symbol_map = symbol_map.drop_duplicates(keep = 'first').reset_index(drop = True)
    
    sessions = calendar.sessions_in_range(start_session, end_session)

    data = pd.DataFrame( columns= columns)

    data = pd.concat([data , equity_data])
    data = pd.concat([data , future_data])
    data = data[columns]

    data = data.drop_duplicates(subset = ["date" , "symbol"])

    data.fillna({"ex_dividend" : 0 , "split_ratio" : 1 , "div_percent" : 0} , inplace = True)
    data['ex_dividend'] = data['ex_dividend'].astype(float)
    data['split_ratio'] = data['split_ratio'].astype(float)
    data['div_percent'] = data['div_percent'].astype(float)

    data.set_index(["date", "symbol"], inplace=True)

    daily_bar_writer.write(
        parse_pricing_and_vol(data, sessions, symbol_map),
        show_progress=show_progress,
    )
    
    symbol_map_dict = dict(zip(symbol_map , symbol_map.index))
    data.reset_index(inplace=True)
    data["sid"] = data.symbol.map(symbol_map_dict)

    adjustment_writer.write(
        splits=parse_splits(
            data[
                [
                    "sid",
                    "date",
                    "split_ratio",
                ]
            ].loc[data.split_ratio != 1],
            show_progress=show_progress,
        ),
        dividends=parse_dividends(
            data[
                [
                    "sid",
                    "date",
                    "ex_dividend",
                    'out_pay',
                    'news_d',
                    'lastreg',
                    'div_percent',
                ]
            ].loc[data.ex_dividend != 0],
            show_progress=show_progress,
        ),
    )



register(
name = 'tquant_future',
f = tej_bundle,
calendar_name='TEJ_morning_future',
start_session = None,
end_session = None,
minutes_per_day = 420,
create_writers = True,
)


register_calendar_alias("tquant_future", "TEJ_morning_future")
