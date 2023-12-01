"""
See also
----------------------------------
:class:`zipline.algorithm.TradingAlgorithm`
:func:`zipline.api.fetch_csv`
"""

import TejToolAPI
import zipline.algorithm as algo

from zipline.utils.calendar_utils import get_calendar
from zipline.utils.input_validation import (
    expect_types,
    validate_keys,
    optional
)
from zipline.errors import (MultipleSymbolsFound,
                            SymbolNotFound,
                            IllegalValueException,
                            EmptyOutputException
                            )

import zipline.pipeline.domain as domain
from zipline.pipeline.pipeline import Pipeline
from zipline.pipeline.data import tejquant, EquityPricing
from zipline.pipeline.loaders.frame import DataFrameLoader
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.pipeline.engine import SimplePipelineEngine

from zipline.data import bundles

import os
import re
import pandas as pd
import numpy 
import tejapi
import datetime
import pytz
from logbook import Logger

logger = Logger("Requests Source Logger(TEJ API)")


tejapi.ApiConfig.page_limit=10000

try :
    tejapi.ApiConfig.api_base = os.environ['TEJAPI_BASE']
except :
    raise KeyError("Cant't load TEJAPI_BASE to get dynamic calendar information. Please set your TEJAPI_BASE. os.environ['TEJAPI_BASE'] = <YOUR_BASE>")
try :
    tejapi.ApiConfig.api_key = os.environ['TEJAPI_KEY']
except :
    raise KeyError("Cant't load TEJAPI_KEY to get dynamic calendar information. Please check your TEJAPI_KEY. os.environ['TEJAPI_KEY'] = <YOUR_KEY>")



# Map from liquidity risk management rules to columns name in tej tool api.
# See also：func:`algorithm.set_liquidity_risk_management_rule`.
LIQUIDITY_RISK_COLUMNS = {'Full_Delivery':'Full_Delivery_Stock_Fg',
                          'Attention':'Attention_Stock_Fg',
                          'Disposition':'Disposition_Stock_Fg',
                          #'Suspended':'Suspended_Trading_Stock_Fg',
                          'Limit_Up_or_Down_in_Opening':'Limit_Up_or_Down_in_Opening_Fg',
                          'Suspension_of_Buy_After_Day_Trading':'Suspension_of_Buy_After_Day_Trading_Fg'
}

# Map bundle name to DataSet and calendar_name.
# 新增bundle時需新增BUNDLE_INFO
BUNDLE_INFO = {'tquant': {'bundle_name':'tquant',
                          'calendar_name':'TEJ_XTAI',
                          'DataSet':tejquant.TQDataSet}
}

# For get_universe
# VALID_PARA_IDXSAMPLE = ['idx_id']
# VALID_PARA_ATTR = ['mkt', 'mkt_bd', 'stktp_c', 'stktp_e', 'industry_c', 'industry_e']
FSI_e = ['M2800 Financial Industry',
         'OTC30 OTC Securities',
         'M3000 Securities',
         'OTC28 OTC Banking']
FSI_c = ['M2800 金融業',
         'OTC28 OTC 金融業',
         'M3000 証券',
         'OTC30 OTC 證券類']
GI_e = ['M1100 Cement',
        'M1200 Foods',
        'M1300 Plastics',
        'M1400 Textiles',
        'M1500 Electric Machinery',
        'M1600 Electrical and Cable',
        'M1700 Chemical Biotechnology & Medical Care',
        'M1800 Glass and Ceramics',
        'M1900 Paper and Pulp',
        'M2000 Iron and Steel',
        'M2100 Rubber',
        'M2200 Automobile',
        'M2300 Electronics',
        'M2500 Building Material and Construction',
        'M2600 Shipping and Transportation',
        'M2700 Tourism',
        "M2900 Trading and Consumers' Goods",
        'M3700 TSE Sports and Leisure',
        'M3800 TSE Household',
        'M9700 Oil Gas, and Electricity',
        'M9900 Others',
        'OTC12 OTC Foods',
        'OTC13 OTC Plastic',
        'OTC14 OTC Textile',
        'OTC15 OTC Electronical',
        'OTC16 OTC Wire & Cable',
        'OTC17 OTC Chemical & Biotech',
        'OTC18 OTC Glass',
        'OTC20 OTC Steel',
        'OTC21 OTC Rubbers',
        'OTC22 OTC Automobile',
        'OTC23 OTC Electronic',
        'OTC25 OTC Construction',
        'OTC26 OTC Transporation',
        'OTC27 OTC Tourism',
        'OTC29 OTC Retailing',
        'OTC32 Cultural & Creative Industry',
        'OTC33 OTC Agricultural Science and Technology',
        'OTC34 OTC E-commerce',
        'OTC37 OTC Sports and Leisure',
        'OTC38 OTC Household',
        'OTC89 OTC Others',
        'OTC97 OTC Gas and Electricity Industry']

GI_c = ['M1100 水泥工業',
        'M1200 食品工業',
        'M1300 塑膠工業',
        'M1400 紡織纖維',
        'M1500 電機機械',
        'M1600 電器電纜',
        'M1700 化學生技醫療',
        'M1800 玻璃陶瓷',
        'M1900 造紙工業',
        'M2000 鋼鐵工業',
        'M2100 橡膠工業',
        'M2200 汽車工業',
        'M2300 電子工業',
        'M2500 建材營造',
        'M2600 航運業',
        'M2700 觀光餐旅',
        'M2900 貿易百貨',
        'M3700 運動休閒',
        'M3800 居家生活',
        'M9700 油電燃氣業',
        'M9900 其他',
        'OTC12 OTC 食品工業',
        'OTC13 OTC 塑膠工業',
        'OTC14 OTC 紡織纖維',
        'OTC15 OTC 電機機械',
        'OTC16 OTC 電器電纜',
        'OTC17 OTC 化學生技',
        'OTC18 OTC 玻璃',
        'OTC20 OTC 鋼鐵工業',
        'OTC21 OTC 橡膠類',
        'OTC22 OTC 汽車類',
        'OTC23 OTC 電子類',
        'OTC25 OTC 建材營造',
        'OTC26 OTC 航運業',
        'OTC27 OTC 觀光餐旅',
        'OTC29 OTC 百貨類',
        'OTC32 文化創意業',
        'OTC33 OTC 農業科技',
        'OTC34 OTC 電子商務',
        'OTC37 OTC 運動休閒',
        'OTC38 OTC 居家生活',
        'OTC89 OTC 其它',
        'OTC97 OTC 油電燃氣業']

SPECIAL_MAIN_IND_E = {'General Industry': GI_e,
                      'Financial and Securities Industry':FSI_e,
}
SPECIAL_MAIN_IND_C = {'一般產業': GI_c,
                      '金融與證券業':FSI_c,
}
SPECIAL_KWARGS = {'main_ind_c':SPECIAL_MAIN_IND_C,
                  'main_ind_e':SPECIAL_MAIN_IND_E}


# get_history_data:設定transfer_to_chinese = False，其餘與TejToolAPI相同。
def get_history_data(ticker,
                     columns,
                     start=None,
                     end=None,
                     fin_type=None,
                     include_self_acc=None,
                     **kwargs
                     ):

    try:

        df = TejToolAPI.get_history_data(ticker = ticker,
                                         columns = columns,
                                         start = start,
                                         end = end,
                                         transfer_to_chinese = False,
                                         fin_type = fin_type,
                                         include_self_acc = include_self_acc,
                                         **kwargs)
        df = df.reset_index(drop=True)

    except Exception as e:
        raise ValueError(f'Error occurs while downloading data by func:`TejToolAPI.get_history_data` due to {e} .')

    return df

#################
### pipeline  ###
#################
@expect_types(df=pd.DataFrame,
              bundle_name=str or type(None))  # FIXME use zipline.utils.input_validation.optional
def to_dict(df, bundle_name='tquant'):

    bundle = bundles.load(bundle_name)

    dfs = {}
    df['coid'] = df['coid'].astype(str)
    df = df.set_index(['coid', 'mdate'])

    for i in df.columns:
        symbols = df.index.get_level_values(0).unique().tolist()
        assets = bundle.asset_finder.lookup_symbols(symbols, as_of_date=None)
        sids = pd.Int64Index([asset.sid for asset in assets])
        symbol_map = dict(zip(symbols, sids))
        dfs[i] = (df[i]
                 .unstack('coid')
                 .rename(columns=symbol_map)
                 .tz_localize('UTC')
                 )

    return dfs

@expect_types(dfs=dict,
              bundle_name=str or type(None)          # FIXME use zipline.utils.input_validation.optional
)
def create_custom_loader(dfs, bundle_name='tquant'):

    custom_loader = {}
    column_names = []

    bundle = bundles.load(bundle_name)
    dataset = BUNDLE_INFO.get(bundle_name).get('DataSet')

    for i in dataset.columns:
        if i.name in list(dfs.keys()):
            custom_loader.update({i:DataFrameLoader(i,dfs[i.name])})
            column_names.append(i.name)

    logger.info(
                "\n `DataSet` used: {data},\n `Column` used: {Column},\n column names: {column_names}",
                data=str(dataset),
                Column=list(custom_loader.keys()),
                column_names=column_names,
                )

    return custom_loader

@expect_types(bundle_name=str or type(None),              # FIXME use zipline.utils.input_validation.optional
              #to_db=type(None),
              #db=type(None)
)
def create_custom_loader_for_algo(ticker,
                                  columns,
                                  start=None,
                                  end=None,
                                  fin_type=None,
                                  include_self_acc=None,
                                  bundle_name='tquant'):
                                  #to_db=None,           # TODO:若db不存在，建db
                                  #db=None):             # TODO:先從db撈資料，若fail再get_history_data()

    raw = get_history_data(ticker,
                           columns,
                           start,
                           end,
                           fin_type,
                           include_self_acc
                           )

    dfs = to_dict(raw, bundle_name)

    custom_loader = create_custom_loader(dfs, bundle_name)

    return raw, custom_loader


@expect_types(custom_loader=dict,
              bundle_name=str or type(None),     # FIXME use zipline.utils.input_validation.optional
              pipeline=Pipeline
)
def run_pipeline(custom_loader, pipeline, start_date, end_date, bundle_name='tquant'):

    bundle = bundles.load(bundle_name)
    dataset = BUNDLE_INFO.get(bundle_name).get('DataSet')
    calendar_name = BUNDLE_INFO.get(bundle_name).get('calendar_name')

    pricing_loader = EquityPricingLoader.without_fx(bundle.equity_daily_bar_reader,
                                                    bundle.adjustment_reader)

    def choose_loader(column):
        if column.name in EquityPricing._column_names:
            return pricing_loader
        elif column.name in dataset._column_names:
            return custom_loader[column]
        else:
            raise Exception('Column: {} not available'.format(str(column.name)))

    engine = SimplePipelineEngine(get_loader = choose_loader,
                                  asset_finder = bundle.asset_finder,
                                  default_domain = algo._DEFAULT_DOMAINS.get(calendar_name))

    pipeline_output = engine.run_pipeline(pipeline, start_date, end_date)

    return pipeline_output


#####################
### FETCH_TEJ_API ###
#####################

class PandasRequestsTEJ_API(object):

    '''
    新增data_source到data_portal供run_algorithm()回測時使用
    see also：zipline.algorithm
    '''

    def __init__(
        self,
        symbol_column,
        date_column,
        date_format,
        trading_day,
        asset_finder,
        columns,
        symbols,
        timezone,
        start,
        end, 
        fin_type,
        include_self_acc,
        import_data,
        country_code,
        pre_func,
        post_func,
        data_frequency,
        **kwargs,
    ):
        self.timezone = timezone
        self.symbol_column = symbol_column or "coid"
        self.date_column = date_column or "mdate"
        self.data_frequency = data_frequency
        self.country_code = country_code
        self.date_format = None
        
        #self.trading_calendar=trading_calendar
        self.finder = asset_finder
        self.trading_day = trading_day
        #self.trading_day=self.trading_calendar.day
        
        self.start = start
        self.end = end
        self.columns = columns
        self.symbols = symbols
        self.fin_type = fin_type
        self.include_self_acc = include_self_acc
        self.pre_func = pre_func
        self.post_func = post_func
        self.import_data=import_data
        #self.df = self.load_df()

    def get_history_data(self):

        df = get_history_data(ticker = self.symbols,
                              columns = self.columns,
                              start = self.start,
                              end = self.end,
                              fin_type = self.fin_type,
                              include_self_acc = self.include_self_acc)
        if len(df)==0:
            raise EmptyOutputException(function = '"PandasRequestsTEJ_API.get_history_data"',
                                       dtype = '"dataframe"')

        return df


    # 時區，要改?
    def roll_dts_to_midnight(self, dts, trading_day):
    
        if len(dts) == 0:
            return dts

        return (
            pd.DatetimeIndex(
                (dts.tz_convert("US/Eastern") - pd.Timedelta(hours=16)).date,
                tz="UTC",
            )
            + trading_day
        )

    
    def parse_date_str_series(
        self, format_str, tz, date_str_series, data_frequency, trading_day
    ):
        """
        Efficient parsing for a 1d Pandas/numpy object containing string
        representations of dates.

        Note: pd.to_datetime is significantly faster when no format string is
        passed, and in pandas 0.12.0 the %p strptime directive is not correctly
        handled if a format string is explicitly passed, but AM/PM is handled
        properly if format=None.

        Moreover, we were previously ignoring this parameter unintentionally
        because we were incorrectly passing it as a positional.  For all these
        reasons, we ignore the format_str parameter when parsing datetimes.
        """

        # Explicitly ignoring this parameter.  See note above.
        if format_str is not None:
            logger.warn(
                "The 'format_str' parameter to fetch_csv is deprecated. "
                "Ignoring and defaulting to pandas default date parsing."
            )
            format_str = None

        tz_str = str(tz)
        if tz_str == pytz.utc.zone:
            parsed = pd.to_datetime(
                date_str_series.values,
                format=format_str,
                utc=True,
                errors="coerce",
            )
        else:
            parsed = (
                pd.to_datetime(
                    date_str_series.values,
                    format=format_str,
                    errors="coerce",
                )
                .tz_localize(tz_str)
                .tz_convert("UTC")
            )

        if data_frequency == "daily":
            parsed = self.roll_dts_to_midnight(parsed, trading_day)
        return parsed
        
        
                
    def _lookup_unconflicted_symbol(self, symbol):
        """
        Attempt to find a unique asset whose symbol is the given string.

        If multiple assets have held the given symbol, return a 0.

        If no asset has held the given symbol, return a  NaN.
        """
        
        try:
            uppered = symbol.upper()
            
        except AttributeError:
            # The mapping fails because symbol was a non-string
            return numpy.nan

        try:
            return self.finder.lookup_symbol(
                uppered,
                as_of_date=None,
                country_code=self.country_code,
            )
        except MultipleSymbolsFound:
            # Fill conflicted entries with zeros to mark that they need to be
            # resolved by date.
            return 0
            
        except SymbolNotFound:
            # Fill not found entries with nans.
            
            return numpy.nan

    def load_df(self):
               
        if self.import_data is None:
            df = self.get_history_data() 
            df = df.sort_values(by=['mdate','coid'])

        elif self.import_data is not None:
            df = self.import_data
        else:
            pass
        
        if self.pre_func:
            df.reset_index(drop=True,inplace=True)
            df = self.pre_func(df)
        
        #必要(不然df.iloc[row_idx, df.columns.get_loc("sid")] = asset這個語法會有問題，導致sid配錯)
        df.reset_index(drop=True,inplace=True)   
        
        # Batch-convert the user-specifed date column into timestamps.
        df["dt"] = self.parse_date_str_series(
            self.date_format,
            self.timezone,
            df[self.date_column],
            self.data_frequency,
            self.trading_day,
        ).values
        

        # ignore rows whose dates we couldn't parse
        df = df[df["dt"].notnull()]
        
        # if self.symbol is not None:
        #    df["sid"] = self.symbol
        
        if self.finder:

            df.sort_values(by=self.symbol_column, inplace=True)

            # Pop the 'sid' column off of the DataFrame, just in case the user
            # has assigned it, and throw a warning
            try:
                df.pop("sid")
                warnings.warn(
                    "Assignment of the 'sid' column of a DataFrame is "
                    "not supported by Fetcher. The 'sid' column has been "
                    "overwritten.",
                    category=UserWarning,
                    stacklevel=2,
                )
            except KeyError:
                # There was no 'sid' column, so no warning is necessary
                pass

                       
            # Fill entries for any symbols that don't require a date to
            # uniquely identify.  Entries for which multiple securities exist
            # are replaced with zeroes, while entries for which no asset
            # exists are replaced with NaNs.
            unique_symbols = df[self.symbol_column].unique()
            sid_series = pd.Series(
                data=map(self._lookup_unconflicted_symbol, unique_symbols),
                index=unique_symbols,
                name="sid",
            )
            df = df.join(sid_series, on=self.symbol_column)        

            # Fill any zero entries left in our sid column by doing a lookup
            # using both symbol and the row date.
            conflict_rows = df[df["sid"] == 0]
            for row_idx, row in conflict_rows.iterrows():
                try:
                    asset = (
                        self.finder.lookup_symbol(
                            row[self.symbol_column],
                            # Replacing tzinfo here is necessary because of the
                            # timezone metadata bug described below.
                            row["dt"].replace(tzinfo=pytz.utc),
                            country_code=self.country_code,
                            # It's possible that no asset comes back here if our
                            # lookup date is from before any asset held the
                            # requested symbol.  Mark such cases as NaN so that
                            # they get dropped in the next step.
                        )
                        or numpy.nan
                    )
                except SymbolNotFound:
                    asset = numpy.nan

                # Assign the resolved asset to the cell
                df.iloc[row_idx, df.columns.get_loc("sid")] = asset
            
            # Filter out rows containing symbols that we failed to find.
            length_before_drop = len(df)
            df = df[df["sid"].notnull()]
            no_sid_count = length_before_drop - len(df)
            if no_sid_count:
                logger.warn(
                    "Dropped {} rows from fetched tej api.".format(no_sid_count),
                    no_sid_count,
                    extra={"syslog": True},
                )
        else:
            pass
        
                   
        # Dates are localized to UTC when they come out of
        # parse_date_str_series, but we need to re-localize them here because
        # of a bug that wasn't fixed until
        # https://github.com/pydata/pandas/pull/7092.
        # We should be able to remove the call to tz_localize once we're on
        # pandas 0.14.0

        # We don't set 'dt' as the index until here because the Symbol parsing
        # operations above depend on having a unique index for the dataframe,
        # and the 'dt' column can contain multiple dates for the same entry.
        df.drop_duplicates(["sid", "dt"])
        df.set_index(["dt"], inplace=True)
        df = df.tz_localize("UTC")
        df.sort_index(inplace=True)
        

        cols_to_drop = [self.date_column]
        
        cols_to_drop.append(self.symbol_column)
        df = df[df.columns.drop(cols_to_drop)]
        
        if self.post_func:
            df = self.post_func(df)

        return df


#################
### Benchmark ###
#################
@expect_types(symbol=str or type(None))         # FIXME use zipline.utils.input_validation.optional
def get_Benchmark_Return(start,
                         end,
                         symbol='IR0001'):
    '''
    為了計算benchmark_period_return,benchmark_volatility...
    
    return 
    ------------
    Series
    '''
    
    # TODO:chk symbol是否為報酬指數(用coid前兩碼是否為IR來判定)

    TEJ_Api_data_source=PandasRequestsTEJ_API(
                            symbol_column = None,
                            date_column = None,                                       
                            date_format = None,
                            trading_day = None,
                            asset_finder= None,
                            columns = ['roi'], # ['coid','mdate','roi']
                            symbols = [symbol],
                            timezone = None,
                            start = start,
                            end = end, 
                            fin_type = None,
                            include_self_acc = None,
                            import_data = None, 
                            country_code = None,
                            data_frequency = None,
                            pre_func = None,
                            post_func = None)
       
    df = TEJ_Api_data_source.get_history_data()

    if len(df)==0:
        raise EmptyOutputException(function = '"get_Benchmark_Return"',
                                   dtype = '"dataframe"')

    #將利率單位轉成%
    df.iloc[:,2] = df.iloc[:,2] / 100                                #預防欄位名稱更改(改用iloc)
    ser = pd.Series(data = df.iloc[:,2].values, index = df['mdate']) #預防欄位名稱更改(改用iloc)

    #參考: zipline\data\benchmarks.py    
    if not ser.index.tz:
        ser = ser.tz_localize("utc")

    return ser.sort_index(ascending=True)

#################
### Treasury  ###
#################
def Treasury_Return_TW(start,
                       end,
                       rate_type,
                       term,
                       symbol = None):
    '''
    TW Treasury：為了計算excess return...

    return 
    ------------
    Series
    '''
    valid_rate_type = ['Time_Deposit_Rate','TAIBOR','Gov_Bond']
    
    Time_Deposit_Rate_terms = {'1m':'fld005',
                               '3m':'fld006',
                               '6m':'fld007',
                               '9m':'fld008',
                               '1y':'fld009'}

    Benchmark_Gov_Bond_terms = {#'0y':'TY00',
                                '2y':'TY02',
                                '5y':'TY05',
                                '10y':'TY10',
                                '12y':'TY12',
                                '20y':'TY20',
                                '30y':'TY30'}

    TAIBOR_terms = {'1w':'tw1',
                    '2w':'tw2',
                    '1m':'tm1',
                    '2m':'tm2',
                    '3m':'tm3',
                    '6m':'tm6',
                    '9m':'tm9',
                    '1y':'tm12'}

    Valid_Time_Deposit_Rate = {'5844':'第一銀行'}

    #TODO：IllegalValueException改為zipline.utils.input_validation.validate_keys／expect_element
    if rate_type not in valid_rate_type:
        raise IllegalValueException(parameter = '"rate_type"',
                                    value = valid_rate_type)

    elif rate_type=='Time_Deposit_Rate' and term not in Time_Deposit_Rate_terms.keys():
         raise IllegalValueException(parameter = '"terms"',
                                     value = str(list(set(Time_Deposit_Rate_terms.keys()))))

    elif rate_type=='Gov_Bond' and term not in Benchmark_Gov_Bond_terms.keys():
         raise IllegalValueException(parameter = '"terms"',
                                     value = str(list(set(Benchmark_Gov_Bond_terms.keys()))))

    elif rate_type=='TAIBOR' and term not in TAIBOR_terms.keys():
         raise IllegalValueException(parameter = '"terms"',
                                     value = str(list(set(TAIBOR_terms.keys()))))

    else:
        pass


    # 銀行定存利率
    if rate_type=='Time_Deposit_Rate':

        if symbol not in Valid_Time_Deposit_Rate:
            raise IllegalValueException(parameter = '"symbol"',
                                    value = str(list(set(Valid_Time_Deposit_Rate.keys()))))

        df = tejapi.get('TWN/ARATE',
                        coid = '5844',
                        opts = {'columns':['coid','mdate',Time_Deposit_Rate_terms[term]]},
                        mdate = {'gte':start,'lte':end},
                        paginate = True)

    # TAIBOR
    elif rate_type=='TAIBOR':
        df = tejapi.get('GLOBAL/WIBOR1',
                       coid = 'Z9999',
                       opts = {'columns':['coid','mdate',TAIBOR_terms[term]]},
                       mdate = {'gte':start,'lte':end},
                       paginate = True)        

    # 指標公債殖利率
    elif rate_type=='Gov_Bond':
        df = tejapi.get('TWN/AGBD8A',
                        coid = Benchmark_Gov_Bond_terms[term],
                        opts = {'columns':['coid','mdate','yield']},
                        mdate = {'gte':start,'lte':end},
                        paginate = True)


    return df

@expect_types(
    rate_type=str,
    term=str,
    symbol=optional(str)
    )
def get_Treasury_Return(start,
                        end,
                        rate_type,
                        term,
                        symbol = None,
                        trading_calendar = get_calendar('TEJ_XTAI')):

    '''
    為了計算excess return...

    return 
    ------------
    Series
    '''

    country_code = algo._DEFAULT_FETCH_CSV_COUNTRY_CODES.get(trading_calendar.name)

    '''
    # query country_code
    # see also : zipline.country
    from iso3166 import countries_by_name
    name = "TAIWAN, PROVINCE OF CHINA"
    print(countries_by_name[name].alpha2)
    '''

    TREASURY_FUNC_NAMES = {
        'TW': Treasury_Return_TW
    }

    if country_code not in TREASURY_FUNC_NAMES.keys():
        raise IllegalValueException(parameter = '"country_code"',
                                    value = str(list(set(TREASURY_FUNC_NAMES.keys()))))
    try:
        df = TREASURY_FUNC_NAMES[country_code](start = start,
                                               end = end,
                                               rate_type = rate_type,
                                               term = term,
                                               symbol = symbol)
    except Exception as e:
            raise ValueError(f'Error occurs while downloading data by get_history_data() due to {e} .')

    if len(df)==0:
        raise EmptyOutputException(function = '"get_Treasury_Return"',
                                   dtype = '"dataframe"')

    #將利率單位轉成%，再將年利率轉為日利率
    df.iloc[:,2] = df.iloc[:,2].apply(lambda x: pow((1 + x / 100), (1 / 252)) - 1) 

    ser = pd.Series(data = df.iloc[:,2].values, index = df['mdate'])

    #See also：zipline.data.benchmarks
    if not ser.index.tz:
        ser = ser.tz_localize("utc")

    return ser.sort_index(ascending=True)


#################
### Universe  ###
#################

def get_universe_TW(start,
                    end=datetime.datetime.now().date().strftime('%Y-%m-%d'),
                    **kwargs):

#     chk **kwargs valid or not 
    attr_table_info = tejapi.table_info('TWN/APISTKATTR')
    valid_para_attr = list(set(attr_table_info['filters'])-set(attr_table_info['primaryKey']))
    valid_para_idxsample = ['idx_id']

    kwargs_valid = {k: v for k, v in kwargs.items() if k in valid_para_attr + valid_para_idxsample}
    validate_keys(kwargs, kwargs_valid, '**kwargs')

#     將idx_id中前兩碼為IR的改為IX 
    for k,v in kwargs_valid.items():
        if k == "idx_id":
            if type(v)==str:
                v=[v]

            kwargs_valid.update({k: [re.sub(r'^(I)(R)', r'\1X', i) for i in v]})

    logger.info(
        "Filters：%s" % kwargs_valid
    )

#     拆分 **kwargs
    kwargs_idxsample = {k: v for k, v in kwargs_valid.items() if k in valid_para_idxsample}
    kwargs_attr = {k: v for k, v in kwargs_valid.items() if k in valid_para_attr}

#     處理特殊 kwargs_attr
    for k, v in list(kwargs_attr.items()):
        if k in SPECIAL_KWARGS:
            # 如果v是字典，且包含'ne'這個key
            if isinstance(v, dict) and 'ne' in v:
                # 用SPECIAL_KWARGS[k]中對應的值来更新
                ne_values = v['ne']  # 取得'ne'對應的列表
                updated_ne_values = []
                for ne_item in ne_values:
                    if ne_item in SPECIAL_KWARGS[k]:  # 如果'ne'列表中的項在SPECIAL_KWARGS[k]中有對應的值，則更新
                        updated_ne_values.extend(SPECIAL_KWARGS[k][ne_item])
                    else:
                        updated_ne_values.append(ne_item)  # 如果没有對應的值，則保持原樣
                kwargs_attr[k] = {'ne': updated_ne_values}
            # 如果v是字串，檢查是否在SPECIAL_KWARGS[k]中
            elif isinstance(v, str) and v in SPECIAL_KWARGS[k]:
                kwargs_attr[k] = SPECIAL_KWARGS[k][v]
            # 如果v是列表，檢查列表中的任何元素是否在SPECIAL_KWARGS[k]中
            elif isinstance(v, list):
                updated_list = []
                for item in v:
                    if item in SPECIAL_KWARGS[k]:  # 更新與SPECIAL_KWARGS[k][item]配對的元素
                        updated_list.extend(SPECIAL_KWARGS[k][item])
                    else:
                        updated_list.append(item)  # 保留原始元素
                kwargs_attr[k] = updated_list


#     TWN/EWISAMPLE
    if len(kwargs_idxsample) > 0:

        try:
            idxsample = tejapi.fastget('TWN/EWISAMPLE',
                                        start_date={'lte':end},
                                        opts={'columns':['idx_id','coid','start_date','end_date']},
                                        paginate=True,
                                        **kwargs_idxsample
                                        )
        except Exception as e:
            raise ValueError(f'Error occurs while downloading data by func:`tejapi.fastget` due to {e} .')

        Illegal_idx_id = [i for i in kwargs_valid["idx_id"] if i not in idxsample.idx_id.unique()]

        if kwargs_valid.__contains__("idx_id") & len(Illegal_idx_id) > 0:

            logger.info(
                "Illegal idx_id：%s, skip it!" %Illegal_idx_id
            )

        symbol_idxsample = set(idxsample[(idxsample.end_date.isnull()) | (idxsample.end_date>=start)].coid)

        if len(kwargs_attr) == 0:
            symbol_attr = symbol_idxsample.copy()

#     TWN/APISTKATTR
    if (len(kwargs_attr) > 0) | ((len(kwargs_attr) == 0) & (len(kwargs_idxsample) == 0)):
        try:
            attr = tejapi.fastget('TWN/APISTKATTR',
                                  mdate={'gte':start, 'lte':end},
                                  opts={'columns':['coid']},
                                  paginate=True,
                                  **kwargs_attr)

        except Exception as e:
            raise ValueError(f'Error occurs while downloading data by func:`tejapi.fastget` due to {e} .')

        symbol_attr = set(attr.coid)

        if len(kwargs_idxsample) == 0:
            symbol_idxsample = symbol_attr.copy()

    return list(symbol_attr & symbol_idxsample)


def get_universe(start,
                 end=datetime.datetime.now().date().strftime('%Y-%m-%d'),
                 trading_calendar = get_calendar('TEJ_XTAI'), 
                 **kwargs):
    """
    取得樣本公司清單

    return
    ------------
    List
    """

    country_code = algo._DEFAULT_FETCH_CSV_COUNTRY_CODES.get(trading_calendar.name)

    UNIVERSE_FUNC_NAMES = {
        'TW': get_universe_TW
    }

    if country_code not in UNIVERSE_FUNC_NAMES.keys():
        raise IllegalValueException(parameter = '"country_code"',
                                    value = str(list(set(UNIVERSE_FUNC_NAMES.keys()))))
    try:
        lst = UNIVERSE_FUNC_NAMES[country_code](start,
                                                end,
                                                **kwargs)
    except Exception as e:
            raise ValueError(f'Error occurs while downloading data func:`tejapi.fastget` due to {e} .')

    if len(lst)==0:
        logger.info(
            "function 'get_universe' return an empty 'list'."
        )

    return sorted(lst)