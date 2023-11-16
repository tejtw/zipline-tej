# -*- coding: utf-8 -*-
"""
Created on Sat Apr 29 20:19:58 2023

@author: 2020033001
"""
import tejapi
import os
tejapi.ApiConfig.page_limit=10000

import time
import pandas as pd
import numpy as np
import datetime
import matplotlib.pyplot as plt

from zipline.data import bundles
from zipline.assets import Equity  
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.data.data_portal import DataPortal
from zipline.utils.calendar_utils import get_calendar
from zipline.pipeline.loaders.frame import DataFrameLoader
from zipline.pipeline.data import EquityPricing,TWEquityPricing
from zipline.pipeline.engine import SimplePipelineEngine
from TejToolAPI import TejToolAPI
from zipline.pipeline.data import tejquant 

from zipline.pipeline import Pipeline
from zipline.pipeline.factors import Returns, Latest

global bundles_name
bundles_name = 'tquant'

def get_prices(start_date,end_date,field:str ,assets:list=None):   
    
    bundle = bundles.load(bundles_name) 
    trading_calendar =  get_calendar('TEJ_XTAI')     

    for i in range(15):
        if start_date in trading_calendar.all_sessions:
            break        
        start_date = start_date+pd.Timedelta(str(i)+'d')
       
    if assets!=None:
        
        if set(assets).issubset(set(['risk-free-rate'])):
            
            df_rf = (tejapi.get('TWN/ARATE', coid = '5844', 
                              opts={'columns':['mdate', 'fld009']},
                              mdate={'gte':start_date,'lte':end_date},
                              paginate=True).rename(columns={'fld009':'risk_free_rate'})
                              .set_index('mdate')
                              .tz_localize('utc')
                           )/100/252 
            df_rf.index.rename('date',inplace=True)
            
            return df_rf
            
        if set(assets).issubset(set(['benchmark'])):
            
            df_bm = (tejapi.get('TWN/APIPRCD', coid = 'IR0001', 
                             opts={'columns':['mdate', 'close_d']},
                             mdate={'gte':start_date,'lte':end_date},
                             paginate=True).rename(columns={'close_d':'benchmark'})
                             .set_index('mdate')
                             .tz_localize('utc')
                          )
            df_bm.index.rename('date',inplace=True)
            
            return df_bm  
        
        if set(assets).issubset(set(['sector'])):
            
            sector_code = { 'IR0001': '報酬指數',
                'IR0010': '水泥類報酬指數',
                'IR0011': '食品類報酬指數',
                'IR0012': '塑膠類報酬指數',
                'IR0016': '紡織纖維報酬指數',
                'IR0017': '電機機械類報酬指數',
                'IR0018': '電器電纜類報酬指數',
                'IR0020': '化學工業類報酬指數',
                'IR0021': '生技醫療類報酬指數',
                'IR0019': '化學生技醫療類報酬指數',
                'IR0022': '玻璃陶瓷類報酬指數',
                'IR0023': '造紙類報酬指數',
                'IR0024': '鋼鐵類報酬指數',
                'IR0025': '橡膠類報酬指數',
                'IR0027': '電子報酬',
                'IR0026': '汽車類報酬指數',
                'IR0028': '半導體報酬指數',
                'IR0029': '電腦及週邊設備業TR',
                'IR0030': '光電類TR',
                'IR0031': '通信網路類TR',
                'IR0032': '電子零組TR',
                'IR0033': '電子通路業TR',
                'IR0034': '資訊服務業TR',
                'IR0035': '其他電子業TR',
                'IR0036': '營造建材類報酬指數',
                'IR0037': '航運類報酬指數',
                'IR0038': '觀光類報酬指數',
                'IR0039': '金融報酬',
                'IR0040': '百貨貿易類報酬指數',
                'IR0041': '油電燃氣類報酬指數',
                'IR0042': '其他類報酬指數',
                'IR0044': 'OTC 紡織纖維報酬指數',
                'IR0045': 'OTC 電機機械報酬指數',
                'IR0046': 'OTC 鋼鐵工業報酬指數',
                'IR0047': 'OTC 電子工業報酬指數',
                'IR0048': 'OTC 建材營造報酬指數',
                'IR0049': 'OTC 航運業報酬指數',
                'IR0050': 'OTC 觀光事業報酬指數',
                'IR0051': 'OTC 化學工業報酬指數',
                'IR0052': 'OTC 生技醫療報酬指數',
                'IR0053': 'OTC 半導體業報酬指數',
                'IR0054': 'OTC 電腦及週邊設備業報酬指數',
                'IR0055': 'OTC 光電業報酬指數',
                'IR0056': 'OTC 通信網路業報酬指數',
                'IR0057': 'OTC 電子零組件業報酬指數',
                'IR0058': 'OTC 電子通路業報酬指數',
                'IR0059': 'OTC 資訊服務業報酬指數',
                'IR0075': 'OTC 文化創意業報酬指數',
                'IR0099': 'OTC 其他電子業報酬指數',
                'IR0100': 'OTC 其他報酬指數',
                'IR0043': '櫃檯報酬指數'
            }

            df_sector=(tejapi.get('TWN/APIPRCD', coid = list(sector_code.keys()), 
                                         opts={'columns':['mdate','coid', 'close_d']},
                                         mdate={'gte':start_date,'lte':end_date},
                                         paginate=True).set_index(['mdate','coid'])
                                         .unstack()
                                         .tz_localize('utc')
                          )['close_d']
  
            
            df_sector.index.rename('date',inplace=True)
            
            return df_sector        
        
    ohlcv= ('open', 'high', 'low', 'close', 'volume')
    
    if field not in ohlcv:               
        raise ValueError(f'Invalid field:{field}') 
    
    if assets==None:    
        assets =  bundle.asset_finder.retrieve_all(bundle.asset_finder.equities_sids)        
    else:
        if isinstance(assets,list)==False:
            raise ValueError("assets should be list") 
        
        niter= 0   
        for a in assets:
            if isinstance(a,Equity)==False:        
                try:
                    assets[niter] = bundle.asset_finder.lookup_symbol(a, as_of_date=None)
                    niter=niter+1
                except:                
                    raise ValueError(f"Invalid zipline symbol: {a}")  
       
    bar_count = trading_calendar.session_distance(start_date,end_date)
    
    portal = DataPortal(bundle.asset_finder,
                       trading_calendar = bundle.equity_daily_bar_reader.trading_calendar,
                       first_trading_day = start_date,                         
                       equity_daily_reader = bundle.equity_daily_bar_reader,
                       adjustment_reader = bundle.adjustment_reader)
            
    prices =  portal.get_history_window(assets=assets, 
                                        end_dt=end_date, 
                                        bar_count=bar_count,
                                        frequency='1d',
                                        field=field,
                                        data_frequency='daily')
    
    prices.index.rename('date',inplace=True)
    
    prices.dropna(how='all',axis=0,inplace=True)
    
    return prices 
    

        

def getToolData(assets:list,query_columns:list,dataframelike:pd.DataFrame):
      
    if query_columns==None:
        raise ValueError('query_columns should not be none')     
            
    if isinstance(assets[0],Equity): 
        assets=[a.symbol for a in assets]
    
    st ,et = dataframelike.index[[0,-1]] 
    
    if 'Industry' not in query_columns:
        query_columns.append('Industry')
    
    if 'Market_Cap_Dollars' not in query_columns:
        query_columns.append('Market_Cap_Dollars')        
    
    out ={}    
    for col in query_columns:
        
        try :        
            columns_col = col
            if col.split('_')[-1] in ('Q','A','TTM'):                
                columns_col = '_'.join(col.split('_')[:-1])
            
            fdata = (TejToolAPI.get_history_data(ticker=assets, columns=[columns_col], 
                        start = st.tz_convert(None),
                        end = et.tz_convert(None),
                        transfer_to_chinese=False)
                        )
            
            try :
                fdata = (fdata.set_index(['mdate','coid'])
                             .unstack()                  
                             .rename(columns={c.symbol:c  for c in dataframelike.columns})
                             )[col]     
            except:
            
                fdata = (fdata.set_index(['mdate','coid'])
                             .unstack()                  
                             .rename(columns={c.symbol:c  for c in dataframelike.columns})
                             )              
                         
            # if col=='Issue_Shares_1000_Shares':
                # fdata=fdata['Common_Stock_Shares_Issued_Thousand_Shares']
                
            # if isinstance(fdata.columns,pd.MultiIndex):
                # fdata.columns = fdata.columns.droplevel(0)
              
        except Exception as err:            
             
            raise    

        if (fdata.index.dtype==dataframelike.index.dtype): 
            pass
        else:
            if isinstance(fdata.index ,type(dataframelike.index))==False: #  hasattr(df1.index, 'tz')
                fdata.index = pd.to_datetime(fdata.index)
        
            if (fdata.index.tz != dataframelike.index.tz):        
                fdata = fdata.tz_localize(tz='UTC')                

        adjustments = fdata.reindex(dataframelike.columns, axis=1)
        adjustments = adjustments.reindex(dataframelike.index, axis=0,method='ffill') 
        
        if col=='Industry':           
            adjustments = adjustments.fillna(method='ffill').fillna(method='bfill')
        
        out[col] = adjustments

    return out


#@abstractmethod
def run_pipeline(pipeline, start_date, end_date, out:dict=None):
    
    if out!=None:
        loaders={}   
        for c in tejquant.TQDataSet.columns:    
            if c.name in list(out.keys()):   
                #print(c.name)
                loaders.update({c:DataFrameLoader(c,out[c.name])})    
        
    bundle_data = bundles.load(bundles_name)
    pricing_loader =EquityPricingLoader.without_fx(bundle_data.equity_daily_bar_reader, bundle_data.adjustment_reader)
         
    def choose_loader(column):
        if column in TWEquityPricing.columns:
            return pricing_loader
        return loaders[column]    
    
    # Create a Pipeline engine
    engine = SimplePipelineEngine(get_loader = choose_loader,
                                  asset_finder = bundle_data.asset_finder)
    
    return engine.run_pipeline(pipeline, start_date, end_date)

 

   