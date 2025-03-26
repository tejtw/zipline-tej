import re
import pandas as pd
from zipline.data import bundles
from zipline.data.data_portal import DataPortal
from zipline.utils.calendar_utils import get_calendar

cn_col_name = ['名稱', '年月日', '多方交易口數', '多方交易契約金額', '多方交易口數比重', '空方交易口數', '空方交易契約金額',
               '空方交易口數比重', '多空交易口數淨額', '多空交易契約金額淨額', '多方未平倉口數', '多方未平倉契約金額',
               '多方未平倉口數持有比', '空方未平倉口數', '空方未平倉契約金額', '空方未平倉口數持有比', '多空未平倉口數淨額',
               '多空未平倉契約淨額', '身份別名稱(中)', '身份別名稱(英)']

en_col_name = ['coid','mdate','volume_con_long','volume_amt_long','volume_con_long_pct','volume_con_short','volume_amt_short','volume_con_short_pct',
               'volume_con_ls_net','volume_amt_ls_net','oi_con_long','oi_amt_long','oi_con_long_pct','oi_con_short','oi_amt_short',
               'oi_con_short_pct','oi_con_ls_net','oi_amt_ls_net','name_c', 'name_e']

inst_col_name_mapping= dict(zip(cn_col_name, en_col_name ))

class RequestFutInstitutions:
    def __init__(self, start_date, end_date):
        self.coid = None
        self.start_date = start_date
        self.end_date = end_date

    # 使用 @staticmethod 來定義靜態方法，這些方法不依賴於類實例
    @staticmethod
    def request_data_from_api(coid, start_date, end_date):
        # 假設 tejapi 是有效的 API 客戶端
        data = tejapi.fastget('TWN/AFINST',coid=coid, mdate={'gte': start_date, 'lte': end_date}, paginate=True, chinese_column_name=True)
        data.rename(columns=inst_col_name_mapping,inplace=True)   
        return data
    
    # 使用 @staticmethod 來處理資料
    @staticmethod
    def process_data(data):
        # 資料處理：根據 coid 欄位來生成 asset_type 和 invest_type
        data['asset_type'] = data['coid'].apply(lambda x: 'futures' if str(x).startswith('1') 
                                                 else 'options' if str(x).startswith('2') 
                                                 else 'derivatives')
        data['invest_type'] = data['coid'].apply(lambda x: 'dealers' if str(x)[1] == '1' 
                                                  else 'funds' if str(x)[1] == '2' 
                                                  else 'finis' if str(x)[1] == '3' 
                                                  else 'major_inst')
        # 生成 sid 欄位
        data=data.assign(
                        sid=lambda x: x.apply(
                            lambda row: row['asset_type'] if row['coid'].isdigit() 
                            else ''.join(re.findall(r'[A-Za-z][A-Za-z0-9]*', row['coid'])) , axis=1
                        ))
        # 轉換資料為長格式，並生成 deriv_col
        df=pd.melt(data.drop(['coid','name_c','name_e'],axis=1), 
                   id_vars = ['mdate','sid','invest_type'], 
                   value_vars = ['volume_con_long','volume_amt_long','volume_con_long_pct',
                                 'volume_con_short','volume_amt_short','volume_con_short_pct',
                                 'volume_con_ls_net','volume_amt_ls_net','oi_con_long','oi_amt_long',
                                 'oi_con_long_pct','oi_con_short','oi_amt_short','oi_con_short_pct',
                                 'oi_con_ls_net','oi_amt_ls_net'])\
                   .assign(deriv_col = lambda x: x['variable']+'_'+x['invest_type'])\
                   .set_index(['mdate','sid','deriv_col'])['value'].unstack()
        df.columns.name = None

        return df.reset_index()

    # 最終的請求過程        
    def get_futures_institutions_data(self,root_symbol=None,st=None,et=None):

        if st:
            self.start_date = st
        if et:
            self.end_date = et
        # 1. 請求資料
        data = self.request_data_from_api(self.coid, self.start_date, self.end_date)
        
        if 'mdate' in data.columns:
            data['mdate'] = pd.to_datetime(data['mdate']).dt.tz_localize('UTC')

    
        # 2. 處理資料
        processed_data = self.process_data(data)        
        if root_symbol:            
            if not isinstance(root_symbol, list):
                root_symbol = [root_symbol] 
            return processed_data.loc[processed_data.sid.isin(root_symbol)].reset_index(drop=True).rename(columns={'sid':'root_symbol'})
        return processed_data.reset_index(drop=True).rename(columns={'sid':'root_symbol'})

institution_future_data = RequestFutInstitutions('2025-01-02', pd.Timestamp.now().date().isoformat())



import pandas as pd
import numpy as np
import tejapi

# 中文欄位名稱與英文欄位名稱映射
cn_col_name = ['期貨名稱', '日期', '到期月', '全市場未沖銷部位', '前五大買方未沖銷部位-交易人', '前五大賣方未沖銷部位-交易人',
               '前十大買方未沖銷部位-交易人', '前十大賣方未沖銷部位-交易人', '前五大買方未沖銷部位%-交易人',
               '前五大賣方未沖銷部位%-交易人', '前十大買方未沖銷部位%-交易人', '前十大賣方未沖銷部位%-交易人',
               '前五大買方未沖銷部位-特定法人', '前五大賣方未沖銷部位-特定法人', '前十大買方未沖銷部位-特定法人',
               '前十大賣方未沖銷部位-特定法人', '前五大買方未沖銷部位%-特定法人', '前五大賣方未沖銷部位%-特定法人',
               '前十大買方未沖銷部位%-特定法人', '前十大賣方未沖銷部位%-特定法人']

en_col_name = ['coid', 'mdate', 'expired_month', 'total_mkt_oi', 
               'top_5_trader_long_oi', 'top_5_trader_short_oi', 'top_10_trader_long_oi', 'top_10_trader_short_oi',
               'top_5_trader_long_oi_pct', 'top_5_trader_short_oi_pct', 'top_10_trader_long_oi_pct', 'top_10_trader_short_oi_pct',
               'top_5_institution_long_oi', 'top_5_institution_short_oi', 'top_10_institution_long_oi', 'top_10_institution_short_oi',
               'top_5_institution_long_oi_pct', 'top_5_institution_short_oi_pct', 'top_10_institution_long_oi_pct', 'top_10_institution_short_oi_pct']

col_name_mapping = dict(zip(cn_col_name, en_col_name))

class RequestFutReportableTrader:
    def __init__(self, start_date, end_date):
        self.coid = None
        self.start_date = start_date
        self.end_date = end_date

    @staticmethod
    def request_data_from_api(coid,start_date, end_date):
        """ 向 TEJ API 請求期貨未沖銷部位數據 """
        data = tejapi.fastget('TWN/AFUTRHU', coid=coid, mdate={'gte': start_date, 'lte': end_date}, paginate=True, chinese_column_name=True)
        
        if data.empty:
            return data
        
        data.rename(columns=col_name_mapping, inplace=True)
        return data

    @staticmethod
    def get_taifex_rootsymbol_mapping():
        """ 取得台灣期交所標的代碼與對應的股票代號與名稱 """
        """ 出現 error時需要先執行 !pip install lxml  """
        url = "https://www.taifex.com.tw/cht/2/stockLists"
        tables = pd.read_html(url)
        df = tables[1]

        if df.empty:
            return {}

        df.dropna(subset=['證券代號'], axis=0, inplace=True)
        df['股票期貨、 選擇權 商品代碼'] = df['股票期貨、 選擇權 商品代碼'].fillna('NA')

        rtsymbol_to_tick = {row['股票期貨、 選擇權 商品代碼']: row['證券代號'] for _, row in df.iterrows()}
        rtsymbol_to_name = {row['股票期貨、 選擇權 商品代碼']: row['標的證券 簡稱'] for _, row in df.iterrows()}

        rtsymbol_to_underlying = {
            'BRF': 'Brent_Crude_Oil', 'BTF': 'Taiwan_Biotechnology_Index', 'E4F': 'Taiwan_Sustainability_Index',
            'F1F': 'UK_FTSE100_Index', 'G2F': 'Taiwan_OTC200_Index', 'GDF': 'Gold_US', 'GTF': 'Taiwan_OTC_Index',
            'M1F': 'Taiwan_Midcap100_Index', 'RHF': 'USDCNY', 'RTF': 'Mini_USDCNY', 'SHF': 'Taiwan_Shipping_Index',
            'SOF': 'Taiwan_Semiconductor30_Index', 'SPF': 'US_S&P500_Index', 'SXF': 'US_Philadelphia_Semiconductor_Index',
            'TE': 'Taiwan_Electron_Index', 'TF': 'Taiwan_Finance_Index', 'TGF': 'Gold_TWD', 'TJF': 'Tokyo_Stock_Price_Index',
            'TX': 'Taiwan_Stock_Index_Futures', 'UDF': 'US_Dow_Jones_Index', 'UNF': 'US_Nasdaq100_Index',
            'XAF': 'AUDUSD', 'XBF': 'GBPUSD', 'XEF': 'EURUSD', 'XIF': 'Taiwan_Non_Financial_Electronics_Index',
            'XJF': 'USDJPY'
        }
        rtsymbol_to_tick.update(rtsymbol_to_underlying)
        return rtsymbol_to_tick
   
    @staticmethod
    def split_coid(coid):
        match = re.match(r"([A-Za-z]+)(\d+)", coid)
        if match:
            return match.group(1)#, match.group(2) if match.group(2) else None
        if coid.startswith('Z'):
            return coid.strip('Z').split('_')[0]
        return coid  # 預防意外格式

    @staticmethod
    def generate_sid_from_coid(df):
        """ 根據 coid 生成 symbol 以及對應的 underlying 標的 """
        if df.empty or 'coid' not in df.columns:
            return df

        df['root_symbol'] = df['coid'].apply(lambda x: pd.Series(RequestFutReportableTrader.split_coid(x)))
        
        rtsymbol_to_tick = RequestFutReportableTrader.get_taifex_rootsymbol_mapping()

        #mask = df['coid'].astype(str).str.startswith('Z')
        #df.loc[mask, 'symbol'] = df.loc[mask, 'coid'].str.strip('Z').str[:-2]
        
        df['underlying'] = df['root_symbol'].map(rtsymbol_to_tick)
        #df.loc[mask, 'symbol'] = df.loc[mask, 'root_symbol'].map(rtsymbol_to_tick)

        return df
    
    def get_futures_oi_trader_data(self, root_symbol=None,contact_code=None,st=None,et=None):
        """ 取得期貨未沖銷部位數據 """
        data = self.request_data_from_api(coid=self.coid, start_date=st, end_date=et)
        
        if data.empty:
            return data
    
        data = self.generate_sid_from_coid(data)
    
        if 'mdate' in data.columns:
            data['mdate'] = pd.to_datetime(data['mdate']).dt.tz_localize('UTC')
        
        if not contact_code:
            contact_code = 'N'
        contact_code = '_'+contact_code    
        
        data = data[data.coid.str.endswith(contact_code)]
        
        for col in data.columns[-2:][::-1]:  # 逆序處理，避免 index 變動影響
            data.insert(2, col, data.pop(col))  #   
            
        if root_symbol:
            if not isinstance(root_symbol,list):
                root_symbol=[root_symbol] 
            return data.loc[data.root_symbol.isin(root_symbol)].reset_index(drop=True)#.set_index(['mdate', 'coid'])
    
        return data.reset_index(drop=True)#.set_index(['mdate', 'coid'])

rept_trader_future_data = RequestFutReportableTrader('2025-01-01',pd.Timestamp.now().date().isoformat())