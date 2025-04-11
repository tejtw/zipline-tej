# fut_analysis.py
import pandas as pd
import tejapi
from zipline.TQresearch.futures_smart_money_positions import institution_future_data

def retail_long_short_ratio(root_symbol='TX', st='2013-01-01', et=None):
    if et is None:
        et = pd.Timestamp.now().date().isoformat()

    # total_mkt_oi: 所有身份的 open interest
    df_fut_inst = institution_future_data.get_futures_institutions_data(root_symbol=[root_symbol], st=st, et=et).set_index('mdate')
    
    df_fut_inst['inst_long'] = (
        df_fut_inst['oi_con_long_dealers'] +
        df_fut_inst['oi_con_long_finis'] +
        df_fut_inst['oi_con_long_funds']
    )
    df_fut_inst['inst_short'] = (
        df_fut_inst['oi_con_short_dealers'] +
        df_fut_inst['oi_con_short_finis'] +
        df_fut_inst['oi_con_short_funds']
    )
    
    # 所有合約的總未平倉量（扣掉 Z 系列）
    data = tejapi.fastget(
        'TWN/AFUTR',
        mdate={'gte': st, 'lte': et},
        opts={'columns': ['coid', 'mdate', 'oi_2']},
        paginate=True
    )
    
    df_oi = (
        data[
            (data.coid.str.contains(root_symbol)) & ~(data.coid.str.startswith('Z'))
        ]
        .groupby('mdate')['oi_2']
        .sum()
        .tz_localize('utc')
    )

    # 計算散戶多空比
    df_retail_ratio = (
        ((df_oi - df_fut_inst.inst_long) - (df_oi - df_fut_inst.inst_short)) / df_oi
    )

    return df_retail_ratio
