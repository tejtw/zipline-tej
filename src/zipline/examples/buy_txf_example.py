# -*- coding: utf-8 -*-
"""
Created on Fri Sep 22 16:15:33 2023

@author: 2022040601
"""

import os


from zipline.api import (
    continuous_future,
    schedule_function,
    date_rules,
    time_rules,
    record,
    order_target_percent,
    set_benchmark,
    set_commission,
    commission,
    set_slippage,
    slippage,
    symbol,
    sid ,
    order,
)


import pandas as pd



def initialize(context):
    
    # set sids
    context.sids = 0
    # Get continuous futures
    context.txf = continuous_future('TX', roll='calendar')    
    

    # Ignore commissions and slippage for now
    set_commission(us_futures=commission.PerTrade(cost=0))
    set_slippage(us_futures=slippage.FixedSlippage(spread=0.0))
    
    context.initial_margin_requirement =  167000
    context.maintenance_margin_requirement = 128000
    
    context.holdings = 0
def handle_data(context, data):

    # startegy : 每天買一口，到期後平倉，重0開始買
    price = data.current(context.txf, 'price')
    start_initial_margin_requirement = context.account.initial_margin_requirement
    settled_cash = context.account.settled_cash
    
    # 契約無法交易後全部平倉，契約轉往下期
    if not data.can_trade(sid(context.sids))   :
        order_target_percent(sid(context.sids), 0)
        context.holdings = 0
        context.sids += 1
        
    # 現金充足
    if (start_initial_margin_requirement + context.initial_margin_requirement) < settled_cash :
        order(sid(context.sids), 1)    
        context.holdings += 1
    context.account_revise = {'initial_margin_requirement' : context.initial_margin_requirement * context.holdings ,
                              'maintenance_margin_requirement':context.maintenance_margin_requirement * context.holdings,
                              'buying_power':1
                              }
    
    maintenance_margin_requirement = context.account.maintenance_margin_requirement
    
    ## 小於維持保證金時，強制平倉
    if context.account.settled_cash < maintenance_margin_requirement :
        order_target_percent(sid(context.sids), 0)
        context.holdings = 0
        context.account_revise = {'initial_margin_requirement' : context.initial_margin_requirement * context.holdings ,
                                  'maintenance_margin_requirement':context.maintenance_margin_requirement * context.holdings,
                                  'buying_power':1
                                  }
    print(context.account)
    record(TXF=price)
    record(maintenance_margin_requirement = maintenance_margin_requirement)
    
def analyze(context, perf):
    import matplotlib.pyplot as plt
    fig = plt.figure(figsize=(16, 12))
    
    # First chart(累計報酬)
    ax = fig.add_subplot(311) 
    ax.set_title('Strategy Results') 
    ax.plot(perf['algorithm_period_return'], linestyle='-', 
                label='algorithm period return', linewidth=3.0)
    ax.legend()
    ax.grid(True)
    
    ax = fig.add_subplot(312)
    ax.set_title("index")
    ax.plot(perf["TXF"])
    ax.grid(True)
    ax = fig.add_subplot(313)
    ax.set_title("cash_maintenance")
    ax.plot(perf["maintenance_margin_requirement"])
    ax.grid(True)
    # ax.legend()
    
from zipline import run_algorithm
from zipline.utils.calendar_utils import get_calendar
start_dt = pd.Timestamp('20220101',tz = 'utc')
end_dt = pd.Timestamp('20230501',tz = 'utc')
bundle_name = 'morning_TXF'
capital_base = 1e7
results = run_algorithm(start=start_dt,            
                        end=end_dt,                          
                        initialize=initialize,
                        handle_data=handle_data,
                        # before_trading_start=before_trading_start,
                        metrics_set = "default",
                        capital_base=capital_base,
                        data_frequency='daily',
                        analyze=analyze,
                        bundle=bundle_name,
                        trading_calendar=get_calendar("TEJ_morning_future"),
                        )