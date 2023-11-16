import numpy as np
import logbook

from zipline.utils.calendar_utils import get_calendar
from zipline.utils.input_validation import (
    expect_types,
    optional,
    expect_strictly_bounded
)
from zipline.utils.events import (
    date_rules,
    time_rules,
    EventRule
)

from zipline.pipeline.data import TQDataSet, USEquityPricing
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.pipeline.loaders.fundamentals import TQuantFundamentalsPipelineLoader

from zipline.finance import slippage, commission

from .basic_algo import BasicAlgo

log = logbook.Logger("PipelineAlgo")


class PipeAlgo(BasicAlgo):
    """
    Base class for Pipeline Algo.
    """
    def __init__(self,
                 bundle_name,
                 start_session,
                 end_session,
                 trading_calendar,
                 capital_base,
                 data_frequency,
                 tradeday,
                 stocklist,
                 benchmark,
                 zero_treasury_returns,
                 initialize,
                 handle_data,
                 before_trading_start,
                 analyze,
                 record_vars,
                 get_record_vars,
                 get_transaction_detail,
                 slippage_model,
                 commission_model,
                 custom_loader,
                 pipeline):

        super().__init__(bundle_name = bundle_name,
                         start_session = start_session,
                         end_session = end_session,
                         trading_calendar = trading_calendar,
                         capital_base = capital_base,
                         data_frequency = data_frequency,
                         tradeday = tradeday,
                         stocklist = stocklist,
                         benchmark = benchmark,
                         zero_treasury_returns = zero_treasury_returns,
                         initialize = initialize,
                         handle_data = handle_data,
                         before_trading_start = before_trading_start,
                         analyze = analyze,
                         record_vars = record_vars,
                         get_record_vars = get_record_vars,
                         get_transaction_detail = get_transaction_detail,
                         slippage_model = slippage_model,
                         commission_model = commission_model,
                         get_pipeline_loader = self.choose_loader)

        self.custom_loader = custom_loader
        self.symbol_mapping_sid = {i.symbol:i.sid for i in self.assets}
        self.TQuantPipelineLoader = TQuantFundamentalsPipelineLoader(zipline_sids_to_real_sids=self.symbol_mapping_sid)
        self.pipeline_loader = EquityPricingLoader.without_fx(self.bundle.equity_daily_bar_reader,
                                                              self.bundle.adjustment_reader,
                                                              )
        self.pipeline = pipeline


    def choose_loader(self, column):
        """
        A function that is given a loadable term and returns a PipelineLoader to use to retrieve
        raw data for that term.

        Parameters
        ----------
        column：zipline.pipeline.data.Column
            An abstract column of data, not yet associated with a dataset.

        See also
        --------
        :func:`zipline.utils.run_algo._run`
        """
        if column in USEquityPricing.columns:  # 不能用EquityPricing.columns
            return self.pipeline_loader
        elif (column in TQDataSet.columns):
            return self.TQuantPipelineLoader

        try:
            return self.custom_loader.get(column)
        except KeyError:
            raise ValueError("No PipelineLoader registered for column %s." % column)


class TargetPercentPipeAlgo(PipeAlgo):
    """
    This algorithm uses the buy and sell lists provided by the pipeline, places
    orders using the `order_target_percent` method, and rebalances periodically.
    """
    @expect_strictly_bounded(max_leverage=(0, None),)
    @expect_types(rebalance_date_rule=optional(EventRule),
                  max_leverage=(float, int),
                  limit_buy_multiplier=optional(float),
                  limit_sell_multiplier=optional(float),
                  allow_short=bool,
                  cancel_datedelta=optional(int),
                  custom_weight=bool,
                  get_transaction_detail=bool,
                  )
    def __init__(self,
                 bundle_name='tquant',
                 start_session=None,
                 end_session=None,
                 trading_calendar=get_calendar('TEJ_XTAI'),
                 capital_base=1e7,
                 data_frequency='daily',
                 tradeday=None,
                 rebalance_date_rule=None,
                 stocklist=None,
                 benchmark='IR0001',
                 zero_treasury_returns=True,
                 slippage_model=slippage.VolumeShareSlippage(volume_limit=0.025, price_impact=0.1),
                 commission_model=commission.Custom_TW_Commission(min_trade_cost=20,
                                                                  discount = 1.0,
                                                                  tax = 0.003),
                 max_leverage=0.8,
                 adjust_amount=False,
                 limit_buy_multiplier=None,
                 limit_sell_multiplier=None,
                 allow_short=False,
                 cancel_datedelta=None,
                 custom_weight=False,
                 custom_loader=None,
                 pipeline=None,
                 analyze=None,
                 record_vars=None,
                 get_record_vars=False,
                 get_transaction_detail=False):

        self.rebalance_date_rule = rebalance_date_rule
        self.max_leverage = max_leverage
        self.adjust_amount = adjust_amount
        self.limit_buy_multiplier = limit_buy_multiplier
        self.limit_sell_multiplier = limit_sell_multiplier
        self.allow_short = allow_short
        self.custom_weight = custom_weight
        self.cancel_datedelta = cancel_datedelta
        self.user_defined_analyze = analyze

        super().__init__(
                         bundle_name=bundle_name,
                         start_session=start_session,
                         end_session=end_session,
                         trading_calendar=trading_calendar,
                         capital_base=capital_base,
                         data_frequency=data_frequency,
                         tradeday=tradeday,
                         stocklist=stocklist,
                         benchmark=benchmark,
                         zero_treasury_returns=zero_treasury_returns,
                         custom_loader=custom_loader,
                         pipeline=pipeline,
                         initialize=self._initialize,
                         handle_data=self._handle_data,
                         before_trading_start=self._before_trading_start,
                         analyze=analyze,
                         record_vars=record_vars,
                         get_record_vars=get_record_vars,
                         get_transaction_detail=get_transaction_detail,
                         slippage_model = slippage_model,
                         commission_model = commission_model
                        )


    def __repr__(self):
        """
        N.B. this does not yet represent a string that can be used
        to instantiate an exact copy of an algorithm.

        However, it is getting close, and provides some value as something
        that can be inspected interactively.
        """
        return """
{class_name}(
    sim_params = {sim_params},
    benchmark = {benchmark},
    zero treasury returns or not（if "True" then treasury returns = 0）= {zero_treasury_returns},
    max_leverage = {max_leverage},
    slippage model used = {slippage_model},
    commission_model = {commission_model},
    adjust amount or not = {adjust_amount},
    limit_buy_multiplier = {limit_buy_multiplier},
    limit_sell_multiplier = {limit_sell_multiplier},
    allow short or not（if "False" then long only）= {allow_short},
    use custom weight or not（if not then "equal weighted"）= {custom_weight},
    cancel_datedelta（if "None" then cancel open orders at next rebalance date）= {cancel_datedelta},
    stocklist = {stocklist},
    tradeday = {tradeday},
    rebalance_date_rule（If the "rebalance_date_rule" parameter is provided, then ignore the "tradeday" parameter."） 
    = {rebalance_date_rule},
    get transaction detail or not = {get_transaction_detail},
    blotter = {blotter},
    recorded_vars = {recorded_vars})
""".strip().format(class_name=self.__class__.__name__,
                           sim_params=self.sim_params,
                           benchmark=self.benchmark,
                           zero_treasury_returns = self.zero_treasury_returns,
                           max_leverage=self.max_leverage,
                           slippage_model=self.slippage_model,
                           commission_model=self.commission_model,
                           adjust_amount=self.adjust_amount,
                           limit_buy_multiplier=self.limit_buy_multiplier,
                           limit_sell_multiplier=self.limit_sell_multiplier,
                           allow_short=self.allow_short,
                           custom_weight=self.custom_weight,
                           cancel_datedelta=self.cancel_datedelta,
                           stocklist=sorted(self.stocklist),
                           tradeday=self.tradeday,
                           rebalance_date_rule=self.rebalance_date_rule,
                           get_transaction_detail=self.get_transaction_detail,
                           blotter=repr(self.blotter),
                           recorded_vars=repr(self.recorded_vars),
                          )


    def get_target_amount(self, asset, count, max_leverage, weight_columns):
        """
        The `get_target_amount` function calculates the number of shares to
        purchase for a given asset. It uses the `order_target_percent` method.

        If custom weights are not provided (`self.custom_weight` is False),
        it calculates the amount based on an even distribution multiplied
        by the maximum leverage.

        If custom weights are provided, it calculates the amount based on the asset's 
        weight from the `output` dataframe and multiplies it by the `maximum leverage`.
        """
        if not self.custom_weight:
            amount = self.calculate_order_target_percent_amount(asset,
                                                                1 / count * max_leverage)
        else:
            amount = self.calculate_order_target_percent_amount(asset,
                                                                self.output.loc[asset, weight_columns] * max_leverage)

        return amount


    def get_limit(self, data, asset, amount):
        """
        This function, get_limit, is designed to calculate the limit price when placing buy or
        sell orders for a given asset. The calculation is based on two predefined multipliers:
        `limit_buy_multiplier` and `limit_sell_multiplier`.

        1. When purchasing (amount > 0), the function multiplies the current price of the asset,
        fetched using `data.current(asset, 'price')`, by `limit_buy_multiplier` to determine the limit price.
        The use of 'price' ensures we have a value even if 'close' is not available, though such a scenario
        should ideally not occur.
        2. When selling (amount < 0), it multiplies the current price of the asset by `limit_sell_multiplier`.
        3. If the calculated limit_price is not a number (NaN), it defaults to None.
        4. Finally, the function returns the calculated `limit_price`.
        """
        if amount > 0:
            limit_price = data.current(asset, 'price') * self.limit_buy_multiplier
        elif amount < 0:
            limit_price = data.current(asset, 'price') * self.limit_sell_multiplier
        else:
            limit_price = None

        if np.isnan(limit_price):
            limit_price = None

        return limit_price


    def _initialize(self, *args, **kwargs):
        """
        This func will be passed to :func:`~basic_algo.initialize` and
        called at the start of the simulation to setup the initial context.
        """

        self.trades = {}

#         避免handle_data裡面沒定義報錯。
        self.divest = []

#         schedule_function
        # We pass exec_cancel.__func__ and rebalance.__func__ to get the unbound method.
        # We will explicitly pass the algorithm to bind it again.

        # exec_cancel和rebalance：
        # 這兩個方法原本是綁定到該物件實例上的（bound method），
        # 這邊利用__func__取得這個方法在原始類別定義中的形式（即unbound的形式）。
        # unbound method是不會自動接收self參數的，之後會在zipline.utils.events.Event重新手動綁定這個方法。
        # （傳入一個context物件作為該方法的第一個參數）

        self.schedule_function(func=self.exec_cancel.__func__,
                               date_rule=date_rules.every_day(),
                               time_rule=time_rules.market_open)

        if self.rebalance_date_rule is not None:
            self.schedule_function(func=self.rebalance.__func__,
                                date_rule=self.rebalance_date_rule,
                                time_rule=time_rules.market_open)
        else:
            self.schedule_function(func=self.rebalance.__func__,
                                date_rule=date_rules.every_day(),
                                time_rule=time_rules.market_open)

        self.schedule_function(func=self._record_vars,
                               date_rule=date_rules.every_day(),
                               time_rule=time_rules.market_close)

#         pipeline
        self.attach_pipeline(self.pipeline(), 'signals')

#         chk long
        if 'longs' not in self.pipeline().columns:
            raise ValueError('No PipelineLoader registered for column "longs", check func："pipeline"')

#         chk allow_short
        if (self.allow_short==True) & ('shorts' not in self.pipeline().columns):
            raise ValueError('No PipelineLoader registered for column "shorts", set "allow_short = False" instead or check func："pipeline"')

#         chk weights
        if ('long_weights' not in self.pipeline().columns) & (self.custom_weight==True):
            raise ValueError('No PipelineLoader registered for column "long_weights", set "custom_weight = False" instead or check func："pipeline"')
        elif (self.allow_short==True) & ('short_weights' not in self.pipeline().columns) & (self.custom_weight==True):
            raise ValueError('No PipelineLoader registered for column "short_weights", set "custom_weight = False" instead or check func："pipeline"')


    def exec_trades(self, data, asset, count, long_short):
        """
        The `exec_trades` function handles the process of placing orders for
        specified assets. Based on whether the trade is a 'long' or 'short',
        it sets the `maximum leverage` and the `weight` columns accordingly.

        Before placing an order, the function checks if the asset is tradable,
        is part of the predefined universe, and there are no existing open orders
        for that asset.

        If the `adjust_amount` flag is set to True, the order amount is adjusted using
        the `get_adj_amount` method. The function also provides the ability to set a
        `limit price` for the order based on the defined multipliers for buying and selling.

        After these checks and adjustments, the function places the order using the order method.
        """
        if long_short=='long':
            max_leverage = self.max_leverage * 1
            weight_columns = 'long_weights'

        elif long_short=='short':
            max_leverage = self.max_leverage * -1  
            weight_columns = 'short_weights'

        if data.can_trade(asset) and \
              asset in self.universe and not \
                self.get_open_orders(asset):

#             calculate order amount
            amount = self.get_target_amount(asset, count, max_leverage, weight_columns)

#             adjust_amount
            if self.adjust_amount==True:
                amount = self.get_adj_amount(data, asset, amount)

#             deal with limit price
            if ((amount > 0) & (self.limit_buy_multiplier is not None)) | \
               ((amount < 0) & (self.limit_sell_multiplier is not None)):
                limit_price = self.get_limit(data, asset, amount)

                self.order(asset,
                           amount,
                           limit_price = limit_price)
            else:
                self.order(asset,
                           amount)


    def _handle_data(self, data):
        """
        This func will be passed to :func:`~basic_algo.before_trading_start` and 
        called on every bar. This is where most logic should be implemented.
        """
        return


    def exec_cancel(self, data):
        """
        The `exec_cancel` function primarily focuses on managing orders marked
        for divestment and checks for custom order cancellation conditions. 

        Initially, it identifies open orders that require divestment and cancels
        them. Immediately after cancellation, it places a new order identical 
        to the canceled one.

        Additionally, it checks for custom order cancellation conditions based on 
        a specified time delta. If an open order has been pending beyond this period 
        from its creation date, it's canceled.
        """

        self.list_longs = []

#         避免divest部位因除權調整而超賣，所以每天都先取消再下一單新的
        open_orders = self.get_open_orders()
        self.divest = [i for i in self.divest if i in open_orders.keys()]
        for asset in self.divest:
            for i in open_orders[asset]:
                self.cancel_order(i)
                try:
                    self.order_target(asset, 0)

                except Exception as e:
                    log.warn('{} {}'.format(self.get_datetime().date(), e))

#         自訂取消訂單時間
        if self.cancel_datedelta:
#             open_orders = self.get_open_orders()
            for asset in open_orders:
                for i in open_orders[asset]:
                    next_trading_date = self.calculate_next_trading_date(self.trading_calendar,
                                                                         i.created.strftime('%Y-%m-%d'),
                                                                         self.cancel_datedelta)

                    if self.get_datetime().strftime('%Y-%m-%d')>=next_trading_date.strftime('%Y-%m-%d'):

                        self.cancel_order(i)

                        str_log = """
                              Cancel_order: current time: {},
                              due to : created>=current time + cancel_datedelta({} days),
                              created: {}, asset: {}, amount: {}, filled: {}' 
                              """.strip().format(self.get_datetime().strftime('%Y-%m-%d'),
                                                 self.cancel_datedelta,
                                                 i.created.strftime('%Y-%m-%d'),
                                                 i.sid,
                                                 i.amount,
                                                 i.filled
                                                )

                        log.info(str_log)


    def rebalance(self, data):
        """
        The rebalance function orchestrates the rebalancing of a trading portfolio
        based on given conditions. It kicks off rebalancing either when a specific
        `date rule` is met or if the current date matches one of the predefined trading
        days.

        First, pending orders are canceled. Buy ("long") ,sell ("short") and divest lists are 
        then constructed from `self.output`. For divestment assets, orders are placed using the 
        `self.order_target` method, while for long and short positions, the `self.exec_trades` 
        method is used.
        """

        # 再平衡
        if  (self.rebalance_date_rule is not None) | \
            (self.get_datetime().strftime('%Y-%m-%d') in self.tradeday):
#             print('Current date(trade)＝' + str(self.get_datetime().date()))

            # 取消未成交的單
            if not self.cancel_datedelta:

                open_orders = self.get_open_orders()
                for asset in open_orders:
                    for i in open_orders[asset]:

                        self.cancel_order(i)

                        log.info('Cancel_order: current time: {} , created: {} , asset: {}, amount: {} , filled: {}'\
                                 .format(self.get_datetime().strftime('%Y-%m-%d'),
                                         i.created.strftime('%Y-%m-%d'),
                                         i.sid,
                                         i.amount,
                                         i.filled
                                        )
                                )

            # 建立買進清單
            self.list_longs = list(set(self.output.loc[self.output['longs']==True].index.to_list()))

            # 建立放空清單
            if self.allow_short:
                self.list_shorts = list(set(self.output.loc[self.output['shorts']==True].index.to_list()))
            else:
                self.list_shorts = []

            # 建立賣出清單
            self.divest = list(set(self.portfolio.positions.keys()) - set(self.list_longs) - set(self.list_shorts))

            # 下單
            N = len(self.list_longs)
            N_S = len(self.list_shorts)


            if (N > 0) | (N_S > 0):

                try:
                    for i in self.divest:
                        self.order_target(i, 0)

                    for i in self.list_longs:
                        self.exec_trades(data, i, N, 'long')

                    for i in self.list_shorts:
                        self.exec_trades(data, i, N_S, 'short') 
                        
                except Exception as e:
                    log.warn('{} {}'.format(self.get_datetime().date(), e))

            else:
                log.info(str(self.get_datetime().date()), 'both long positions and short positions = 0')

        else:
            pass

        # 記錄每日權重
        self.daily_weights = self.portfolio.current_portfolio_weights


    def _before_trading_start(self, data):
        """
        This func will be passed to :func:`~basic_algo.before_trading_start`
        and called once before each trading day (after initialize on the first day)

        Its primary role is to compute the results of the pipeline and then
        store these results in the `self.output` variable. 
        """

        output = self.pipeline_output('signals')
        self.output = output




