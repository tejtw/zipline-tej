import numpy as np
import pandas as pd
import logbook

from zipline.algorithm import TradingAlgorithm

from zipline._protocol import handle_non_market_minutes

from zipline.sources.TEJ_Api_Data import get_Treasury_Return

from zipline.data import bundles
from zipline.data.data_portal import DataPortal

from zipline.utils.run_algo import  (
    get_transaction_detail,
    get_record_vars,
    TreasurySpec,
    BenchmarkSpec
)
from zipline.utils.api_support import ZiplineAPI
from zipline.utils.input_validation import (
    preprocess,
    expect_types,
    optional,
)
from zipline.utils.compat import ExitStack

from zipline.finance.slippage import SlippageModel
from zipline.finance.commission import CommissionModel

from zipline.finance.trading import SimulationParameters

from .utils import check_tradeday

log = logbook.Logger("BasicAlgo")


class BasicAlgo(TradingAlgorithm):
    """
    Base class for Algo.
    """
    @preprocess(tradeday=check_tradeday)
    @expect_types(stocklist=optional(list),
                  zero_treasury_returns=bool,
                  get_record_vars=bool,
                  get_transaction_detail=bool,
                  slippage_model=SlippageModel,
                  commission_model=CommissionModel,
                  #get_pipeline_loader=,
                  benchmark=optional(str),
                  )
    def __init__(self,
                 bundle_name,
                 start_session,
                 end_session,
                 trading_calendar,
                 capital_base,
                 data_frequency,
                 tradeday,
                 stocklist,
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
                 get_pipeline_loader,
                 benchmark = 'IR0001'):

        self.bundle_name = bundle_name
        self.bundle = bundles.load(bundle_name)
        asset_finder = self.bundle.asset_finder
        sids = self.bundle.asset_finder.equities_sids
        assets = self.bundle.asset_finder.retrieve_all(sids)
        self.assets = assets

        if start_session is None:
            start_session = self.default_pipeline_domain(trading_calendar).\
                next_open(min([i.start_date for i in self.assets]))
        if end_session is None:
            end_session = self.default_pipeline_domain(trading_calendar).\
                prev_open(max([i.end_date for i in self.assets]))

        data_portal = DataPortal(asset_finder=asset_finder,
                                 trading_calendar=trading_calendar,
                                 first_trading_day=self.bundle.equity_daily_bar_reader.first_trading_day,
                                 equity_daily_reader=self.bundle.equity_daily_bar_reader,
                                 adjustment_reader=self.bundle.adjustment_reader
                                 )

        sim_params = SimulationParameters(start_session=start_session,
                                          end_session=end_session,
                                          trading_calendar=trading_calendar,
                                          capital_base=capital_base,
                                          data_frequency=data_frequency,
                                          )

        super().__init__(data_portal = data_portal,
                         sim_params = sim_params,
                         initialize = initialize,
                         handle_data = handle_data,
                         before_trading_start = before_trading_start,
                         analyze = analyze,
                         get_pipeline_loader=get_pipeline_loader
                        )

        if tradeday:
            if len([i for i in tradeday if self.trading_calendar.is_session(i)!=False])==0:
                raise ValueError("At least one of valid tradeday should be in `tradeday`")
            else:
                self.tradeday = tradeday
        else:
            self.tradeday = sim_params.sessions

        if stocklist:
            self.stocklist = stocklist
        else:
            self.stocklist = list(set([i.symbol for i in self.assets]) - set(benchmark))

        # class:`zipline.algorithm.TradingAlgorithm`
        # property:`benchmark_sid`and `benchmark_returns`
        try:
            benchmark_sid = self.bundle.asset_finder.lookup_symbol(benchmark, as_of_date=None).sid
            self.benchmark_sid = benchmark_sid # same as set_benchmark
            self.benchmark = benchmark
        except:
            self.benchmark_returns = BenchmarkSpec.from_returns(None).\
                _zero_benchmark_returns(start_date = self.sim_params.start_session,
                                        end_date = self.sim_params.end_session)
            self.benchmark = None

        # class:`zipline.algorithm.TradingAlgorithm`
        # property:`treasury_returns`
        self.zero_treasury_returns = zero_treasury_returns
        if not self.zero_treasury_returns:
            self.treasury_returns = get_Treasury_Return(start = self.sim_params.start_session,
                                                        end = self.sim_params.end_session,
                                                        rate_type = 'Time_Deposit_Rate',
                                                        term = '1y',
                                                        symbol = '5844'
                                                    )
        else:

            self.treasury_returns = TreasurySpec.from_returns(None).\
                _zero_treasury_returns(start_date = self.sim_params.start_session,
                                       end_date = self.sim_params.end_session)

        self._record_vars = record_vars
        self.get_record_vars = get_record_vars
        self.get_transaction_detail = get_transaction_detail
        self.slippage_model = slippage_model
        self.commission_model = commission_model


    @staticmethod
    def calculate_next_trading_date(calendar, start_date, days_to_add):
        """
        The `calculate_next_trading_date` function is designed to determine
        the trading date that falls a specific number of days after a given
        starting date.

        Parameters
        ----------
        calendar：TradingCalendar

        start_date：str

        days_to_add：int

        Returns:
        ----------
        pd.Timestamp: The next trading date.
        """
        schedule = calendar.sessions_in_range(start_date, pd.Timestamp.max)
        start_date_timestamp = pd.Timestamp(start_date, tz="UTC")

#         檢查是否有足夠的交易日可供選擇
        if len(schedule) <= days_to_add:
            next_trading_date = pd.NaT
#             log.info("Not enough trading days available for the given days_to_add {} at {}".format(days_to_add,
#                                                                                                    start_date.strftime('%Y-%m-%d')))
        else:
            next_trading_date = schedule[schedule > start_date_timestamp][days_to_add - 1]

        return next_trading_date


    def get_adj_amount(self, data, asset, amount):
        """
        The function adjusts the number of shares placed in an order based on the price
        difference between the time of placing the order and the time of execution,
        reducing overbought and oversold issues.
        """
        # 預計成交時價格
        transaction_price = self.data_portal.get_spot_value(assets = asset,
                                                            field = 'close',
                                                            dt = self.calculate_next_trading_date(self.trading_calendar,
                                                                                                  self.get_datetime().strftime('%Y-%m-%d'),
                                                                                                  1),
                                                            data_frequency = self.data_frequency)

        if np.isnan(transaction_price)==False:
            # 前一筆價格
            order_price = data.current(asset, "price")

            # 價格變化
            chg = transaction_price / order_price

            # 調整下單數，並捨去
            # （Round a to the nearest integer if that integer is 
            # within an epsilon of 1e-4）
            adj_amount = self.round_order(amount / chg)

        else:
            adj_amount = amount

        return adj_amount


    def record_vars(self, data):
        """
        These values will appear in the performance packets and the performance
        dataframe passed to ``analyze`` and returned from :func:
        `~zipline.TradingAlgorithm.run()`.
        """
        if self._record_vars is None:
            return

        self._record_vars(self, data)


    def initialize(self, *args, **kwargs):
        """
        Call self._initialize with `self` made available to Zipline API
        functions.

        See also
        --------
        :class:`zipline.algorithm.TradingAlgorithm`
        :func:`initialize`
        """
        with ZiplineAPI(self):

            # 交易成本
            if self.slippage_model:
                self.set_slippage(self.slippage_model)

            self.set_commission(self.commission_model)

            self.universe = [self.symbol(i) for i in self.stocklist]

            """
            這邊理論上不可用self._initialize(self, *args, **kwargs)，因為這是在底層類別下定義的
            （這代表_initialize是bound method，不需要手動傳遞self參數），而非使用者自定義。
            實際上若使用self._initialize(self, *args, **kwargs)也不會出錯，是因為self會被傳到
            *args中。
            """
            self._initialize(*args, **kwargs)


    def before_trading_start(self, data):
        """
        See also
        --------
        :class:`zipline.algorithm.TradingAlgorithm`
        :func:`before_trading_start`
        """
        self.compute_eager_pipelines()

        if self._before_trading_start is None:
            return

        self._in_before_trading_start = True

        with handle_non_market_minutes(
            data
        ) if self.data_frequency == "minute" else ExitStack():

            # 這邊不可用self._before_trading_start(self, data)，因為這是在底層類別下定義的
            # （這代表_before_trading_start是bound method，不需要手動傳遞self參數），而非使用者自定義。
            self._before_trading_start(data)

        self._in_before_trading_start = False


    def handle_data(self, data):
        """
        See also
        --------
        :class:`zipline.algorithm.TradingAlgorithm`
        :func:`handle_data`
        """
        if self._handle_data:

            # 這邊不可用self._handle_data(self, data)，因為這是在底層類別下定義的
            # （_handle_data是bound method，不需要手動傳遞self參數），而非使用者自定義。
            self._handle_data(data)


    def analyze(self, perf):
        """
        See also
        --------
        :class:`zipline.algorithm.TradingAlgorithm`
        :func:`analyze`
        """

        # Get the transaction details if needed
        if self.get_transaction_detail:
            try:
                positions, transactions, orders = get_transaction_detail(perf)
                self.positions, self.transactions, self.orders = positions, transactions, orders
            except Exception as e:
                log.warn(f'Fail to generate transaction detail due to {e}.')

        # Get record vars if needed
        if self.get_record_vars:
            try:
                # self._recorded_vars:class:`zipline.algorithm.TradingAlgorithm`
                # If self._recorded_vars is None，then `algo.dict_record_vars`={}
                self.dict_record_vars = get_record_vars(perf, list(self._recorded_vars.keys()))
            except Exception as e:
                log.warn(f'Fail to generate record vars due to {e}.')

            if not self._recorded_vars:
                log.warn(f'Since the `record_vars` parameter was not set,'
                         '`algo.dict_record_vars` will return a empty dict.')

        # Call the user's analyze function
        if self._analyze is None:
            return

        with ZiplineAPI(self):

            # user defined analyze func：
            # 這邊不可用self._analyze(perf)，因為這是使用者自定義的。（_analyze是unbound method
            # ，需要手動傳遞self參數）
            self._analyze(self, perf)