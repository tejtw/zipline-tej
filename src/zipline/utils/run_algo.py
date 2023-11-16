import click
import os
import sys
import warnings
import re                  #20230206 (by MRC)


try:
    from pygments import highlight
    from pygments.lexers import PythonLexer
    from pygments.formatters import TerminalFormatter

    PYGMENTS = True
except ImportError:
    PYGMENTS = False
import logbook
import pandas as pd
from toolz import concatv
from zipline.utils.calendar_utils import get_calendar

from zipline.data import bundles
from zipline.data.benchmarks import get_benchmark_returns_from_file
from zipline.data.treasury import get_treasury_returns_from_file   #20230210 (by MRC)
from zipline.data.data_portal import DataPortal
from zipline.finance import metrics
from zipline.finance.trading import SimulationParameters
from zipline.pipeline.data import USEquityPricing, TQDataSet
from zipline.pipeline.loaders import USEquityPricingLoader, TQuantFundamentalsPipelineLoader

import zipline.utils.paths as pth
from zipline.utils.input_validation import expect_types #20230809 (by MRC)

from zipline.extensions import load
from zipline.errors import SymbolNotFound
from zipline.errors import (IllegalDTypeException,IllegalColumnException) #20230206 (by MRC)

from zipline.algorithm import TradingAlgorithm, NoBenchmark
from zipline.algorithm import NoTreasury      #20230210 (by MRC)
from zipline.finance.blotter import Blotter

import zipline                                #20230325 (by MRC)

log = logbook.Logger(__name__)


class _RunAlgoError(click.ClickException, ValueError):
    """Signal an error that should have a different message if invoked from
    the cli.

    Parameters
    ----------
    pyfunc_msg : str
        The message that will be shown when called as a python function.
    cmdline_msg : str, optional
        The message that will be shown on the command line. If not provided,
        this will be the same as ``pyfunc_msg`
    """

    exit_code = 1

    def __init__(self, pyfunc_msg, cmdline_msg=None):
        if cmdline_msg is None:
            cmdline_msg = pyfunc_msg

        super(_RunAlgoError, self).__init__(cmdline_msg)
        self.pyfunc_msg = pyfunc_msg

    def __str__(self):
        return self.pyfunc_msg


# TODO: simplify
# flake8: noqa: C901
def _run(
    handle_data,
    initialize,
    before_trading_start,
    analyze,
    algofile,
    algotext,
    defines,
    data_frequency,
    capital_base,
    bundle,
    bundle_timestamp,
    start,
    end,
    output,
    trading_calendar,
    print_algo,
    metrics_set,
    local_namespace,
    environ,
    blotter,
    custom_loader,
    benchmark_spec,
    treasury_spec         #20230209 (by MRC) for treasury return column in perf
):
    """Run a backtest for the given algorithm.

    This is shared between the cli and :func:`zipline.run_algo`.
    """

    bundle_data = bundles.load(
        bundle,
        environ,
        bundle_timestamp,
    )

    if trading_calendar is None:
        #trading_calendar = get_calendar("XNYS")        #20230208 (by MRC) modify default value
        trading_calendar = get_calendar("TEJ")          #20230516 (by MRC) modify default value

    # date parameter validation
    if trading_calendar.session_distance(start, end) < 1:
        raise _RunAlgoError(
            "There are no trading days between %s and %s"
            % (
                start.date(),
                end.date(),
            ),
        )

    benchmark_sid, benchmark_returns = benchmark_spec.resolve(
        asset_finder=bundle_data.asset_finder,
        start_date=start,
        end_date=end,
    )
    
    #--------------------------------------------------------------------    
    #20230209 (by MRC) for treasury return column in perf  #start#
    #Note:other source of treasury returns(not available now)
    
    treasury_sid, treasury_returns = treasury_spec.resolve(
        asset_finder=bundle_data.asset_finder,
        start_date=start,
        end_date=end,
    )
    
    #20230209 (by MRC) for treasury return column in perf  #end#
    #--------------------------------------------------------------------
    
    if algotext is not None:
        if local_namespace:
            ip = get_ipython()  # noqa
            namespace = ip.user_ns
        else:
            namespace = {}

        for assign in defines:
            try:
                name, value = assign.split("=", 2)
            except ValueError:
                raise ValueError(
                    "invalid define %r, should be of the form name=value" % assign,
                )
            try:
                # evaluate in the same namespace so names may refer to
                # eachother
                namespace[name] = eval(value, namespace)
            except Exception as e:
                raise ValueError(
                    "failed to execute definition for name %r: %s" % (name, e),
                )
    elif defines:
        raise _RunAlgoError(
            "cannot pass define without `algotext`",
            "cannot pass '-D' / '--define' without '-t' / '--algotext'",
        )
    else:
        namespace = {}
        if algofile is not None:
            algotext = algofile.read()

    if print_algo:
        if PYGMENTS:
            highlight(
                algotext,
                PythonLexer(),
                TerminalFormatter(),
                outfile=sys.stdout,
            )
        else:
            click.echo(algotext)

    first_trading_day = bundle_data.equity_minute_bar_reader.first_trading_day

    data = DataPortal(
        bundle_data.asset_finder,
        trading_calendar=trading_calendar,
        first_trading_day=first_trading_day,
        equity_minute_reader=bundle_data.equity_minute_bar_reader,
        equity_daily_reader=bundle_data.equity_daily_bar_reader,
        adjustment_reader=bundle_data.adjustment_reader,
        future_minute_reader=bundle_data.equity_minute_bar_reader,
        future_daily_reader=bundle_data.equity_daily_bar_reader,
    )

    pipeline_loader = USEquityPricingLoader.without_fx(
        bundle_data.equity_daily_bar_reader,
        bundle_data.adjustment_reader,
    )
    
    sids = bundle_data.asset_finder.equities_sids
    assets = bundle_data.asset_finder.retrieve_all(sids)
    symbol_mapping_sid = {i.symbol:i.sid for i in assets}

    TQuantPipelineLoader = TQuantFundamentalsPipelineLoader(zipline_sids_to_real_sids=symbol_mapping_sid)

    def choose_loader(column):
        if column in USEquityPricing.columns:
            return pipeline_loader
        
        if column in TQDataSet.columns:
            return TQuantPipelineLoader
        
        try:
            return custom_loader.get(column)
        except KeyError:
            raise ValueError("No PipelineLoader registered for column %s." % column)

    if isinstance(metrics_set, str):
        try:
            metrics_set = metrics.load(metrics_set)
        except ValueError as e:
            raise _RunAlgoError(str(e))

    if isinstance(blotter, str):
        try:
            blotter = load(Blotter, blotter)
        except ValueError as e:
            raise _RunAlgoError(str(e))

    try:
        perf = TradingAlgorithm(
            namespace=namespace,
            data_portal=data,
            get_pipeline_loader=choose_loader,
            trading_calendar=trading_calendar,
            sim_params=SimulationParameters(
                start_session=start,
                end_session=end,
                trading_calendar=trading_calendar,
                capital_base=capital_base,
                data_frequency=data_frequency,
            ),
            metrics_set=metrics_set,
            blotter=blotter,
            benchmark_returns=benchmark_returns,
            benchmark_sid=benchmark_sid,
            treasury_returns=treasury_returns,    #20230209  (by MRC) for treasury return column in perf
            treasury_sid=treasury_sid,            #20230209  (by MRC) for treasury return column in perf
            **{
                "initialize": initialize,
                "handle_data": handle_data,
                "before_trading_start": before_trading_start,
                "analyze": analyze,
            }
            if algotext is None
            else {
                "algo_filename": getattr(algofile, "name", "<algorithm>"),
                "script": algotext,
            },
        ).run()
        
        
    except NoBenchmark:
        raise _RunAlgoError(
            (
                "No ``benchmark_spec`` was provided, and"
                " ``zipline.api.set_benchmark`` was not called in"
                " ``initialize``."
            ),
            (
                "Neither '--benchmark-symbol' nor '--benchmark-sid' was"
                " provided, and ``zipline.api.set_benchmark`` was not called"
                " in ``initialize``. Did you mean to pass '--no-benchmark'?"
            ),
        )
        
    #20230210-------------------------------------------------------------------------
    #Note:generally useless
    except NoTreasury:
        raise _RunAlgoError(
            (
                "No ``treasury_spec`` was provided."
            ),
            (
                "Neither '--treasury-symbol' nor '--treasury-sid' was"
                " provided. Did you mean to pass '--no-benchmark'?"
            ),
        )
    #20230210-------------------------------------------------------------------------        
        
    if output == "-":
        click.echo(str(perf))
    elif output != os.devnull:  # make the zipline magic not write any data
        perf.to_pickle(output)

    return perf


# All of the loaded extensions. We don't want to load an extension twice.
_loaded_extensions = set()


def load_extensions(default, extensions, strict, environ, reload=False):
    """Load all of the given extensions. This should be called by run_algo
    or the cli.

    Parameters
    ----------
    default : bool
        Load the default exension (~/.zipline/extension.py)?
    extension : iterable[str]
        The paths to the extensions to load. If the path ends in ``.py`` it is
        treated as a script and executed. If it does not end in ``.py`` it is
        treated as a module to be imported.
    strict : bool
        Should failure to load an extension raise. If this is false it will
        still warn.
    environ : mapping
        The environment to use to find the default extension path.
    reload : bool, optional
        Reload any extensions that have already been loaded.
    """
    if default:
        default_extension_path = pth.default_extension(environ=environ)
        pth.ensure_file(default_extension_path)
        # put the default extension first so other extensions can depend on
        # the order they are loaded
        extensions = concatv([default_extension_path], extensions)

    for ext in extensions:
        if ext in _loaded_extensions and not reload:
            continue
        try:
            # load all of the zipline extensionss
            if ext.endswith(".py"):
            
                # with open(ext) as f:                   # 20221222 (by LDW)
                with open(ext , encoding="utf-8") as f:  # 20221222 (by LDW)
                    ns = {}
                    exec(compile(f.read(), ext, "exec"), ns, ns)
            else:
                __import__(ext)
        except Exception as e:
            if strict:
                # if `strict` we should raise the actual exception and fail
                raise
            # without `strict` we should just log the failure
            warnings.warn("Failed to load extension: %r\n%s" % (ext, e), stacklevel=2)
        else:
            _loaded_extensions.add(ext)


def run_algorithm(
    start,
    end,
    initialize,
    capital_base,
    handle_data=None,
    before_trading_start=None,
    analyze=None,
    data_frequency="daily",
    bundle="tquant",             #20230907 (by MRC) 由"quantopian-quandl"->"tquant"
    bundle_timestamp=None,
    trading_calendar=None,
    metrics_set="default",
    benchmark_returns=None,
    treasury_returns=None,       #20230209 (by MRC) for treasury return column in perf
    default_extension=True,
    extensions=(),
    strict_extensions=True,
    environ=os.environ,
    custom_loader=None,
    blotter="default",
):
    """
    Run a trading algorithm.

    Parameters
    ----------
    start : datetime
        The start date of the backtest.
    end : datetime
        The end date of the backtest..
    initialize : callable[context -> None]
        The initialize function to use for the algorithm. This is called once
        at the very begining of the backtest and should be used to set up
        any state needed by the algorithm.
    capital_base : float
        The starting capital for the backtest.
    handle_data : callable[(context, BarData) -> None], optional
        The handle_data function to use for the algorithm. This is called
        every minute when ``data_frequency == 'minute'`` or every day
        when ``data_frequency == 'daily'``.
    before_trading_start : callable[(context, BarData) -> None], optional
        The before_trading_start function for the algorithm. This is called
        once before each trading day (after initialize on the first day).
    analyze : callable[(context, pd.DataFrame) -> None], optional
        The analyze function to use for the algorithm. This function is called
        once at the end of the backtest and is passed the context and the
        performance data.
    data_frequency : {'daily', 'minute'}, optional
        The data frequency to run the algorithm at.
    bundle : str, optional
        The name of the data bundle to use to load the data to run the backtest
        with. This defaults to 'quantopian-quandl'.
    bundle_timestamp : datetime, optional
        The datetime to lookup the bundle data for. This defaults to the
        current time.
    trading_calendar : TradingCalendar, optional
        The trading calendar to use for your backtest.
    metrics_set : iterable[Metric] or str, optional
        The set of metrics to compute in the simulation. If a string is passed,
        resolve the set with :func:`zipline.finance.metrics.load`.
    benchmark_returns : pd.Series, optional
        Series of returns to use as the benchmark.
    treasury_returns : pd.Series, optional
        Series of returns to use as the treasury.
    default_extension : bool, optional
        Should the default zipline extension be loaded. This is found at
        ``$ZIPLINE_ROOT/extension.py``
    extensions : iterable[str], optional
        The names of any other extensions to load. Each element may either be
        a dotted module path like ``a.b.c`` or a path to a python file ending
        in ``.py`` like ``a/b/c.py``.
    strict_extensions : bool, optional
        Should the run fail if any extensions fail to load. If this is false,
        a warning will be raised instead.
    environ : mapping[str -> str], optional
        The os environment to use. Many extensions use this to get parameters.
        This defaults to ``os.environ``.
    blotter : str or zipline.finance.blotter.Blotter, optional
        Blotter to use with this algorithm. If passed as a string, we look for
        a blotter construction function registered with
        ``zipline.extensions.register`` and call it with no parameters.
        Default is a :class:`zipline.finance.blotter.SimulationBlotter` that
        never cancels orders.

    Returns
    -------
    perf : pd.DataFrame
        The daily performance of the algorithm.

    See Also
    --------
    zipline.data.bundles.bundles : The available data bundles.
    """
    load_extensions(default_extension, extensions, strict_extensions, environ)

    benchmark_spec = BenchmarkSpec.from_returns(benchmark_returns)    
    treasury_spec = TreasurySpec.from_returns(treasury_returns)    #20230210(by MRC) for treasury return column in perf
    
    return _run(
        handle_data=handle_data,
        initialize=initialize,
        before_trading_start=before_trading_start,
        analyze=analyze,
        algofile=None,
        algotext=None,
        defines=(),
        data_frequency=data_frequency,
        capital_base=capital_base,
        bundle=bundle,
        bundle_timestamp=bundle_timestamp,
        start=start,
        end=end,
        output=os.devnull,
        trading_calendar=trading_calendar,
        print_algo=False,
        metrics_set=metrics_set,
        local_namespace=False,
        environ=environ,
        blotter=blotter,
        custom_loader=custom_loader,
        benchmark_spec=benchmark_spec,
        treasury_spec=treasury_spec           #20230209 (by MRC) for treasury return column in perf
    )

def _transform_perf_table_for_record(perf, column):
    '''
    20230206 (by MRC)

    transform price, factor and group data returned by zipline.TradingAlgorithm.run().
    
    Parameters
    ----------
    perf : pd.DataFrame
        DataFrame returned by zipline.TradingAlgorithm.run()

    column
    -------
    column : str
        The name of the column.
    
    reference
    ---------
    1.ML4T 04_alpha_factor_research/06_performance_eval_alphalens.ipynb
    2.pyfolio.utils.extract_rets_pos_txn_from_zipline(backtest)

    TODO
    ---------
    這寫法需優化
    '''
    df1 = []
    df_YN = 'N'
    # 以下處理需假設perf[column]中所有items的type一致
    for d, df in perf[column].dropna().items():
        if isinstance(df, pd.Series):
            df1.append(df.to_frame(d))
        elif isinstance(df, list):
            df1.append(pd.DataFrame(df, columns=[d]))
        elif isinstance(df, int):
            df1.append(pd.DataFrame([df], columns=[d]))
        elif isinstance(df, float):
            df1.append(pd.DataFrame([df], columns=[d]))
        elif isinstance(df, set):
            df1.append(pd.DataFrame(list(df), columns=[d]))
        elif isinstance(df, pd.DataFrame):   # 針對data.history()結果
            df = df.reset_index()
            df['date'] = d
            df = df.set_index('date')
            df1.append(df)
            df_YN = 'Y'
        else:
            raise IllegalDTypeException(parameter =f'"results[{column}].items()"',
                                        dtype ='"pd.Series or list or int or float or set"')

    # Concatenate the individual DataFrames
    if df_YN=='N':
        data = pd.concat(df1, axis=1).T
    else:
        data = pd.DataFrame()
        for i in df1:
            data = i.append(data)

    # transform sid to symbol    
    for i in data.columns:
        if type(i)==zipline.assets._assets.Equity:
          
            data.rename({i:i.symbol}, axis=1,inplace=True) 
        else:
            pass

    # Convert times to midnight
    # data.index=data.index.tz_convert(tz='UTC')
    data.index = data.index.normalize()

    return data, df_YN

@expect_types(perf=pd.DataFrame,
              columns=list)
def get_record_vars(perf,
                    columns):
    '''
    20230325 (by MRC)

    Export columns recorded by zipline.api.record() 
    and returned by zipline.TradingAlgorithm.run().

    Parameters
    ----------
    perf : pd.DataFrame
        DataFrame returned by zipline.TradingAlgorithm.run()

    column
    -------
    columns : list
        A list of the column names.
    TODO
    ---------
    這寫法需優化
    ''' 
    dict_data = {}

    for i in columns:
        if i in perf.columns:

            data, df_YN = _transform_perf_table_for_record(perf,i)
            
            if df_YN=='N':
                dict_data[i] = (data.stack()
                                    .reset_index()
                                    .rename(columns={'level_0':'date',
                                                     'level_1':'symbol',
                                                     0:i})
                                )
            else:  # 針對data.history()結果
                column_names = data.reset_index().columns
                dict_data[i] = (data.reset_index()
                                    .set_index([column_names[0],column_names[1]])
                                    .stack()
                                    .reset_index()
                                    .rename(columns={'level_2':'symbol',
                                                      0:i})
                                    .sort_values(by=[column_names[0],column_names[1]])
                                )

            #dict_data[i].index.names = ['date',i]
            if type(dict_data[i].values.all())==zipline.assets._assets.Equity:
                dict_data[i] = dict_data[i].apply(lambda x:x.symbol)
            else:
                pass
        else:
            log.info(i + ' does not exist')

    return dict_data

def transform_perf_table(perf, column):
    '''
    20230206 (by MRC)
    transform positions/transactions/orders data returned by zipline.TradingAlgorithm.run().
    
    Parameters
    ----------
    perf : pd.DataFrame
        The daily performance of the algorithm.
    column : str
        The name of the positions/transactions/orders column.
        
    reference
    ---------
    1.ML4T 04_alpha_factor_research/06_performance_eval_alphalens.ipynb
    2.pyfolio.utils.extract_rets_pos_txn_from_zipline(backtest)    
    ''' 

    if column not in perf.columns:
        raise IllegalColumnException(column = column)   

    raw = []
    for dt, row in perf[column].iteritems():
        df = pd.DataFrame(row)
        df.index = [dt] * len(df)
        raw.append(df)

    out = pd.concat(raw)

    if len(out)==0:
        print('The backtest does not have any ', str(column))
    else:

        out.rename(columns={'sid':'asset'},inplace=True)
        out.insert(0, 'sid', out['asset'].apply(lambda x: x.sid))
        out.insert(1, 'symbol', out['asset'].apply(lambda x: x.symbol))

    return out 

@expect_types(perf=pd.DataFrame)
def get_transaction_detail(perf):
    '''
    20230206 (by MRC)

    get positions/transactions/orders data returned by zipline.TradingAlgorithm.run().
    
    Parameters
    ----------
    perf : pd.DataFrame
        The daily performance of the algorithm.
    
    Note
    ----------
    similar to pyfolio.utils.extract_rets_pos_txn_from_zipline(backtest)
    '''
    positions = transform_perf_table(perf,'positions')
    transactions = transform_perf_table(perf,'transactions')
    orders = transform_perf_table(perf,'orders')

    return positions,transactions,orders

class BenchmarkSpec(object):
    """
    Helper for different ways we can get benchmark data(treasury data) for the Zipline CLI and
    zipline.utils.run_algo.run_algorithm.

    Parameters
    ----------
    benchmark_returns : pd.Series, optional
        Series of returns to use as the benchmark.
    benchmark_file : str or file
        File containing a csv with `date` and `return` columns, to be read as
        the benchmark.
    benchmark_sid : int, optional
        Sid of the asset to use as a benchmark.
    benchmark_symbol : str, optional
        Symbol of the asset to use as a benchmark. Symbol will be looked up as
        of the end date of the backtest.
    no_benchmark : bool
        Flag indicating that no benchmark is configured. Benchmark-dependent
        metrics will be calculated using a dummy benchmark of all-zero returns.
    """

    def __init__(
        self,
        benchmark_returns,
        benchmark_file,
        benchmark_sid,
        benchmark_symbol,
        no_benchmark,
    ):

        self.benchmark_returns = benchmark_returns
        self.benchmark_file = benchmark_file
        self.benchmark_sid = benchmark_sid
        self.benchmark_symbol = benchmark_symbol
        self.no_benchmark = no_benchmark

    @classmethod
    def from_cli_params(
        cls, benchmark_sid, benchmark_symbol, benchmark_file, no_benchmark
    ):

        return cls(
            benchmark_returns=None,
            benchmark_sid=benchmark_sid,
            benchmark_symbol=benchmark_symbol,
            benchmark_file=benchmark_file,
            no_benchmark=no_benchmark,
        )

    @classmethod
    def from_returns(cls, benchmark_returns):
        return cls(
            benchmark_returns=benchmark_returns,
            benchmark_file=None,
            benchmark_sid=None,
            benchmark_symbol=None,
            no_benchmark=benchmark_returns is None,
        )
    
    #inputs priority(benchmark_returns>benchmark_file>benchmark_sid>benchmark_symbol>no_benchmark)
    def resolve(self, asset_finder, start_date, end_date):
        """
        Resolve inputs into values to be passed to TradingAlgorithm.

        Returns a pair of ``(benchmark_sid, benchmark_returns)`` with at most
        one non-None value. Both values may be None if no benchmark source has
        been configured.

        Parameters
        ----------
        asset_finder : zipline.assets.AssetFinder
            Asset finder for the algorithm to be run.
        start_date : pd.Timestamp
            Start date of the algorithm to be run.
        end_date : pd.Timestamp
            End date of the algorithm to be run.

        Returns
        -------
        benchmark_sid : int
            Sid to use as benchmark.
        benchmark_returns : pd.Series
            Series of returns to use as benchmark.
        """
        if self.benchmark_returns is not None:
            benchmark_sid = None
            benchmark_returns = self.benchmark_returns
        elif self.benchmark_file is not None:
            benchmark_sid = None
            benchmark_returns = get_benchmark_returns_from_file(
                self.benchmark_file,
            )
        elif self.benchmark_sid is not None:
            benchmark_sid = self.benchmark_sid
            benchmark_returns = None
        elif self.benchmark_symbol is not None:
            try:
                asset = asset_finder.lookup_symbol(
                    self.benchmark_symbol,
                    as_of_date=end_date,
                )
                benchmark_sid = asset.sid
                benchmark_returns = None
            except SymbolNotFound:
                raise _RunAlgoError(
                    "Symbol %r as a benchmark not found in this bundle."
                    % self.benchmark_symbol
                )
        elif self.no_benchmark:
            benchmark_sid = None
            benchmark_returns = self._zero_benchmark_returns(
                start_date=start_date,
                end_date=end_date,
            )
        else:
            log.warn(
                "No benchmark configured. " "Assuming algorithm calls set_benchmark."
            )
            log.warn(
                "Pass --benchmark-sid, --benchmark-symbol, or"
                " --benchmark-file to set a source of benchmark returns."
            )
            log.warn(
                "Pass --no-benchmark to use a dummy benchmark " "of zero returns.",
            )
            benchmark_sid = None
            benchmark_returns = None

        return benchmark_sid, benchmark_returns

    @staticmethod
    def _zero_benchmark_returns(start_date, end_date):
        return pd.Series(
            index=pd.date_range(start_date, end_date, tz="utc"),
            data=0.0,
        )


#--------------------------------------------------------------------
#20230210 (by MRC) for treasury return column in perf  #end#
class TreasurySpec(object):
    """    
    Helper for different ways we can get treasury data for the Zipline CLI and
    zipline.utils.run_algo.run_algorithm.

    Parameters
    ----------
    treasury_returns : pd.Series, optional
        Series of returns to use as the treasury.
    treasury_file : str or file
        File containing a csv with `date` and `return` columns, to be read as
        the treasury.
    treasury_sid(not available now) : int, optional
        Sid of the asset to use as a treasury.
    treasury_symbol(not available now) : str, optional
        Symbol of the asset to use as a treasury. Symbol will be looked up as
        of the end date of the backtest.
    no_treasury : bool
        Flag indicating that no treasury is configured. Treasury-dependent
        metrics will be calculated using a dummy treasury of all-zero returns.
        
    Reference
    ----------
    BenchmarkSpec()    
    """

    def __init__(
        self,
        treasury_returns,
        treasury_file,
        treasury_sid,
        treasury_symbol,
        no_treasury,
    ):

        self.treasury_returns = treasury_returns
        self.treasury_file = treasury_file
        self.treasury_sid = treasury_sid
        self.treasury_symbol = treasury_symbol
        self.no_treasury = no_treasury

    @classmethod
    def from_cli_params(
        cls, treasury_sid, treasury_symbol, treasury_file, no_treasury
    ):
        #do not use treasury_returns as input
        return cls(
            treasury_returns=None,
            treasury_sid=treasury_sid,
            treasury_symbol=treasury_symbol,
            treasury_file=treasury_file,
            no_treasury=no_treasury,
        )

    @classmethod
    def from_returns(cls, treasury_returns):
        #use treasury_returns as input
        return cls(
            treasury_returns=treasury_returns,
            treasury_file=None,
            treasury_sid=None,
            treasury_symbol=None,
            no_treasury=treasury_returns is None,
        )

    def resolve(self, asset_finder, start_date, end_date):
        """
        Resolve inputs into values to be passed to TradingAlgorithm.

        Returns a pair of ``(treasury_sid, treasury_returns)`` with at most
        one non-None value. Both values may be None if no treasury source has
        been configured.

        Parameters
        ----------
        asset_finder : zipline.assets.AssetFinder
            Asset finder for the algorithm to be run.
        start_date : pd.Timestamp
            Start date of the algorithm to be run.
        end_date : pd.Timestamp
            End date of the algorithm to be run.

        Returns
        -------
        treasury_sid : int
            Sid to use as treasury.
        treasury_returns : pd.Series
            Series of returns to use as treasury.
        """
        
        #inputs priority(treasury_returns>treasury_file>treasury_sid>treasury_symbol>no_treasury)
        if self.treasury_returns is not None:
            treasury_sid = None
            treasury_returns = self.treasury_returns
        elif self.treasury_file is not None:
            treasury_sid = None
            treasury_returns = get_treasury_returns_from_file(
                self.treasury_file,
            )
        elif self.treasury_sid is not None:
            treasury_sid = self.treasury_sid
            treasury_returns = None
        elif self.treasury_symbol is not None:
            try:
                asset = asset_finder.lookup_symbol(
                    self.treasury_symbol,
                    as_of_date=end_date,
                )
                treasury_sid = asset.sid
                treasury_returns = None
            except SymbolNotFound:
                raise _RunAlgoError(
                    "Symbol %r as a treasury not found in this bundle."
                    % self.treasury_symbol
                )
        elif self.no_treasury:
            treasury_sid = None
            treasury_returns = self._zero_treasury_returns(
                start_date=start_date,
                end_date=end_date,
            )
        else:
            log.warn(
                "No treasury configured. " "Assuming algorithm calls set_treasury."
            )
            log.warn(
                "Pass --treasury-sid, --treasury-symbol, or"
                " --treasury-file to set a source of treasury returns."
            )
            log.warn(
                "Pass --no-treasury to use a dummy treasury " "of zero returns.",
            )
            treasury_sid = None
            treasury_returns = None

        return treasury_sid, treasury_returns

    @staticmethod
    def _zero_treasury_returns(start_date, end_date):
        return pd.Series(
            index=pd.date_range(start_date, end_date, tz="utc"),
            data=0.0,
        )
        
#20230210 (by MRC) for treasury return column in perf  #end#
#--------------------------------------------------------------------