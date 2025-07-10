#
# Copyright 2016 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from typing import List

from operator import mul

from pandas.core.api import Timestamp as Timestamp

from logbook import Logger

import numpy as np
from numpy import float64, int64, nan

import pandas as pd
from pandas import isnull

import re
from toolz import curry, complement, take
import errno
import os
import sqlite3
from functools import reduce
from collections import Counter

from zipline.protocol import BarData

from zipline.finance.asset_restrictions import NoRestrictions

from zipline.assets import (
    Asset,
    AssetConvertible,
    Equity,
    Future,
    PricingDataAssociable,
)
from zipline.assets.continuous_futures import ContinuousFuture

from zipline.data.continuous_future_reader import (
    ContinuousFutureSessionBarReader,
    ContinuousFutureMinuteBarReader,
)
from zipline.assets.roll_finder import (
    CalendarRollFinder,
    VolumeRollFinder,
)
from zipline.data.dispatch_bar_reader import (
    AssetDispatchMinuteBarReader,
    AssetDispatchSessionBarReader,
)
from zipline.data.resample import (
    DailyHistoryAggregator,
    ReindexMinuteBarReader,
    ReindexSessionBarReader,
)
from zipline.data.history_loader import (
    DailyHistoryLoader,
    MinuteHistoryLoader,
)
from zipline.data.bar_reader import NoDataOnDate
from zipline.data import bundles
from zipline.data.bundles.core import UnknownBundle, from_bundle_ingest_dirname

from zipline.utils.memoize import remember_last
from zipline.utils.pandas_utils import normalize_date
from zipline.utils.calendar_utils import get_calendar
from zipline.utils.input_validation import expect_element
import zipline.utils.paths as pth
from TejToolAPI import Map_Dask_API as dask_api

from zipline.errors import HistoryWindowStartsBeforeData


log = Logger("DataPortal")

BASE_FIELDS = frozenset(
    [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "price",
        "open_price", # !343 #113 for opening price backtesting
        "high_price", # !343 #113 for opening price backtesting
        "low_price",  # !343 #113 for opening price backtesting
        "contract",
        "sid",
        "last_traded",
        "annotation", # 20230914 add annotation
    ]
)

OHLCV_FIELDS_LIST = ["open", "high", "low", "close", "volume"]
OHLCV_FIELDS = frozenset(OHLCV_FIELDS_LIST)

OHLCV_ADJ_FIELDS_LIST = ["open_adj", "high_adj", "low_adj", "close_adj", "volume_adj"]
OHLCV_ADJ_FIELDS = frozenset(OHLCV_ADJ_FIELDS_LIST)

OHLCVP_FIELDS_LIST = ["open", "high", "low", "close", "volume", "price"]
OHLCVP_FIELDS = frozenset(OHLCVP_FIELDS_LIST)

PRICE_MAPPING = {'price':'close',
                 'open_price':'open',
                 'high_price':'high',
                 'low_price':'low'}
INVERSE_PRICE_MAPPING = {v: k for k, v in PRICE_MAPPING.items()}

HISTORY_FREQUENCIES = set(["1m", "1d"])

DEFAULT_MINUTE_HISTORY_PREFETCH = 1560
DEFAULT_DAILY_HISTORY_PREFETCH = 40

_DEF_M_HIST_PREFETCH = DEFAULT_MINUTE_HISTORY_PREFETCH
_DEF_D_HIST_PREFETCH = DEFAULT_DAILY_HISTORY_PREFETCH


class DataPortal(object):
    """Interface to all of the data that a zipline simulation needs.

    This is used by the simulation runner to answer questions about the data,
    like getting the prices of assets on a given day or to service history
    calls.

    Parameters
    ----------
    asset_finder : zipline.assets.assets.AssetFinder
        The AssetFinder instance used to resolve assets.
    trading_calendar: zipline.utils.calendar.exchange_calendar.TradingCalendar
        The calendar instance used to provide minute->session information.
    first_trading_day : pd.Timestamp
        The first trading day for the simulation.
    equity_daily_reader : BcolzDailyBarReader, optional
        The daily bar reader for equities. This will be used to service
        daily data backtests or daily history calls in a minute backetest.
        If a daily bar reader is not provided but a minute bar reader is,
        the minutes will be rolled up to serve the daily requests.
    equity_minute_reader : BcolzMinuteBarReader, optional
        The minute bar reader for equities. This will be used to service
        minute data backtests or minute history calls. This can be used
        to serve daily calls if no daily bar reader is provided.
    future_daily_reader : BcolzDailyBarReader, optional
        The daily bar ready for futures. This will be used to service
        daily data backtests or daily history calls in a minute backetest.
        If a daily bar reader is not provided but a minute bar reader is,
        the minutes will be rolled up to serve the daily requests.
    future_minute_reader : BcolzFutureMinuteBarReader, optional
        The minute bar reader for futures. This will be used to service
        minute data backtests or minute history calls. This can be used
        to serve daily calls if no daily bar reader is provided.
    adjustment_reader : SQLiteAdjustmentWriter, optional
        The adjustment reader. This is used to apply splits, dividends, and
        other adjustment data to the raw data from the readers.
    last_available_session : pd.Timestamp, optional
        The last session to make available in session-level data.
    last_available_minute : pd.Timestamp, optional
        The last minute to make available in minute-level data.
    """

    def __init__(
        self,
        asset_finder,
        trading_calendar,
        first_trading_day,
        equity_daily_reader=None,
        equity_minute_reader=None,
        future_daily_reader=None,
        future_minute_reader=None,
        adjustment_reader=None,
        last_available_session=None,
        last_available_minute=None,
        minute_history_prefetch_length=_DEF_M_HIST_PREFETCH,
        daily_history_prefetch_length=_DEF_D_HIST_PREFETCH,
    ):

        self.trading_calendar = trading_calendar

        self.asset_finder = asset_finder

        self._adjustment_reader = adjustment_reader

        # caches of sid -> adjustment list
        self._splits_dict = {}
        self._mergers_dict = {}
        self._dividends_dict = {}

        # Handle extra sources, like Fetcher.
        self._augmented_sources_map = {}
        self._extra_source_df = None

        self._first_available_session = first_trading_day

        if last_available_session:
            self._last_available_session = last_available_session
        else:
            # Infer the last session from the provided readers.
            last_sessions = [
                reader.last_available_dt
                for reader in [equity_daily_reader, future_daily_reader]
                if reader is not None
            ]
            if last_sessions:
                self._last_available_session = min(last_sessions)
            else:
                self._last_available_session = None

        if last_available_minute:
            self._last_available_minute = last_available_minute
        else:
            # Infer the last minute from the provided readers.
            last_minutes = [
                reader.last_available_dt
                for reader in [equity_minute_reader, future_minute_reader]
                if reader is not None
            ]
            if last_minutes:
                self._last_available_minute = max(last_minutes)
            else:
                self._last_available_minute = None

        aligned_equity_minute_reader = self._ensure_reader_aligned(equity_minute_reader)
        aligned_equity_session_reader = self._ensure_reader_aligned(equity_daily_reader)
        aligned_future_minute_reader = self._ensure_reader_aligned(future_minute_reader)
        aligned_future_session_reader = self._ensure_reader_aligned(future_daily_reader)

        self._roll_finders = {
            "calendar": CalendarRollFinder(self.trading_calendar, self.asset_finder),
        }

        aligned_minute_readers = {}
        aligned_session_readers = {}

        if aligned_equity_minute_reader is not None:
            aligned_minute_readers[Equity] = aligned_equity_minute_reader
        if aligned_equity_session_reader is not None:
            aligned_session_readers[Equity] = aligned_equity_session_reader

        if aligned_future_minute_reader is not None:
            aligned_minute_readers[Future] = aligned_future_minute_reader
            aligned_minute_readers[ContinuousFuture] = ContinuousFutureMinuteBarReader(
                aligned_future_minute_reader,
                self._roll_finders,
            )

        if aligned_future_session_reader is not None:
            aligned_session_readers[Future] = aligned_future_session_reader
            self._roll_finders["volume"] = VolumeRollFinder(
                self.trading_calendar,
                self.asset_finder,
                aligned_future_session_reader,
            )
            aligned_session_readers[
                ContinuousFuture
            ] = ContinuousFutureSessionBarReader(
                aligned_future_session_reader,
                self._roll_finders,
            )

        _dispatch_minute_reader = AssetDispatchMinuteBarReader(
            self.trading_calendar,
            self.asset_finder,
            aligned_minute_readers,
            self._last_available_minute,
        )

        _dispatch_session_reader = AssetDispatchSessionBarReader(
            self.trading_calendar,
            self.asset_finder,
            aligned_session_readers,
            self._last_available_session,
        )

        self._pricing_readers = {
            "minute": _dispatch_minute_reader,
            "daily": _dispatch_session_reader,
        }

        self._daily_aggregator = DailyHistoryAggregator(
            self.trading_calendar.schedule.market_open,
            _dispatch_minute_reader,
            self.trading_calendar,
        )
        self._history_loader = DailyHistoryLoader(
            self.trading_calendar,
            _dispatch_session_reader,
            self._adjustment_reader,
            self.asset_finder,
            self._roll_finders,
            prefetch_length=daily_history_prefetch_length,
        )
        self._minute_history_loader = MinuteHistoryLoader(
            self.trading_calendar,
            _dispatch_minute_reader,
            self._adjustment_reader,
            self.asset_finder,
            self._roll_finders,
            prefetch_length=minute_history_prefetch_length,
        )

        self._first_trading_day = first_trading_day

        # Get the first trading minute
        self._first_trading_minute, _ = (
            self.trading_calendar.open_and_close_for_session(self._first_trading_day)
            if self._first_trading_day is not None
            else (None, None)
        )

        # Store the locs of the first day and first minute
        self._first_trading_day_loc = (
            self.trading_calendar.all_sessions.get_loc(self._first_trading_day)
            if self._first_trading_day is not None
            else None
        )

    def _ensure_reader_aligned(self, reader):
        if reader is None:
            return

        if reader.trading_calendar.name == self.trading_calendar.name:
            return reader
        elif reader.data_frequency == "minute":
            return ReindexMinuteBarReader(
                self.trading_calendar,
                reader,
                self._first_available_session,
                self._last_available_session,
            )
        elif reader.data_frequency == "session":
            return ReindexSessionBarReader(
                self.trading_calendar,
                reader,
                self._first_available_session,
                self._last_available_session,
            )

    def _reindex_extra_source(self, df, source_date_index):
        return df.reindex(index=source_date_index, method="ffill")

    def handle_extra_source(self, source_df, sim_params):
        """
        Extra sources always have a sid column.

        We expand the given data (by forward filling) to the full range of
        the simulation dates, so that lookup is fast during simulation.
        """
        if source_df is None:
            return

        # Normalize all the dates in the df
        source_df.index = source_df.index.normalize()

        # source_df's sid column can either consist of assets we know about
        # (such as sid(24)) or of assets we don't know about (such as
        # palladium).
        #
        # In both cases, we break up the dataframe into individual dfs
        # that only contain a single asset's information.  ie, if source_df
        # has data for PALLADIUM and GOLD, we split source_df into two
        # dataframes, one for each. (same applies if source_df has data for
        # AAPL and IBM).
        #
        # We then take each child df and reindex it to the simulation's date
        # range by forward-filling missing values. this makes reads simpler.
        #
        # Finally, we store the data. For each column, we store a mapping in
        # self.augmented_sources_map from the column to a dictionary of
        # asset -> df.  In other words,
        # self.augmented_sources_map['days_to_cover']['AAPL'] gives us the df
        # holding that data.
        source_date_index = self.trading_calendar.sessions_in_range(
            sim_params.start_session, sim_params.end_session
        )

        # Break the source_df up into one dataframe per sid.  This lets
        # us (more easily) calculate accurate start/end dates for each sid,
        # de-dup data, and expand the data to fit the backtest start/end date.
        grouped_by_sid = source_df.groupby(["sid"])
        group_names = grouped_by_sid.groups.keys()
        group_dict = {}
        for group_name in group_names:
            group_dict[group_name] = grouped_by_sid.get_group(group_name)           

        # This will be the dataframe which we query to get fetcher assets at
        # any given time. Get's overwritten every time there's a new fetcher
        # call
        extra_source_df = pd.DataFrame()

        for identifier, df in group_dict.items():
            # Since we know this df only contains a single sid, we can safely
            # de-dupe by the index (dt). If minute granularity, will take the
            # last data point on any given day
            df = df.groupby(level=0).last()

            # Reindex the dataframe based on the backtest start/end date.
            # This makes reads easier during the backtest.
            df = self._reindex_extra_source(df, source_date_index)

            for col_name in df.columns.difference(["sid"]):
                if col_name not in self._augmented_sources_map:
                    self._augmented_sources_map[col_name] = {}

                self._augmented_sources_map[col_name][identifier] = df

            # Append to extra_source_df the reindexed dataframe for the single
            # sid
            extra_source_df = pd.concat([extra_source_df,df])
            
        self._extra_source_df = extra_source_df


 
    def _get_pricing_reader(self, data_frequency):
        return self._pricing_readers[data_frequency]

    def get_last_traded_dt(self, asset, dt, data_frequency):
        """
        Given an asset and dt, returns the last traded dt from the viewpoint
        of the given dt.

        If there is a trade on the dt, the answer is dt provided.
        """
        return self._get_pricing_reader(data_frequency).get_last_traded_dt(asset, dt)

    @staticmethod
    def _is_extra_source(asset, field, map):
        """
        Internal method that determines if this asset/field combination
        represents a fetcher value or a regular OHLCVP lookup.
        """
        # If we have an extra source with a column called "price", only look
        # at it if it's on something like palladium and not AAPL (since our
        # own price data always wins when dealing with assets).

        return not (
            field in BASE_FIELDS and (isinstance(asset, (Asset, ContinuousFuture)))
        )

    def _get_fetcher_value(self, asset, field, dt):
        day = normalize_date(dt)

        try:
            return self._augmented_sources_map[field][asset].loc[day, field]
        except KeyError:
            return np.NaN

    def _get_single_asset_value(self, session_label, asset, field, dt, data_frequency):
        if self._is_extra_source(asset, field, self._augmented_sources_map):
            return self._get_fetcher_value(asset, field, dt)

        if field not in BASE_FIELDS:
            raise KeyError("Invalid column: " + str(field))

        if (
            dt < asset.start_date
            or (data_frequency == "daily" and session_label > asset.end_date)
            or (data_frequency == "minute" and session_label > asset.end_date)
        ):
            if field == "volume":
                return 0
            elif field == "contract":
                return None
            elif field != "last_traded":
                return np.NaN

        if data_frequency == "daily":
            if field == "contract":
                return self._get_current_contract(asset, session_label)
            else:
                return self._get_daily_spot_value(
                    asset,
                    field, # fix no open_price , high_price , low_price error
                    session_label,
                )
        else:
            if field == "last_traded":
                return self.get_last_traded_dt(asset, dt, "minute")
            elif field == "price":
                return self._get_minute_spot_value(
                    asset,
                    "close",
                    dt,
                    ffill=True,
                )
            elif field == "contract":
                return self._get_current_contract(asset, dt)
            elif field == 'open_price' :
                return self._get_minute_spot_value(
                    asset,
                    "open",
                    dt,
                    ffill=True,
                )
            elif field == 'high_price' :
                return self._get_minute_spot_value(
                    asset,
                    "high",
                    dt,
                    ffill=True,
                )
            elif field == 'low_price' :
                return self._get_minute_spot_value(
                    asset,
                    "low",
                    dt,
                    ffill=True,
                )
            else:
                return self._get_minute_spot_value(
                    asset ,
                    field ,
                    dt)

    def get_spot_value(self, assets, field, dt, data_frequency):
        """
        Public API method that returns a scalar value representing the value
        of the desired asset's field at either the given dt.

        Parameters
        ----------
        assets : Asset, ContinuousFuture, or iterable of same.
            The asset or assets whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'open_price', 'high_price', 'low_price', 'price',
                 'last_traded'}
            The desired field of the asset.
        dt : pd.Timestamp
            The timestamp for the desired value.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or pd.Timestamp
            The spot value of ``field`` for ``asset`` The return type is based
            on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', 'open_price', 'high_price', 'low_price' or 'price', 
            the value will be a float. If the ``field`` is 'volume' the value 
            will be a int. If the ``field`` is 'last_traded' the value will be
            a Timestamp.
        """
        assets_is_scalar = False
        if isinstance(assets, (AssetConvertible, PricingDataAssociable)):
            assets_is_scalar = True
        else:
            # If 'assets' was not one of the expected types then it should be
            # an iterable.
            try:
                iter(assets)
            except TypeError:
                raise TypeError(
                    "Unexpected 'assets' value of type {}.".format(type(assets))
                )

        session_label = self.trading_calendar.minute_to_session_label(dt)

        if assets_is_scalar:
            return self._get_single_asset_value(
                session_label,
                assets,
                field,
                dt,
                data_frequency,
            )
        else:
            get_single_asset_value = self._get_single_asset_value
            return [
                get_single_asset_value(
                    session_label,
                    asset,
                    field,
                    dt,
                    data_frequency,
                )
                for asset in assets
            ]

    def get_scalar_asset_spot_value(self, asset, field, dt, data_frequency):
        """
        Public API method that returns a scalar value representing the value
        of the desired asset's field at either the given dt.

        Parameters
        ----------
        assets : Asset
            The asset or assets whose data is desired. This cannot be
            an arbitrary AssetConvertible.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'open_price', 'high_price', 'low_price', 'price',
                 'last_traded'}
            The desired field of the asset.
        dt : pd.Timestamp
            The timestamp for the desired value.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or pd.Timestamp
            The spot value of ``field`` for ``asset`` The return type is based
            on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', 'open_price', 'high_price', 'low_price' or 'price', 
            the value will be a float. If the ``field`` is 'volume' the value 
            will be a int. If the ``field`` is 'last_traded' the value will be
            a Timestamp.
        """
        return self._get_single_asset_value(
            self.trading_calendar.minute_to_session_label(dt),
            asset,
            field,
            dt,
            data_frequency,
        )

    def get_adjustments(self, assets, field, dt, perspective_dt):
        """
        Returns a list of adjustments between the dt and perspective_dt for the
        given field and list of assets

        Parameters
        ----------
        assets : list of type Asset, or Asset
            The asset, or assets whose adjustments are desired.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'open_price', 'high_price', 'low_price', 'price',
                 'last_traded'}
            The desired field of the asset.
        dt : pd.Timestamp
            The timestamp for the desired value.
        perspective_dt : pd.Timestamp
            The timestamp from which the data is being viewed back from.

        Returns
        -------
        adjustments : list[Adjustment]
            The adjustments to that field.
        """
        if isinstance(assets, Asset):
            assets = [assets]

        adjustment_ratios_per_asset = []

        def split_adj_factor(x):
            return x if field != "volume" else 1.0 / x

        for asset in assets:
            adjustments_for_asset = []
            split_adjustments = self._get_adjustment_list(
                asset, self._splits_dict, "SPLITS"
            )
            for adj_dt, adj in split_adjustments:
                if dt < adj_dt <= perspective_dt:
                    adjustments_for_asset.append(split_adj_factor(adj))
                elif adj_dt > perspective_dt:
                    break

            if field != "volume":
                merger_adjustments = self._get_adjustment_list(
                    asset, self._mergers_dict, "MERGERS"
                )
                for adj_dt, adj in merger_adjustments:
                    if dt < adj_dt <= perspective_dt:
                        adjustments_for_asset.append(adj)
                    elif adj_dt > perspective_dt:
                        break

                dividend_adjustments = self._get_adjustment_list(
                    asset,
                    self._dividends_dict,
                    "DIVIDENDS",
                )
                for adj_dt, adj in dividend_adjustments:
                    if dt < adj_dt <= perspective_dt:
                        adjustments_for_asset.append(adj)
                    elif adj_dt > perspective_dt:
                        break

            ratio = reduce(mul, adjustments_for_asset, 1.0)
            adjustment_ratios_per_asset.append(ratio)

        return adjustment_ratios_per_asset

    def get_adjusted_value(
        self, asset, field, dt, perspective_dt, data_frequency, spot_value=None
    ):
        """
        Returns a scalar value representing the value
        of the desired asset's field at the given dt with adjustments applied.

        Parameters
        ----------
        asset : Asset
            The asset whose data is desired.
        field : {'open', 'high', 'low', 'close', 'volume',
                 'open_price', 'high_price', 'low_price', 'price',
                 'last_traded'}
            The desired field of the asset.
        dt : pd.Timestamp
            The timestamp for the desired value.
        perspective_dt : pd.Timestamp
            The timestamp from which the data is being viewed back from.
        data_frequency : str
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars

        Returns
        -------
        value : float, int, or pd.Timestamp
            The value of the given ``field`` for ``asset`` at ``dt`` with any
            adjustments known by ``perspective_dt`` applied. The return type is
            based on the ``field`` requested. If the field is one of 'open', 'high',
            'low', 'close', 'open_price', 'high_price', 'low_price' or 'price',
            the value will be a float. If the ``field`` is 'volume' the value will
            be a int. If the ``field`` is 'last_traded' the value will be a Timestamp.
        """
        if spot_value is None:
            # if this a fetcher field, we want to use perspective_dt (not dt)
            # because we want the new value as of midnight (fetcher only works
            # on a daily basis, all timestamps are on midnight)
            if self._is_extra_source(asset, field, self._augmented_sources_map):
                spot_value = self.get_spot_value(
                    asset, field, perspective_dt, data_frequency
                )
            else:
                spot_value = self.get_spot_value(asset, field, dt, data_frequency)

        if isinstance(asset, (Equity , Future)):
            ratio = self.get_adjustments(asset, field, dt, perspective_dt)[0]
            spot_value *= ratio

        return spot_value

    def _get_minute_spot_value(self, asset, column, dt, ffill=False):
        reader = self._get_pricing_reader("minute")

        if not ffill:
            try:
                return reader.get_value(asset.sid, dt, column)
            except NoDataOnDate:
                if column != "volume":
                    return np.nan
                else:
                    return 0

        # At this point the pairing of column='close' and ffill=True is
        # assumed.
        try:
            # Optimize the best case scenario of a liquid asset
            # returning a valid price.
            result = reader.get_value(asset.sid, dt, column)
            if not pd.isnull(result):
                return result
        except NoDataOnDate:
            # Handling of no data for the desired date is done by the
            # forward filling logic.
            # The last trade may occur on a previous day.
            pass
        # If forward filling, we want the last minute with values (up to
        # and including dt).
        query_dt = reader.get_last_traded_dt(asset, dt)

        if pd.isnull(query_dt):
            # no last traded dt, bail
            return np.nan

        result = reader.get_value(asset.sid, query_dt, column)

        if (dt == query_dt) or (dt.date() == query_dt.date()):
            return result

        # the value we found came from a different day, so we have to
        # adjust the data if there are any adjustments on that day barrier
        return self.get_adjusted_value(
            asset, column, query_dt, dt, "minute", spot_value=result
        )

    def _get_daily_spot_value(self, asset, column, dt):
        reader = self._get_pricing_reader("daily")
        if column == "last_traded":
            last_traded_dt = reader.get_last_traded_dt(asset, dt)

            if isnull(last_traded_dt):
                return pd.NaT
            else:
                return last_traded_dt
        elif column in OHLCV_FIELDS or column == "annotation" : # 20230914 add annotation
            # don't forward fill
            try:
                return reader.get_value(asset, dt, column)
            except NoDataOnDate:
                return np.nan
        # !343 #113 for opening price backtesting: `PRICE_MAPPING.keys()` and `PRICE_MAPPING[column]`
        elif column in PRICE_MAPPING.keys():
            found_dt = dt
            while True:
                try:
                    value = reader.get_value(asset, found_dt, PRICE_MAPPING[column])
                    if not isnull(value):
                        if dt == found_dt:
                            return value
                        else:
                            # adjust if needed
                            return self.get_adjusted_value(
                                asset, column, found_dt, dt, "minute", spot_value=value
                            )
                    else:
                        # forward fill
                        found_dt -= self.trading_calendar.day
                except NoDataOnDate:
                    return np.nan

    @remember_last
    def _get_days_for_window(self, end_date, bar_count):
        tds = self.trading_calendar.all_sessions
        end_loc = tds.get_loc(end_date)
        start_loc = end_loc - bar_count + 1
        if start_loc < self._first_trading_day_loc:
            raise HistoryWindowStartsBeforeData(
                first_trading_day=self._first_trading_day.date(),
                bar_count=bar_count,
                suggested_start_day=tds[self._first_trading_day_loc + bar_count].date(),
            )
        return tds[start_loc : end_loc + 1]

    def _get_history_daily_window(
        self, assets, end_dt, bar_count, field_to_use, data_frequency
    ):
        """
        Internal method that returns a dataframe containing history bars
        of daily frequency for the given sids.
        """
        session = self.trading_calendar.minute_to_session_label(end_dt)
        days_for_window = self._get_days_for_window(session, bar_count)

        if len(assets) == 0:
            return pd.DataFrame(None, index=days_for_window, columns=None)

        data = self._get_history_daily_window_data(
            assets, days_for_window, end_dt, field_to_use, data_frequency
        )
        return pd.DataFrame(data, index=days_for_window, columns=assets)

    def _get_history_daily_window_data(
        self, assets, days_for_window, end_dt, field_to_use, data_frequency
    ):
        if data_frequency == "daily":
            # two cases where we use daily data for the whole range:
            # 1) the history window ends at midnight utc.
            # 2) the last desired day of the window is after the
            # last trading day, use daily data for the whole range.
            return self._get_daily_window_data(
                assets, field_to_use, days_for_window, extra_slot=False
            )
        else:
            # minute mode, requesting '1d'
            daily_data = self._get_daily_window_data(
                assets, field_to_use, days_for_window[0:-1]
            )

            if field_to_use == "open":
                minute_value = self._daily_aggregator.opens(assets, end_dt)
            elif field_to_use == "high":
                minute_value = self._daily_aggregator.highs(assets, end_dt)
            elif field_to_use == "low":
                minute_value = self._daily_aggregator.lows(assets, end_dt)
            elif field_to_use == "close":
                minute_value = self._daily_aggregator.closes(assets, end_dt)
            elif field_to_use == "volume":
                minute_value = self._daily_aggregator.volumes(assets, end_dt)
            elif field_to_use == "sid":
                minute_value = [
                    int(self._get_current_contract(asset, end_dt)) for asset in assets
                ]

            # append the partial day.
            daily_data[-1] = minute_value

            return daily_data

    def _handle_minute_history_out_of_bounds(self, bar_count):
        cal = self.trading_calendar

        first_trading_minute_loc = (
            cal.all_minutes.get_loc(self._first_trading_minute)
            if self._first_trading_minute is not None
            else None
        )

        suggested_start_day = cal.minute_to_session_label(
            cal.all_minutes[first_trading_minute_loc + bar_count] + cal.day
        )

        raise HistoryWindowStartsBeforeData(
            first_trading_day=self._first_trading_day.date(),
            bar_count=bar_count,
            suggested_start_day=suggested_start_day.date(),
        )

    def _get_history_minute_window(self, assets, end_dt, bar_count, field_to_use):
        """
        Internal method that returns a dataframe containing history bars
        of minute frequency for the given sids.
        """
        # get all the minutes for this window
        try:
            minutes_for_window = self.trading_calendar.minutes_window(
                end_dt, -bar_count
            )
        except KeyError:
            self._handle_minute_history_out_of_bounds(bar_count)

        if minutes_for_window[0] < self._first_trading_minute:
            self._handle_minute_history_out_of_bounds(bar_count)

        asset_minute_data = self._get_minute_window_data(
            assets,
            field_to_use,
            minutes_for_window,
        )

        return pd.DataFrame(asset_minute_data, index=minutes_for_window, columns=assets)

    def get_history_window(
        self, assets, end_dt, bar_count, frequency, field, data_frequency, ffill=True
    ):
        """
        Public API method that returns a dataframe containing the requested
        history window.  Data is fully adjusted.

        Parameters
        ----------
        assets : list of zipline.data.Asset objects
            The assets whose data is desired.

        bar_count: int
            The number of bars desired.

        frequency: string
            "1d" or "1m"

        field: string
            The desired field of the asset.

        data_frequency: string
            The frequency of the data to query; i.e. whether the data is
            'daily' or 'minute' bars.

        ffill: boolean
            Forward-fill missing values. Only has effect if field
            is 'price'.

        Returns
        -------
        A dataframe containing the requested data.
        """
        if field not in OHLCVP_FIELDS and field != "sid":
            raise ValueError("Invalid field: {0}".format(field))

        if bar_count < 1:
            raise ValueError("bar_count must be >= 1, but got {}".format(bar_count))

        if frequency == "1d":
            if field == "price":
                df = self._get_history_daily_window(
                    assets, end_dt, bar_count, "close", data_frequency
                )
            else:
                df = self._get_history_daily_window(
                    assets, end_dt, bar_count, field, data_frequency
                )
        elif frequency == "1m":
            if field == "price":
                df = self._get_history_minute_window(assets, end_dt, bar_count, "close")
            else:
                df = self._get_history_minute_window(assets, end_dt, bar_count, field)
        else:
            raise ValueError("Invalid frequency: {0}".format(frequency))

        # forward-fill price
        if field == "price":
            if frequency == "1m":
                ffill_data_frequency = "minute"
            elif frequency == "1d":
                ffill_data_frequency = "daily"
            else:
                raise Exception("Only 1d and 1m are supported for forward-filling.")

            assets_with_leading_nan = np.where(isnull(df.iloc[0]))[0]

            history_start, history_end = df.index[[0, -1]]
            if ffill_data_frequency == "daily" and data_frequency == "minute":
                # When we're looking for a daily value, but we haven't seen any
                # volume in today's minute bars yet, we need to use the
                # previous day's ffilled daily price. Using today's daily price
                # could yield a value from later today.
                history_start -= self.trading_calendar.day

            initial_values = []
            for asset in df.columns[assets_with_leading_nan]:
                last_traded = self.get_last_traded_dt(
                    asset,
                    history_start,
                    ffill_data_frequency,
                )
                if isnull(last_traded):
                    initial_values.append(nan)
                else:
                    initial_values.append(
                        self.get_adjusted_value(
                            asset,
                            field,
                            dt=last_traded,
                            perspective_dt=history_end,
                            data_frequency=ffill_data_frequency,
                        )
                    )

            # Set leading values for assets that were missing data, then ffill.
            df.iloc[0, assets_with_leading_nan] = np.array(
                initial_values, dtype=np.float64
            )
            df.fillna(method="ffill", inplace=True)

            # forward-filling will incorrectly produce values after the end of
            # an asset's lifetime, so write NaNs back over the asset's
            # end_date.
            normed_index = df.index.normalize()
            for asset in df.columns:
                if history_end >= asset.end_date:
                    # if the window extends past the asset's end date, set
                    # all post-end-date values to NaN in that asset's series
                    df.loc[normed_index > asset.end_date, asset] = nan
        return df

    def _get_minute_window_data(self, assets, field, minutes_for_window):
        """
        Internal method that gets a window of adjusted minute data for an asset
        and specified date range.  Used to support the history API method for
        minute bars.

        Missing bars are filled with NaN.

        Parameters
        ----------
        assets : iterable[Asset]
            The assets whose data is desired.

        field: string
            The specific field to return.  "open", "high", "close_price", etc.

        minutes_for_window: pd.DateTimeIndex
            The list of minutes representing the desired window.  Each minute
            is a pd.Timestamp.

        Returns
        -------
        A numpy array with requested values.
        """
        return self._minute_history_loader.history(
            assets, minutes_for_window, field, False
        )

    def _get_daily_window_data(self, assets, field, days_in_window, extra_slot=True):
        """
        Internal method that gets a window of adjusted daily data for a sid
        and specified date range.  Used to support the history API method for
        daily bars.

        Parameters
        ----------
        asset : Asset
            The asset whose data is desired.

        start_dt: pandas.Timestamp
            The start of the desired window of data.

        bar_count: int
            The number of days of data to return.

        field: string
            The specific field to return.  "open", "high", "close_price", etc.

        extra_slot: boolean
            Whether to allocate an extra slot in the returned numpy array.
            This extra slot will hold the data for the last partial day.  It's
            much better to create it here than to create a copy of the array
            later just to add a slot.

        Returns
        -------
        A numpy array with requested values.  Any missing slots filled with
        nan.

        """
        bar_count = len(days_in_window)
        # create an np.array of size bar_count
        dtype = float64 if field != "sid" else int64
        if extra_slot:
            return_array = np.zeros((bar_count + 1, len(assets)), dtype=dtype)
        else:
            return_array = np.zeros((bar_count, len(assets)), dtype=dtype)

        if field != "volume":
            # volumes default to 0, so we don't need to put NaNs in the array
            return_array = return_array.astype(float64)
            return_array[:] = np.NAN

        if bar_count != 0:
            data = self._history_loader.history(
                assets, days_in_window, field, extra_slot
            )
            if extra_slot:
                return_array[: len(return_array) - 1, :] = data
            else:
                return_array[: len(data)] = data
        return return_array

    def _get_adjustment_list(self, asset, adjustments_dict, table_name):
        """
        Internal method that returns a list of adjustments for the given sid.

        Parameters
        ----------
        asset : Asset
            The asset for which to return adjustments.

        adjustments_dict: dict
            A dictionary of sid -> list that is used as a cache.

        table_name: string
            The table that contains this data in the adjustments db.

        Returns
        -------
        adjustments: list
            A list of [multiplier, pd.Timestamp], earliest first

        """
        if self._adjustment_reader is None:
            return []

        sid = int(asset)

        try:
            adjustments = adjustments_dict[sid]
        except KeyError:
            adjustments = adjustments_dict[
                sid
            ] = self._adjustment_reader.get_adjustments_for_sid(table_name, sid)

        return adjustments

    def get_splits(self, assets, dt):
        """
        Returns any splits for the given sids and the given dt.

        Parameters
        ----------
        assets : container
            Assets for which we want splits.
        dt : pd.Timestamp
            The date for which we are checking for splits. Note: this is
            expected to be midnight UTC.

        Returns
        -------
        splits : list[(asset, float)]
            List of splits, where each split is a (asset, ratio) tuple.
        """
        if self._adjustment_reader is None or not assets:
            return []

        # convert dt to # of seconds since epoch, because that's what we use
        # in the adjustments db
        seconds = int(dt.value / 1e9)

        splits = self._adjustment_reader.conn.execute(
            "SELECT sid, ratio FROM SPLITS WHERE effective_date = ?", (seconds,)
        ).fetchall()

        splits = [split for split in splits if split[0] in assets]
        splits = [
            (self.asset_finder.retrieve_asset(split[0]), split[1]) for split in splits
        ]

        return splits

    def get_stock_dividends(self, sid, trading_days):
        """
        Returns all the stock dividends for a specific sid that occur
        in the given trading range.

        Parameters
        ----------
        sid: int
            The asset whose stock dividends should be returned.

        trading_days: pd.DatetimeIndex
            The trading range.

        Returns
        -------
        list: A list of objects with all relevant attributes populated.
        All timestamp fields are converted to pd.Timestamps.
        """

        if self._adjustment_reader is None:
            return []

        if len(trading_days) == 0:
            return []

        start_dt = trading_days[0].value / 1e9
        end_dt = trading_days[-1].value / 1e9

        dividends = self._adjustment_reader.conn.execute(
            "SELECT declared_date, ex_date, pay_date, payment_sid, ratio, "
            "record_date, sid FROM stock_dividend_payouts "
            "WHERE sid = ? AND ex_date > ? AND pay_date < ?",
            (
                int(sid),
                start_dt,
                end_dt,
            ),
        ).fetchall()

        dividend_info = []
        for dividend_tuple in dividends:
            dividend_info.append(
                {
                    "declared_date": pd.Timestamp(dividend_tuple[0], unit="s"),
                    "ex_date": pd.Timestamp(dividend_tuple[1], unit="s"),
                    "pay_date": pd.Timestamp(dividend_tuple[2], unit="s"),
                    "payment_sid": dividend_tuple[3],
                    "ratio": dividend_tuple[4],
                    "record_date": pd.Timestamp(dividend_tuple[5], unit="s"),
                    "sid": dividend_tuple[6],
                }
            )

        return dividend_info

    def contains(self, asset, field):
        return field in BASE_FIELDS or (
            field in self._augmented_sources_map
            and asset in self._augmented_sources_map[field]
        )

    def get_fetcher_assets(self, dt):
        """
        Returns a list of assets for the current date, as defined by the
        fetcher data.

        Returns
        -------
        list: a list of Asset objects.
        """
        # return a list of assets for the current date, as defined by the
        # fetcher source
        if self._extra_source_df is None:
            return []

        day = normalize_date(dt)

        if day in self._extra_source_df.index:
            assets = self._extra_source_df.loc[day]["sid"]
        else:
            return []

        if isinstance(assets, pd.Series):
            return [x for x in assets if isinstance(x, Asset)]
        else:
            return [assets] if isinstance(assets, Asset) else []

    def get_current_future_chain(self, continuous_future, dt):
        """
        Retrieves the future chain for the contract at the given `dt` according
        the `continuous_future` specification.

        Returns
        -------

        future_chain : list[Future]
            A list of active futures, where the first index is the current
            contract specified by the continuous future definition, the second
            is the next upcoming contract and so on.
        """
        rf = self._roll_finders[continuous_future.roll_style]
        session = self.trading_calendar.minute_to_session_label(dt)
        contract_center = rf.get_contract_center(
            continuous_future.root_symbol, session, continuous_future.offset
        )
        oc = self.asset_finder.get_ordered_contracts(continuous_future.root_symbol)
        chain = oc.active_chain(contract_center, session.value)
        return self.asset_finder.retrieve_all(chain)

    def _get_current_contract(self, continuous_future, dt):
        rf = self._roll_finders[continuous_future.roll_style]
        contract_sid = rf.get_contract_center(
            continuous_future.root_symbol, dt, continuous_future.offset
        )
        if contract_sid is None:
            return None
        return self.asset_finder.retrieve_asset(contract_sid)

    @property
    def adjustment_reader(self):
        return self._adjustment_reader


##############
# get_bundle #
##############
def get_bundle_price(
        start_dt: pd.Timestamp | str,
        end_dt: pd.Timestamp | str,
        fields: str | List[str] = None,
        bundle_name: str = 'tquant',
        calendar_name: str = 'TEJ_XTAI',
        frequency: str = '1d',
        data_frequency: str = 'daily',
        assets: list = None,
        transform: bool = False
    ) -> pd.DataFrame:
    """
    Export the bundle’s price and volume data (both pre- and post-adjustment) to a DataFrame.

    The post-adjustment price is backward-adjusted using `end_dt` as the reference date, so the
    final adjustment price may differ depending on the selected `end_dt`.

    See `zipline.protocol.BarData` or `DataPortal.get_history_window` for more details.

    Parameters
    ----------
    start_dt: pandas.Timestamp | str
        The start of the desired window of data.
    end_dt: pandas.Timestamp | str
        The end session of the desired window of data.
    fields: string or list[string], optional
        The specific field to return. If set to `None`, all fields will be returned.
        See `OHLCV_ADJ_FIELDS` and `OHLCV_FIELDS` for more details.
    bundle_name : str, optional
        The name of the bundle. Default is 'tquant'.
    calendar_name : str, optional
       The name of a calendar used to align bundle data. Default is 'TEJ_XTAI'.
    frequency: string, optional
        The frequency of the data to query; i.e. '1d'. Default is '1d'.
    data_frequency: string, optional
        The frequency of the data to query; i.e. 'daily'. Default is 'daily'.
    assets : list of `zipline.data.Asset objects`, optional
        The assets whose data is desired.
    transform : bool, optional
        If True then transform data format into a DataFrame with MultiIndex
        columns, where level 0 is `fields` and level 1 is `Asset` objects, and the
        index is a 'DatetimeIndex'. (`For alphalens`)

    Returns
    -------
    df : pd.DataFrame
        A DataFrame containing the bundle's price and volume data.

    Raises
    ------
    IndexError
        Raised if the requested date range exceeds the available trading day coverage,
        or if the required `bar_count` surpasses the actual data length.

        For instance, this can occur when `start_dt` is earlier than the earliest available
        trading day or `end_dt` is beyond the available range.

        Please adjust `start_dt` (or choose a valid date range) to avoid this issue.
    """
    def _get_history(adj, fields, N_tradate, transform=False):

        adjustment_reader = bundle.adjustment_reader if adj else None

        portal = DataPortal(
            asset_finder=bundle.asset_finder,
            trading_calendar=get_calendar(calendar_name),
            first_trading_day=bundle.equity_daily_bar_reader.first_trading_day,
            # equity_minute_reader=bundle.equity_minute_bar_reader,
            equity_daily_reader=bundle.equity_daily_bar_reader,
            adjustment_reader=adjustment_reader
            )

        if not transform:
            Bar = BarData(
                data_portal=portal,
                simulation_dt_func=lambda: end_dt,
                data_frequency=data_frequency,
                trading_calendar=get_calendar(calendar_name),
                restrictions=NoRestrictions()
            )
            df = Bar.history(assets=assets, fields=fields, bar_count=N_tradate, frequency=frequency)
            df.index.names = ['date','asset']

            # Add the suffix '_adj' to the column names in adj_field.
            if adj:
                df.columns = [f'{col}_adj' for col in df.columns]
            return df

        else:
            dict = {}
            for i in fields:
                df = portal.get_history_window(
                    assets=assets,
                    end_dt=end_dt,
                    bar_count=N_tradate,
                    frequency=frequency,
                    field=i,
                    data_frequency=data_frequency
                )

                name = i if not adj else i+'_adj'
                dict[name] = df
            return dict

    bundle = bundles.load(bundle_name)

    # assets
    all_sids = bundle.asset_finder.equities_sids
    all_assets = bundle.asset_finder.retrieve_all(all_sids)
    if assets is None:
        assets = all_assets.copy()

    # Convert start_dt and end_dt to UTC if they are pandas Timestamps
    if isinstance(start_dt, pd.Timestamp):
        if start_dt.tzinfo is None:
            start_dt = start_dt.tz_localize('UTC')
        else:
            start_dt = start_dt.tz_convert('UTC')

    if isinstance(end_dt, pd.Timestamp):
        if end_dt.tzinfo is None:
            end_dt = end_dt.tz_localize('UTC')
        else:
            end_dt = end_dt.tz_convert('UTC')

    # Convert start_dt and end_dt to pandas Timestamps if they are strings
    if isinstance(start_dt, str):
        start_dt = pd.Timestamp(start_dt, tz='UTC')
    if isinstance(end_dt, str):
        end_dt = pd.Timestamp(end_dt, tz='UTC')

    # 避免get_bundle出來的日期與start_dt及end_dt不一致
    if not get_calendar(calendar_name).is_session(end_dt):
        end_dt = get_calendar(calendar_name).previous_close(end_dt).normalize()

    max_data_end = max([asset.end_date for asset in all_assets])
    if end_dt > max_data_end:
        log.info('`End_dt` is later than the latest end date of the assets. Adjusting end_dt to the latest end date.')
        end_dt = max_data_end

    min_start = min([asset.start_date for asset in all_assets])
    if start_dt < min_start:
        log.info('`Start_dt` is earlier than the earliest start date of the assets. Adjusting start_dt to the earliest start date.')
        start_dt = min_start

    # fields
    if isinstance(fields, str):
        fields = [fields]

    if not fields:
        fields = OHLCVP_FIELDS_LIST + OHLCV_ADJ_FIELDS_LIST

    expect_element(fields = OHLCVP_FIELDS_LIST + OHLCV_ADJ_FIELDS_LIST)

    non_adj_field = [i for i in fields if i in OHLCVP_FIELDS_LIST]
    adj_field = [re.sub(r'_adj$', '', i) for i in fields if i in OHLCV_ADJ_FIELDS_LIST] # 結尾的adj要刪除

    # N_tradate： number of trading days
    dt = get_calendar(calendar_name).sessions_in_range(start_dt, end_dt)
    N_tradate = len(dt)

    bundle_price_adj = pd.DataFrame()
    if adj_field:
        bundle_price_adj = _get_history(True, adj_field, N_tradate, transform)

    bundle_price = pd.DataFrame()
    if non_adj_field:
        bundle_price = _get_history(False, non_adj_field, N_tradate, transform)

    # merge
    if not transform:
        df = pd.concat([bundle_price, bundle_price_adj], axis=1, join='outer').reset_index()
        # add：symbol and sid
        df.insert(2, 'symbol', df['asset'].apply(lambda x: x.symbol))
        df.insert(2, 'sid', df['asset'].apply(lambda x: x.sid))

    else:
        merged_dict = {**bundle_price, **bundle_price_adj}
        df = pd.concat(merged_dict.values(), axis=1, keys=merged_dict.keys())

    # prevent from PermissionError: [WinError 32]
    bundle.adjustment_reader.close()

    return df


def get_bundle_adj(bundle_name: str = 'tquant') -> pd.DataFrame:

    """
    Loads the adjustments from data bundle into a DataFrame.

    See `unpack_db_to_component_dfs` for more details.

    Parameters
    ----------
    bundle_name : str, optional
        The name of the bundle. Default is 'tquant'.

    Returns
    -------
    df : pd.DataFrame
        Returns a DataFrame containing all adjustments data for the bundle.
    """

    bundle = bundles.load(bundle_name) 

    # 取得所有的adjustment
    df_adjustment = bundle.adjustment_reader.unpack_db_to_component_dfs(convert_dates=True)

    # dividend_payouts
    df_dividend_payouts = df_adjustment['dividend_payouts'].rename(
        columns={'ex_date':'effective_date',
                'pay_date':'dividend_payouts.pay_date',
                'record_date':'dividend_payouts.record_date',
                'declared_date':'dividend_payouts.declared_date',
                'amount':'dividend_payouts.amount',
                #'cash_back':'dividend_payouts.cash_back',
                'div_percent':'dividend_payouts.div_percent'}
        ).set_index(['effective_date','sid'])

    # stock_dividend_payouts
    df_stock_dividend_payouts = df_adjustment['stock_dividend_payouts'].rename(
        columns={'ex_date':'effective_date',
                 'pay_date':'stock_dividend_payouts.pay_date',
                 'record_date':'stock_dividend_payouts.record_date',
                 'declared_date':'stock_dividend_payouts.declared_date',
                 'amount':'stock_dividend_payouts.amount'
        }).set_index(['effective_date','sid'])

    # splits
    df_splits = df_adjustment['splits'].rename(columns={'ratio':'splits.ratio'}).set_index(['effective_date','sid'])
    # mergers
    df_mergers = df_adjustment['mergers'].rename(columns={'ratio':'mergers.ratio'}).set_index(['effective_date','sid'])
    # dividends
    df_dividends = df_adjustment['dividends'].rename(columns={'ratio':'dividends.ratio'}).set_index(['effective_date','sid'])

    df = pd.concat([df_dividend_payouts, df_dividends, df_splits, df_mergers],axis=1,join='outer')

    df.index.names = ['date','sid']
    df = df.sort_index().reset_index()

    df.insert(2,'asset',df['sid'].apply(lambda x: bundle.asset_finder.retrieve_asset(x)))
    df.insert(2,'symbol',df['asset'].apply(lambda x: x.symbol))

    # prevent from PermissionError: [WinError 32]
    bundle.adjustment_reader.close() 

    return df


def get_bundle(
        start_dt: pd.Timestamp | str,
        end_dt: pd.Timestamp | str,
        bundle_name: str = 'tquant',
        calendar_name: str = 'TEJ_XTAI',
        frequency: str = '1d',
        data_frequency: str = 'daily',
        assets: list = None
    ) -> pd.DataFrame:
    """
    Loads both pre- and post-adjustment price and volume data, along with any adjustments,
    and converts them into a DataFrame.

    Parameters
    ----------
    start_dt: pandas.Timestamp | str
        The start of the desired window of data.
    end_dt: pandas.Timestamp | str
        The end session of the desired window of data.
    bundle_name : str, optional
        The name of the bundle. Default is 'tquant'.
    calendar_name : str, optional
       The name of a calendar used to align bundle data. Default is 'TEJ_XTAI'.
    frequency: string, optional
        The frequency of the data to query; i.e. '1d'. Default is '1d'.
    data_frequency: string, optional
        The frequency of the data to query; i.e. 'daily'. Default is 'daily'.
    assets : list of `zipline.data.Asset objects`, optional
        The assets whose data is desired.

    Returns
    -------
    df : pd.DataFrame
        Contains the bundle's price data (both pre- and post-adjustment) and
        the corresponding adjustment information.

    Raises
    ------
    IndexError
        Raised if the requested date range exceeds the available trading day coverage,
        or if the required `bar_count` surpasses the actual data length.

        For instance, this can occur when `start_dt` is earlier than the earliest available
        trading day or `end_dt` is beyond the available range.

        Please adjust `start_dt` (or choose a valid date range) to avoid this issue.
    """
    df_bundle_price = get_bundle_price(
        bundle_name=bundle_name,
        calendar_name=calendar_name,
        start_dt=start_dt,
        end_dt=end_dt,
        assets=assets,
        frequency=frequency,
        data_frequency=data_frequency).set_index(['date','sid'])

    df_bundle_adj = get_bundle_adj(bundle_name).set_index(['date','sid']).drop(columns=['asset','symbol'])

    df = df_bundle_price.join(df_bundle_adj, how='left').reset_index()

    return df


####################
# get_annotation   #
####################
def get_annotation(start_dt,
                   end_dt,
                   bundle_name='tquant',
                   calendar_name='TEJ_XTAI',
                   assets=None,
                   transform=False):
    """
    Export the bundle's annotation data(20231106)

    Parameters
    ----------
    start_dt: pandas.Timestamp
        The start of the desired window of data.
    end_dt: pandas.Timestamp
        The end session of the desired window of data.
    bundle_name : str, optional
        The name of the bundle.
    calendar_name : str, optional
        The name of a calendar used to align bundle data.
    fields: string or list[string], optional
        field : {'open', 'high', 'low', 'close', 'volume',
        'price', 'last_traded','annotation'}
        The desired field of the asset.
    assets : Asset or iterable of same, optional
        The asset or assets whose data is desired.
    transform : bool, optional
        If set to True, the DataFrame will be structured with
        'Asset' objects as columns and 'DatetimeIndex' as the index.

    Returns
    -------
    df : pd.DataFrame

    See Also
    --------
    :Zipline.data.bcolz_daily_bars(20230914)
    :Zipline.data.bundles.tquant(20230914)
    """
    bundle = bundles.load(bundle_name)

    if assets is None:
        sids = bundle.asset_finder.equities_sids
        assets = bundle.asset_finder.retrieve_all(sids)

    dt = get_calendar(calendar_name).sessions_in_range(start_dt, end_dt)

    portal = DataPortal(asset_finder=bundle.asset_finder,
                        trading_calendar=get_calendar(calendar_name),
                        first_trading_day=bundle.equity_daily_bar_reader.first_trading_day,
                        equity_daily_reader=bundle.equity_daily_bar_reader,
                        adjustment_reader=bundle.adjustment_reader)

    data = []
    for i in dt:
        _data = portal.get_spot_value(assets=assets,
                                      field='annotation',
                                      dt=i,
                                      data_frequency ='daily')
        data.append(_data)

    if type(assets)==str:
        assets = [assets]

    df = pd.DataFrame(columns=assets,  # [i.symbol for i in assets]
                      data=data,
                      index=[dt])

    if transform:
        return df

    else:

        df = df.stack().reset_index().rename(columns={'level_1':'asset',
                                                      'level_0':'date',
                                                      0:'annotation'})

        df['atten_fg'] = np.where(df['annotation'].str.contains("注意股票"), 'Y', '')
        df['disp_fg'] = np.where(df['annotation'].str.contains("處置股票"), 'Y', '')
        df['full_fg'] = np.where(df['annotation'].str.contains("全額交割股票"), 'Y', '')
        df['sbadt_fg'] = np.where(df['annotation'].str.contains("暫停當沖先賣後買"), 'Y', '')
        df['ssadt_fg'] = np.where(df['annotation'].str.contains("暫停當沖先買後賣"), 'Y', '')
        df['limo_fg'] = np.where(df['annotation'].str.contains("開盤即鎖死"), 'Y', '')

        df['limit_fg'] = ''
        df.loc[df['annotation'].str.contains("漲停股票",na=False), 'limit_fg'] = '+'
        df.loc[df['annotation'].str.contains("跌停股票",na=False), 'limit_fg'] = '-'

        df['mch_prd'] = 0.0
        df.loc[df['annotation'].str.contains("五分分盤處置",na=False), 'mch_prd'] = 5.0
        df.loc[df['annotation'].str.contains("二十分分盤處置",na=False), 'mch_prd'] = 20.0

        df.insert(1,'symbol',df['asset'].apply(lambda x: x.symbol))
        df.insert(1,'sid',df['asset'].apply(lambda x: x.sid))

        return df


####################
# get_fundamentals #
####################

registered_bundle = bundles.bundles.keys()  # the registered bundles
# Expose _bundles through a proxy so that users cannot mutate this
# accidentally. Users may go through `register` to update this which will
# warn when trampling another bundle.


def most_recent_data(bundle_name, timestamp, environ=None):
    """Get the path to the most recent data after ``date``for the
    given bundle.

    Parameters
    ----------
    bundle_name : str
        The name of the bundle to lookup.
    timestamp : datetime
        The timestamp to begin searching on or before.
    environ : dict, optional
        An environment dict to forward to zipline_root.
    """
    if bundle_name not in registered_bundle:
        raise UnknownBundle(bundle_name)

    try:
        candidates = os.listdir(
            pth.data_path([bundle_name], environ=environ),
        )
        return pth.data_path(
            [
                bundle_name,
                max(
                    filter(complement(pth.hidden), candidates),
                    key=from_bundle_ingest_dirname,
                ),
            ],
            environ=environ,
        )
    except (ValueError, OSError) as e:
        if getattr(e, "errno", errno.ENOENT) != errno.ENOENT:
            raise
        raise ValueError(
            "no data for bundle {bundle!r} on or before {timestamp}\n"
            "maybe you need to run: $ zipline ingest -b {bundle}".format(
                bundle=bundle_name,
                timestamp=timestamp,
            ),
        )

## 20231211 HJK 新增slice method: MRQ, MRA, Daily
_dt_to_period = [
'MRQ',
'MRA',
'Daily',
]

SUPPORTED_SLICE_FREQUENCIES = frozenset(_dt_to_period)


expect_slice_frequency = expect_element(
    frequency=SUPPORTED_SLICE_FREQUENCIES,
)
from abc import ABC, abstractmethod
from zipline.data.bundles.fundamentals import get_cached_columns


class TQDataLoader(ABC):
    @abstractmethod
    def get_sqlite_script(self, fields):
        pass


    @abstractmethod
    def retrieve_data(self, 
                      fields:list = None,
                    frequency = 'Daily',
                    start_dt = '2013-01-01', 
                    end_dt = pd.Timestamp.utcnow(),
                    period_offset = 0,
                    dataframeloaders = False):
        pass

class TQAltDataLoader(TQDataLoader):
    def __init__(self) -> None:
        pass

    def create_index(self, connection, table_name = 'factor_table'):
        check_index = f'''
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'index'
        AND tbl_name = '{table_name}';
        '''
        # Use a context manager for the database connection
        # with sqlite3.connect(fundamentals_path) as conn:
        index_data = pd.read_sql(check_index, connection)
        # 建立 cursor
        cursor = connection.cursor()
        if len(index_data) == 0:
            c_index_scripts = f'''
                CREATE INDEX fin_index ON {table_name} (symbol, date);
            '''
            cursor.execute(c_index_scripts)
            # conn.close()

    def process_fields(self, fields:list):
        default_fields = ['symbol', 'date']

        if not fields:
            return ','.join(default_fields)
        
        processed_fields = list(set(fields+default_fields))
        return ','.join(processed_fields)

    def get_sqlite_script(self, fields=None, peroid_offset = 0):

        scripts = f'''SELECT {self.fields}
                    FROM factor_table a
                    where a.date >= "{self.start_dt}" and strftime('%Y-%m-%d',a.date) <= "{self.end_dt}"
                '''
        return scripts
    
    
    def retrieve_data(self, 
                      fields: list = None, 
                      frequency='Daily', 
                      start_dt='2013-01-01', 
                      end_dt=pd.Timestamp.utcnow(), 
                      period_offset=0, 
                      dataframeloaders=False):
        
        # transfer list to string
        self.fields = self.process_fields(fields=fields)

        # Adjust datetime format
        self.start_dt =  pd.to_datetime(start_dt).strftime('%Y-%m-%d')
        self.end_dt = pd.to_datetime(end_dt).strftime('%Y-%m-%d')
        self.dataframeloaders = dataframeloaders
        
        # get sql scripts
        scripts = self.get_sqlite_script()
        timestamp = pd.Timestamp.utcnow()
        bundle_path = most_recent_data('fundamentals', timestamp)
        fundamentals_path = f'{bundle_path}/factor_table.db'

        # Use a context manager for the database connection
        with sqlite3.connect(fundamentals_path) as conn:
            # Create index for the table
            self.create_index(conn)
            data = pd.read_sql(scripts, conn, parse_dates=["fin_date", "mon_sales_date", "share_date"])
        
        # if fields == '*':
        #     filled_columns = data.columns.tolist() 
        #     filled_columns = [i for i in filled_columns if i != 'symbol']
        #     # print(filled_columns)
        # else:
        #     filled_columns = set(fields)
        #     filled_columns.update(['fin_date', 'lag_fin_date'])
        #     filled_columns =  list(filled_columns)
        
        data = data.groupby('symbol', group_keys=False).apply(dask_api.fillna_multicolumns)


        if self.dataframeloaders:
            bundle_name = 'tquant'
            tquant = bundles.load(bundle_name)
            sids = tquant.asset_finder.equities_sids
            assets = tquant.asset_finder.retrieve_all(sids)
            symbol_mapping_sid = {i.symbol:i.sid for i in assets}
            data.date = pd.to_datetime(data.date, utc =True)
            # Drop duplicated rows
            data = data.drop_duplicates(subset=['symbol', 'date'], keep='last')
            data = data.set_index(['symbol', 'date']).unstack('symbol')
            data = data.rename(columns = symbol_mapping_sid)
        
        conn.close()
        
        return data
        

class TQWSDataLoader(TQDataLoader):
    pass



class TQFDataLoader(TQDataLoader):

    def __init__(self, 
                #  frequency = 'Daily', 
                 **kwargs
                 ) -> None:
        
        self.kwargs = kwargs
        
    def process_fields(self, fields):
        if fields == '*':
            ts = pd.Timestamp.now()
            ms = most_recent_data(bundle_name='fundamentals', timestamp=ts)
            processed_fields = get_cached_columns(ms)
            processed_fields = ['a.'+i for i in processed_fields]
            processed_fields = ', '.join(processed_fields)
            # 移除重複的 fin_date
            processed_fields = processed_fields.replace(', a.fin_date', '')  
            return processed_fields
        
        specific_fields = ['symbol', 'date', 'fin_order', 'lag_fin_date', 'fin_date']
        processed_fields = []

        for field in fields:
            if field not in specific_fields:
                processed_fields.append(f'a.{field}')

        processed_fields = ['a.symbol', 'a.date'] + processed_fields
        processed_fields = list(set(processed_fields))

        return ', '.join(processed_fields)

    def get_sqlite_script(self, frequency = 'Daily' ,peroid_offset = 0):
        if peroid_offset != 0:
            if frequency == 'Daily':
                scripts = f'''SELECT {self.fields}, a.fin_date,
                ROW_NUMBER() OVER (PARTITION BY a.symbol, a.fin_date ORDER BY a.date) AS fin_order,
                    datetime(a.date, '{peroid_offset} days') as lag_fin_date 
                    FROM factor_table a
                    left join factor_table b on a.symbol = b.symbol and datetime(a.date, '{peroid_offset} days') = b.fin_date
                    where a.date >= "{self.start_dt}" and strftime('%Y-%m-%d',a.date) <= "{self.end_dt}"
                '''

            elif frequency == 'MRA':
                fields = set(self.fields.split(', ')) - set(['a.symbol', 'a.date', 'a.fin_order', 'a.lag_fin_date'])
                fields = [field.replace('a.','b.') for field in fields if 'a.' in field]
                fields = ','.join(fields)
                if fields:
                    fields = ','+fields
                others = fields.replace('b.','a.')
                scripts = f'''
                with first_announce as (
                select symbol, date, fin_date, fin_order, datetime(fin_date, '{peroid_offset} years') as lag_fin_date
                    from (
                        select symbol, fin_date, date,
                        row_number () over (PARTITION by symbol, fin_date order by date) as fin_order
                        from factor_table
                    )tmp
                    where fin_date is not NULL and fin_order = 1 and strftime('%m', fin_date)='12'
                    
                ), 
                samll_factor_table as (
                    select a.symbol, a.fin_date, b.lag_fin_date, b.fin_order {others}
                    from factor_table a
                    inner join first_announce b on a.date = b.date and a.symbol = b.symbol 
                ),
                year_fin_data as (
                    select a.symbol, a.date, a.fin_date, a.lag_fin_date, a.fin_order {fields}
                    from  first_announce a
                    left join samll_factor_table b on a.symbol = b.symbol and a.lag_fin_date = b.fin_date

                )

                select a.symbol, a.date, b.fin_date, b.lag_fin_date, b.fin_order {fields}
                from factor_table a
                left join year_fin_data b on a.symbol = b.symbol and a.date = b.date
                where a.date >= '{self.start_dt}' and strftime('%Y-%m-%d',a.date) <= '{self.end_dt}'
                '''
                

            else: 
                fields = set(self.fields.split(', ')) - set(['a.symbol', 'a.date', 'a.fin_order', 'a.lag_fin_date'])
                fields = [field.replace('a.','b.') for field in fields if 'a.' in field]
                fields = ','.join(fields)
                if fields:
                    fields = ','+fields
                others = fields.replace('b.','a.')
                scripts = f'''
                with first_announce as (
                select symbol, date, fin_date, fin_order, datetime(fin_date, '{peroid_offset*3} months') as lag_fin_date
                    from (
                        select symbol, fin_date, date,
                        row_number () over (PARTITION by symbol, fin_date order by date) as fin_order
                        from factor_table
                    )tmp
                    where fin_date is not NULL and fin_order = 1
                    
                ), 
                samll_factor_table as (
                    select a.symbol, a.fin_date, b.lag_fin_date, b.fin_order {others}
                    from factor_table a
                    inner join first_announce b on a.date = b.date and a.symbol = b.symbol 
                ),
                month_fin_data as (
                    select a.symbol, a.date, a.fin_date, a.lag_fin_date, a.fin_order {fields}
                    from  first_announce a
                    left join samll_factor_table b on a.symbol = b.symbol and a.lag_fin_date = b.fin_date

                )

                select a.symbol, a.date, b.fin_date, b.lag_fin_date, b.fin_order {fields}
                from  factor_table a
                left join month_fin_data b on a.symbol = b.symbol and a.date = b.date
                where a.date >= '{self.start_dt}' and strftime('%Y-%m-%d',a.date) <= '{self.end_dt}'
                '''
        else:
            # if frequency == 'Daily':
            scripts = f'''SELECT {self.fields}, a.fin_date,
            ROW_NUMBER() OVER (PARTITION BY a.symbol, a.fin_date ORDER BY a.date) AS fin_order,
                datetime(a.date, '{peroid_offset} days') as lag_fin_date 
                FROM factor_table a
                where a.date >= "{self.start_dt}" and strftime('%Y-%m-%d',a.date) <= "{self.end_dt}"
            '''
        return scripts


    def create_index(self, connection, table_name = 'factor_table'):
        check_index = f'''
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'index'
        AND tbl_name = '{table_name}';
        '''
        # Use a context manager for the database connection
        # with sqlite3.connect(fundamentals_path) as conn:
        index_data = pd.read_sql(check_index, connection)
        # 建立 cursor
        cursor = connection.cursor()
        if len(index_data) == 0:
            c_index_scripts = f'''
                CREATE INDEX fin_index ON {table_name} (symbol, date);
            '''
            cursor.execute(c_index_scripts)
            # conn.close()

    @expect_slice_frequency
    def retrieve_data(self, 
                    fields:list = '*',
                    frequency = 'Daily',
                    start_dt = '2013-01-01', 
                    end_dt = pd.Timestamp.utcnow(),
                    period_offset = 0,
                    dataframeloaders = False):
        
        # transfer list to string
        self.fields = self.process_fields(fields=fields)

        # Adjust datetime format
        self.start_dt =  pd.to_datetime(start_dt).strftime('%Y-%m-%d')
        self.end_dt = pd.to_datetime(end_dt).strftime('%Y-%m-%d')
        self.dataframeloaders = dataframeloaders
        
        # get sql scripts
        scripts = self.get_sqlite_script(frequency = frequency, peroid_offset=period_offset)
        timestamp = pd.Timestamp.utcnow()
        bundle_path = most_recent_data('fundamentals', timestamp)
        fundamentals_path = f'{bundle_path}/factor_table.db'

        # Use a context manager for the database connection
        with sqlite3.connect(fundamentals_path) as conn:
            # Create index for the table
            self.create_index(conn)
            data = pd.read_sql(scripts, conn, parse_dates=["fin_date", "mon_sales_date", "share_date", 'lag_fin_date'])
        
        if fields == '*':
            filled_columns = data.columns.tolist() 
            filled_columns = [i for i in filled_columns if i != 'symbol']
            # print(filled_columns)
        else:
            filled_columns = set(fields)
            filled_columns.update(['fin_date', 'lag_fin_date'])
            filled_columns =  list(filled_columns)
        
        data = data.groupby('symbol', group_keys=False).apply(dask_api.fillna_multicolumns)


        if self.dataframeloaders:
            bundle_name = 'tquant'
            tquant = bundles.load(bundle_name)
            sids = tquant.asset_finder.equities_sids
            assets = tquant.asset_finder.retrieve_all(sids)
            symbol_mapping_sid = {i.symbol:i.sid for i in assets}
            data.date = pd.to_datetime(data.date, utc =True)
            # Drop duplicated rows
            data = data.drop_duplicates(subset=['symbol', 'date'], keep='last')
            data = data.set_index(['symbol', 'date']).unstack('symbol')
            data = data.rename(columns = symbol_mapping_sid)

        conn.close()

        return data

# @expect_slice_frequency
def get_fundamentals(bundle_name = 'fundamentals',
                     fields:list = None,
                     start_dt = '2013-01-01' ,
                     end_dt = pd.Timestamp.utcnow(),
                     assets=None,
                     dataframeloaders = False,
                    #  frequency:str = 'Daily',
                    #  period_offset:int = 0
                     ):
    
    def process_fields(fields):
        # Remove duplicates and exclude certain fields, if needed
        unique_fields = set(fields) - {'symbol', 'date', '*'}
        return ', '.join(unique_fields)

    
    if 'tquant' not in registered_bundle:
        raise ValueError('tquant尚未註冊，請先執行 !zipline ingest -b tquant')
    
    # Specify the full path to the database file
    start_dt =  pd.to_datetime(start_dt).strftime('%Y-%m-%d')
    end_dt = pd.to_datetime(end_dt).strftime('%Y-%m-%d')
    timestamp = pd.Timestamp.utcnow()
    bundle_path = most_recent_data(bundle_name, timestamp)
    db_path = f'{bundle_path}/factor_table.db'
    if not fields:
        scripts =  f'''
        select * from factor_table
        where date >= '{start_dt}' and  strftime('%Y-%m-%d', date) <= '{end_dt}'
        '''
    else:
        processed_fields = process_fields(fields)
        scripts =  f'''
        select symbol, date, {processed_fields} from factor_table
        where date >= '{start_dt}' and  strftime('%Y-%m-%d', date) <= '{end_dt}'
        '''
    
    with sqlite3.connect(db_path) as conn:
        # Create index for the table
        data = pd.read_sql(scripts, conn, parse_dates=["fin_date", "mon_sales_date", "share_date"])

    conn.close()
    return data
