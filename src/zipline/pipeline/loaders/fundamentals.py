from interface import implements
from collections import defaultdict
import pandas as pd
from zipline.pipeline.loaders.base import PipelineLoader
from zipline.lib.adjusted_array import AdjustedArray
from functools import partial
from zipline.lib.adjustment import make_adjustment_from_labels
from numpy import (
    ix_,
    zeros,
)
from pandas import (
    DataFrame,
    DatetimeIndex,
    Index,
    Int64Index,
    isnull
)

from .utils import shift_dates

from zipline.pipeline.loaders.missing import MISSING_VALUES_BY_DTYPE
from zipline.data.data_portal import (get_fundamentals,
                                      TQFDataLoader)

from zipline.pipeline.loaders.frame import DataFrameLoader


ADJUSTMENT_COLUMNS = Index(
    [
        "sid",
        "value",
        "kind",
        "start_date",
        "end_date",
        "apply_date",
    ]
)

class TQuantFundamentalsPipelineLoader(implements(PipelineLoader)):
    ## 20230829 created by HRK

    def __init__(self, zipline_sids_to_real_sids, adjustments=None, frequency = 'Daily'):
            
            self.zipline_sids_to_real_sids = zipline_sids_to_real_sids
            # frequency: Daily, MRA(Most Recent Annual), MRQ(Most Recent Quarter)
            self.frequency = frequency
            if frequency in ['Daily', 'MRQ', 'MRA']:
                self.frequency = frequency
            else:
                self.frequency = 'Daily'

            self.loader = TQFDataLoader(frequency = self.frequency)


    def load_adjusted_array(self, domain, columns, dates, sids, mask, **kwargs):
        
        sessions = domain.all_sessions()
        shifted_dates = shift_dates(sessions, dates[0], dates[-1], shift=1)
        reindex_like = pd.DataFrame(None, index=shifted_dates, columns=sids)

        out = {}
        pivot = kwargs.get('dataframeloaders', True)

        fields = [c.name for c in columns] + ['symbol', 'date']

        fundamentals = self.loader.retrieve_data(fields = fields,
                                                start_dt = shifted_dates[0],
                                                end_dt = shifted_dates[-1],
                                                dataframeloaders = pivot)
        
        # print(fundamentals.info())
        

        for column in columns:
            # missing_value = MISSING_VALUES_BY_DTYPE[column.dtype]

            if fundamentals is not None:
                fundamentals_for_column = DataFrameLoader(column=column, baseline=fundamentals[column.name])

            else:
                fundamentals_for_column = DataFrameLoader(column=column, baseline=reindex_like)

            # out[column] = AdjustedArray(
            #     data=fundamentals_for_column.astype(column.dtype).fillna(missing_value).values,
            #     adjustments=self.format_adjustments(dates, sids),
            #     missing_value=missing_value
            # )
            out[column] = fundamentals_for_column.load_adjusted_array(domain, [column], shifted_dates, sids, mask)[column]

        return out

    
    def format_adjustments(self, dates, assets):
        """
        Build a dict of Adjustment objects in the format expected by
        AdjustedArray.

        Returns a dict of the form:
        {
            # Integer index into `dates` for the date on which we should
            # apply the list of adjustments.
            1 : [
                Float64Multiply(first_row=2, last_row=4, col=3, value=0.5),
                Float64Overwrite(first_row=3, last_row=5, col=1, value=2.0),
                ...
            ],
            ...
        }
        """
        make_adjustment = partial(make_adjustment_from_labels, dates, assets)

        min_date, max_date = dates[[0, -1]]
        # TODO: Consider porting this to Cython.
        if len(self.adjustments) == 0:
            return {}

        # Mask for adjustments whose apply_dates are in the requested window of
        # dates.
        date_bounds = self.adjustment_apply_dates.slice_indexer(
            min_date,
            max_date,
        )
        dates_filter = zeros(len(self.adjustments), dtype="bool")
        dates_filter[date_bounds] = True
        # Ignore adjustments whose apply_date is in range, but whose end_date
        # is out of range.
        dates_filter &= self.adjustment_end_dates >= min_date

        # Mask for adjustments whose sids are in the requested assets.
        sids_filter = self.adjustment_sids.isin(assets.values)

        adjustments_to_use = self.adjustments.loc[dates_filter & sids_filter].set_index(
            "apply_date"
        )

        # For each apply_date on which we have an adjustment, compute
        # the integer index of that adjustment's apply_date in `dates`.
        # Then build a list of Adjustment objects for that apply_date.
        # This logic relies on the sorting applied on the previous line.
        out = {}
        previous_apply_date = object()
        for row in adjustments_to_use.itertuples():
            # This expansion depends on the ordering of the DataFrame columns,
            # defined above.
            apply_date, sid, value, kind, start_date, end_date = row
            if apply_date != previous_apply_date:
                # Get the next apply date if no exact match.
                row_loc = dates.get_loc(apply_date, method="bfill")
                current_date_adjustments = out[row_loc] = []
                previous_apply_date = apply_date

            # Look up the approprate Adjustment constructor based on the value
            # of `kind`.
            current_date_adjustments.append(
                make_adjustment(start_date, end_date, sid, kind, value)
            )
        return out
