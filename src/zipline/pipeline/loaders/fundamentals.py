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
    isnull
)

from .utils import shift_dates

from zipline.pipeline.loaders.missing import MISSING_VALUES_BY_DTYPE
from zipline.data.data_portal import (get_fundamentals,
                                      TQFDataLoader,
                                      TQAltDataLoader)

from zipline.pipeline.loaders.frame import DataFrameLoader


# ADJUSTMENT_COLUMNS = Index(
#     [
#         "sid",
#         "value",
#         "kind",
#         "start_date",
#         "end_date",
#         "apply_date",
#     ]
# )

class TQuantFundamentalsPipelineLoader(implements(PipelineLoader)):
    ## 20230829 created by HRK

    def __init__(self, 
                 zipline_sids_to_real_sids, 
                 adjustments=None, 
                #  frequency = 'Daily'
                 ):
            
            self.zipline_sids_to_real_sids = zipline_sids_to_real_sids
            # frequency: Daily, MRA(Most Recent Annual), MRQ(Most Recent Quarter)
            self.loader = TQFDataLoader()


    def load_adjusted_array(self, domain, columns, dates, sids, mask, **kwargs):
        
        sessions = domain.all_sessions()
        shifted_dates = shift_dates(sessions, dates[0], dates[-1], shift=1)
        reindex_like = pd.DataFrame(None, index=shifted_dates, columns=sids)

        out = {}
        pivot = kwargs.get('dataframeloaders', True)

        
        # 20231208 HJK
        # group columns by dimension, period_offset, and dtype, to make
        # different calls to self.loader.retrieve_data
        # for each column group
        column_groups = defaultdict(list)
        for column in columns:
            if 'frequency' in column.dataset.extra_coords:
                frequency = column.dataset.extra_coords["frequency"]
            else:
                frequency = 'Daily'

            if 'period_offset' in column.dataset.extra_coords:
                period_offset = column.dataset.extra_coords["period_offset"]
            else:
                period_offset = 0

            dtype = column.dtype
            column_groups[(frequency, period_offset, dtype)].append(column)
        

        for (frequency, period_offset, _), columns in column_groups.items():
            fields = list({c.name for c in columns})

            self.frequency = frequency
            fundamentals = self.loader.retrieve_data(fields = fields,
                                                    frequency = frequency,
                                                    period_offset = period_offset,
                                                    start_dt = shifted_dates[0],
                                                    end_dt = shifted_dates[-1],
                                                    dataframeloaders = True)

            for column in columns:
                missing_value = MISSING_VALUES_BY_DTYPE[column.dtype]
                if fundamentals is not None:
                    fundamentals_for_column = DataFrameLoader(column=column, baseline=fundamentals[column.name])

                else:
                    fundamentals_for_column = DataFrameLoader(column=column, baseline=reindex_like)

           
                # Utilize DataframLoader.load_adjusted_array function to incorporate fundamental data.
                out[column] = fundamentals_for_column.load_adjusted_array(domain, [column], shifted_dates, sids, mask)[column]

        return out


class TQuantAlternativesPipelineLoader(implements(PipelineLoader)):
    ## 20240124 created by HRK

    def __init__(self, 
                 zipline_sids_to_real_sids, 
                 adjustments=None, 
                 ):
            
            self.zipline_sids_to_real_sids = zipline_sids_to_real_sids
            self.loader = TQAltDataLoader()


    def load_adjusted_array(self, domain, columns, dates, sids, mask, **kwargs):
        
        sessions = domain.all_sessions()
        shifted_dates = shift_dates(sessions, dates[0], dates[-1], shift=1)
        reindex_like = pd.DataFrame(None, index=shifted_dates, columns=sids)

        out = {}
        pivot = kwargs.get('dataframeloaders', True)

        
        # 20240125 HJK
        # group columns by dimension, period_offset, and dtype, to make
        # different calls to self.loader.retrieve_data
        # for each column group
        column_groups = defaultdict(list)
        for column in columns:
            if 'period_offset' in column.dataset.extra_coords:
                period_offset = column.dataset.extra_coords["period_offset"]
            else:
                period_offset = 0

            dtype = column.dtype
            # column_groups[(frequency, period_offset, dtype)].append(column)
            column_groups[(period_offset, dtype)].append(column)
        

        for (period_offset, _), columns in column_groups.items():
            fields = list({c.name for c in columns})

            # self.frequency = frequency
            alternatives = self.loader.retrieve_data(fields = fields,
                                                    frequency = 'Daily',
                                                    period_offset = 0,
                                                    start_dt = shifted_dates[0],
                                                    end_dt = shifted_dates[-1],
                                                    dataframeloaders = True)

            for column in columns:
                missing_value = MISSING_VALUES_BY_DTYPE[column.dtype]
                if alternatives is not None:
                    alternatives_for_column = DataFrameLoader(column=column, baseline=alternatives[column.name])

                else:
                    alternatives_for_column = DataFrameLoader(column=column, baseline=reindex_like)

                # Utilize DataframLoader.load_adjusted_array function to incorporate fundamental data.
                out[column] = alternatives_for_column.load_adjusted_array(domain, [column], shifted_dates, sids, mask)[column]

        return out