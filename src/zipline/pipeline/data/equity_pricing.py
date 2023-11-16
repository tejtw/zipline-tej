"""
Dataset representing OHLCV data.
"""
from zipline.utils.numpy_utils import float64_dtype, categorical_dtype

from ..domain import US_EQUITIES,TW_EQUITIES
from .dataset import Column, DataSet


class EquityPricing(DataSet):
    """
    :class:`~zipline.pipeline.data.DataSet` containing daily trading prices and
    volumes.
    """    

    open = Column(float64_dtype, currency_aware=True)
    high = Column(float64_dtype, currency_aware=True)
    low = Column(float64_dtype, currency_aware=True)
    close = Column(float64_dtype, currency_aware=True)
    volume = Column(float64_dtype)
    currency = Column(categorical_dtype)
    
    
# Backwards compat alias.
USEquityPricing = EquityPricing.specialize(TW_EQUITIES)   #20230217 (by MRC) 
TWEquityPricing = EquityPricing.specialize(TW_EQUITIES)