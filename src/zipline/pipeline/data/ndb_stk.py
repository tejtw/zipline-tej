"""
Dataset representing OHLCV data.
"""
from zipline.utils.numpy_utils import float64_dtype, categorical_dtype
from zipline.pipeline.data import Column, DataSet, DataSetFamily
from zipline.pipeline.domain import Domain,TW_EQUITIES
import os 
from typing import Literal
import json



class NDBStkData(DataSetFamily):
    """
    :class:`~zipline.pipeline.data.DataSet` containing daily trading prices and
    volumes.
    """    
    domain = TW_EQUITIES
    symbol = Column(object)
    date = Column('datetime64[ns]')
    open = Column(float64_dtype, currency_aware=True)
    high = Column(float64_dtype, currency_aware=True)
    low = Column(float64_dtype, currency_aware=True)
    close = Column(float64_dtype, currency_aware=True)
    zclose = Column(float64_dtype, currency_aware=True)
    vol = Column(float64_dtype)
    tradingamt  = Column(float64_dtype)
    amt  = Column(float64_dtype)
    
    extra_dims = [
            ('frequency', {'Daily'}),
            ('period_offset', set(range(-127,1))),
        ]
    
    @classmethod
    def slice(
        cls,
        frequency: Literal['Daily'],
        period_offset: int = 0

    )->'NDBStkData':


        return super().slice(frequency=frequency, period_offset=period_offset)

NDBStk = NDBStkData.slice(frequency='Daily', period_offset=0)
