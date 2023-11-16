from zipline.pipeline.data import TQDataSet, EquityPricing
from zipline.pipeline import SimplePipelineEngine
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.pipeline.domain import TW_EQUITIES
from zipline.data import bundles
from zipline.pipeline.loaders import TQuantFundamentalsPipelineLoader
import os


bundle_name = 'tquant'
bundle_data  = bundles.load(bundle_name)

pricing_loader = EquityPricingLoader.without_fx(bundle_data.equity_daily_bar_reader,
                                                bundle_data.adjustment_reader)
sids = bundle_data.asset_finder.equities_sids
assets = bundle_data.asset_finder.retrieve_all(sids)
symbol_mapping_sid = {i.symbol:i.sid for i in assets}

freq = os.environ.get('frequency')
# print(freq)
TQuantPipelineLoader = TQuantFundamentalsPipelineLoader(zipline_sids_to_real_sids=symbol_mapping_sid, frequency=freq)



def choose_loader(column):
    if column.name in EquityPricing._column_names:
        return pricing_loader
    
    elif column.name in TQDataSet._column_names:     
        return TQuantPipelineLoader
    else:
        raise Exception('Column not available')
        


def run_pipeline(*args, **kwargs):
    engine = SimplePipelineEngine(get_loader = choose_loader,
                            asset_finder = bundle_data.asset_finder,
                            default_domain = TW_EQUITIES)
    
    return engine.run_pipeline(*args, **kwargs)