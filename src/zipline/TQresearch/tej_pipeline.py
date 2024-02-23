from zipline.pipeline.data import EquityPricing
from zipline.pipeline import SimplePipelineEngine
from zipline.pipeline.data import EquityPricing
from zipline.pipeline.loaders import EquityPricingLoader
from zipline.pipeline.domain import TW_EQUITIES
from zipline.data import bundles
from zipline.pipeline.loaders.router import TQuantLabPipelineLoaderRouter
from zipline.utils.calendar_utils import get_calendar


def run_pipeline(*args, **kwargs):

    bundle_name = 'tquant'
    bundle_data  = bundles.load(bundle_name)

    pricing_loader = EquityPricingLoader.without_fx(bundle_data.equity_daily_bar_reader,
                                                    bundle_data.adjustment_reader)
    sids = bundle_data.asset_finder.equities_sids
    assets = bundle_data.asset_finder.retrieve_all(sids)
    symbol_mapping_sid = {i.symbol:i.sid for i in assets}

    calendar_name = bundles.bundles[bundle_name].calendar_name
    exchange_calendar = get_calendar(calendar_name)
    pipeline_loader = TQuantLabPipelineLoaderRouter(
        sids_to_real_sids = symbol_mapping_sid,
        calendar = exchange_calendar,
        default_loader = pricing_loader,
        default_loader_columns=EquityPricing.columns

    )

    # bundle_data.adjustment_reader.close() # 會導致ProgrammingError: Cannot operate on a closed database.

    engine = SimplePipelineEngine(get_loader = pipeline_loader,
                            asset_finder = bundle_data.asset_finder,
                            default_domain = TW_EQUITIES)

    return engine.run_pipeline(*args, **kwargs)