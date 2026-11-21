import pandas as pd

from zipline.data.bundles.tquant import gen_asset_metadata


def test_missing_stock_names_preserve_equities_and_date_ranges():
    dates = pd.date_range("2018-01-02", periods=2)
    prices = pd.DataFrame({
        "symbol": ["1101", "1101", "2330", "2330", "6208", "6208"],
        "stk_name": [None, None, None, "TSMC", "Example", "Example"],
        "date": list(dates) * 3,
    })

    assets = gen_asset_metadata(prices, show_progress=False).set_index("symbol")
    assert assets.index.is_unique
    assert assets["asset_name"].to_dict() == {
        "1101": "1101", "2330": "TSMC", "6208": "Example",
    }
    assert (assets["start_date"] == dates[0]).all()
    assert (assets["end_date"] == dates[1]).all()
