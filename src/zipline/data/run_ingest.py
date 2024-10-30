import os
from typing import Union , List
from TejToolAPI import get_api_key_info
def simple_ingest (
        name : str  ,
        tickers : Union[List[str] , str] , 
        start_date : str , 
        end_date : str , 
        fields : Union[List[str] , str] = None , 
        self_acc : str = 'N', 
        ) :
    """
    name : `tquant` or `fundamentals`.

    ticker : a list of tickers or one ticker.

    start_date : format : YYYYMMDD.

    end_date : format : YYYYMMDD.

    fields : [fundamentals only] the fields that fundamentals column need.

    self_acc : [fundamentals only] `Y` to include the financial data company computed by themselves.
    """
    mdate = str(start_date)+' '+str(end_date)
    if isinstance(tickers , list) :
        if not all(isinstance(i ,str) for i in tickers) :
            tickers = list(map(lambda x: str(x), tickers))
        tickers = ' '.join(tickers)
    os.environ['mdate'] = mdate
    os.environ['ticker'] = tickers
    if fields is not None :
        if isinstance(fields , list) :
            fields = ' '.join(fields)
        os.environ['fields'] = fields
    if self_acc == 'Y' :
        os.environ['include_self_acc'] = 'Y'
    print("Now ingesting data.")
    if name == 'tquant' :
        os.system('zipline ingest -b tquant')
        print(f"End of ingesting {name}.")
        print(f"Please call function `get_bundle(start_dt = pd.Timestamp('{str(start_date)}', tz = 'utc'),end_dt = pd.Timestamp('{str(end_date)}' ,tz = 'utc'))` in `zipline.data.data_portal` to check data.")
    elif name == 'fundamentals' :
        os.system('zipline ingest -b fundamentals')
        print(f"End of ingesting {name}.")
        print("Please call function `get_fundamentals()` in `zipline.data.data_portal` to check data.")
    get_api_key_info()