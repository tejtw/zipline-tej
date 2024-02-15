import tejapi
import os
import pandas as pd
from logbook import Logger

log = Logger(__name__)

tejapi.ApiConfig.api_key = os.environ.get("TEJAPI_KEY")
tejapi.ApiConfig.api_base = os.environ.get("TEJAPI_BASE")

def get_api_key_info():
    info = tejapi.ApiConfig.info()
    call_day_limit = info["reqDayLimit"]
    data_day_limit = info["rowsDayLimit"]
    call_day_used = info["todayReqCount"]
    data_day_used = info["todayRows"]
    
    usage_percent = round((100 * call_day_used/call_day_limit),2)
    date_percent = round((100 * data_day_used/data_day_limit), 2)

    print(f"Currently used TEJ API key call quota {call_day_used}/{call_day_limit} ({usage_percent}%)")
    print(f"Currently used TEJ API key data quota {data_day_used}/{data_day_limit} ({date_percent}%)")
