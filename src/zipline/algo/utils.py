import pandas  as pd


def check_tradeday(func, argname, arg):
#     None
    if isinstance(arg, type(None)):
        return arg

#     DatetimeIndex
    if isinstance(arg, pd.DatetimeIndex):
        return arg.strftime('%Y-%m-%d').tolist()
#     Timestamp
    elif isinstance(arg, list) and all(isinstance(x, pd.Timestamp) for x in arg):
        return [ts.strftime('%Y-%m-%d') for ts in arg]
#     str
    elif isinstance(arg, list) and all(isinstance(x, str) for x in arg):
        return arg
    else:
        raise TypeError(
            "%s() expected argument '%s' to be of type None, DatetimeIndex,"
            " list of Timestamps, or list of strings. Received type %s instead." 
            % (
                func.__name__, argname, type(arg).__name__,
            )
        )
