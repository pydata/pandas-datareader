import os
from pandas import (DataFrame, Panel)
from pandas.io.common import urlencode
from pandas_datareader.utils import _retry_read_url
from pandas_datareader.utils import _sanitize_dates
from pandas_datareader.utils import _get_data_from
from pandas_datareader.utils import _calc_return_index
from pandas_datareader.utils import _adjust_prices

_URL = 'https://www.quandl.com/api/v3/datasets/'
__API_KEY = None


def set_api_key(api_key):
    global __API_KEY
    __API_KEY = api_key

def _get_data(symbols=None, start=None, end=None, retry_count=3, pause=0.001,
              adjust_price=False, ret_index=False, chunksize=25,
              interval=None):
    """
    Returns DataFrame/Panel of historical stock prices from symbols, over date
    range, start to end. To avoid being penalized by Quandl servers,
    pauses between downloading 'chunks' of symbols can be specified, or
    if you're a registered user, you can set the environment variable
    QUANDL_API_KEY to your api key.

    Parameters
    ----------
    symbols : string, array-like object (list, tuple, Series), or DataFrame
        Single stock symbol (ticker), array-like object of symbols or
        DataFrame with index containing stock symbols.
    start : string, (defaults to '1/1/2010')
        Starting date, timestamp. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : string, (defaults to today)
        Ending date, timestamp. Same format as starting date.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.001
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    adjust_price : bool, default False
        If True, adjusts all prices in hist_data ('Open', 'High', 'Low',
        'Close') based on 'Adj Close' price. Adds 'Adj_Ratio' column and drops
        'Adj Close'.
    ret_index : bool, default False
        If True, includes a simple return index 'Ret_Index' in hist_data.
    chunksize : int, default 25
        Number of symbols to download consecutively before intiating pause.
    interval : string, default 'none' (do not alter data)
        The frequency of the data to return. Valid values are 'none', 'daily',
        'weekly', 'monthly', 'quarterly' or 'annual'
        Note: When you change the frequency of a dataset, Quandl returns the
        last observation for the given period. So, if you collapse a daily
        dataset to monthly, you will get a sample of the original dataset
        where the observation for each month is the last data point available
        for that month. Thus this transformation does not work well for
        datasets that measure percentage changes, period averages or period
        extremes (highs and lows).

    Returns
    -------
    hist_data : DataFrame (str) or Panel (array-like object, DataFrame)
    """
    interval = interval or 'none'

    valid_intervals = ['none', 'daily', 'weekly', 'monthly', 'quarterly', 'annual']
    if interval not in valid_intervals:
        raise ValueError('Invalid interval: valid values are %s' % ', '.join(valid_intervals))

    hist_data = _get_data_from(symbols, start, end, interval, retry_count,
                               pause, chunksize, _get_data_one)

    column_renames = {'Adj. Open': 'Adj Open', 'Adj. High': 'Adj High',
                      'Adj. Low': 'Adj Low', 'Adj. Close': 'Adj Close',
                      'Adj. Volume': 'Adj Volume'}
    if isinstance(hist_data, DataFrame):
        hist_data.rename(columns=column_renames, inplace=True)
    elif isinstance(hist_data, Panel):
        hist_data.rename(items=column_renames, inplace=True)

    if ret_index:
        hist_data['Ret_Index'] = _calc_return_index(hist_data['Adj Close'])
    if adjust_price:
        hist_data = _adjust_prices(hist_data)

    return hist_data

def _get_data_one(symbol, start, end, interval, retry_count, pause):
    """
    Get historical data for the given name from quandl.
    Date format is datetime

    Returns a DataFrame.
    """
    start, end = _sanitize_dates(start, end)

    # if no specific dataset was provided, default to free WIKI dataset
    if '/' not in symbol:
        symbol = 'WIKI/' + symbol

    query_params = {'start_date': start.strftime('%Y-%m-%d'),
                    'end_date': end.strftime('%Y-%m-%d'),
                    'collapse': interval}

    if __API_KEY:
        query_params['api_key'] = __API_KEY

    url = '%s%s.csv?%s' % (_URL, symbol, urlencode(query_params))

    return _retry_read_url(url, retry_count, pause, 'Quandl')
