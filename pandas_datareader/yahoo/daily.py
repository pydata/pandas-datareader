from pandas_datareader.utils import _retry_read_url
from pandas_datareader.utils import _sanitize_dates
from pandas_datareader.utils import _get_data_from
from pandas_datareader.utils import _calc_return_index
from pandas_datareader.utils import _adjust_prices

_URL = 'http://ichart.finance.yahoo.com/table.csv?'

def _get_data(symbols=None, start=None, end=None, retry_count=3,
                   pause=0.001, adjust_price=False, ret_index=False,
                   chunksize=25, interval='d'):
    """
    Returns DataFrame/Panel of historical stock prices from symbols, over date
    range, start to end. To avoid being penalized by Yahoo! Finance servers,
    pauses between downloading 'chunks' of symbols can be specified.

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
    pause : int, default 0
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
    interval : string, default 'd'
        Time interval code, valid values are 'd' for daily, 'w' for weekly,
        'm' for monthly and 'v' for dividend.

    Returns
    -------
    hist_data : DataFrame (str) or Panel (array-like object, DataFrame)
    """
    if interval not in ['d', 'w', 'm', 'v']:
        raise ValueError("Invalid interval: valid values are 'd', 'w', 'm' and 'v'")
    hist_data = _get_data_from(symbols, start, end, interval, retry_count, pause, \
                    chunksize, _get_data_one)
    if ret_index:
        hist_data['Ret_Index'] = _calc_return_index(hist_data['Adj Close'])
    if adjust_price and interval is not 'v':
        hist_data = _adjust_prices(hist_data)
    return hist_data

def _get_data_one(sym, start, end, interval, retry_count, pause):
    """
    Get historical data for the given name from yahoo.
    Date format is datetime

    Returns a DataFrame.
    """
    start, end = _sanitize_dates(start, end)
    url = (_URL + 's=%s' % sym +
           '&a=%s' % (start.month - 1) +
           '&b=%s' % start.day +
           '&c=%s' % start.year +
           '&d=%s' % (end.month - 1) +
           '&e=%s' % end.day +
           '&f=%s' % end.year +
           '&g=%s' % interval +
           '&ignore=.csv')
    return _retry_read_url(url, retry_count, pause, 'Yahoo!')
