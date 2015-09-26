from pandas.io.common import urlencode
from pandas_datareader._utils import (
    _retry_read_url, _encode_url, _sanitize_dates, _get_data_from
)

_URL = 'http://www.google.com/finance/historical'


def _get_data(symbols=None, start=None, end=None, retry_count=3,
                    pause=0.001, chunksize=25):
    """
    Returns DataFrame/Panel of historical stock prices from symbols, over date
    range, start to end. To avoid being penalized by Google Finance servers,
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
    chunksize : int, default 25
        Number of symbols to download consecutively before intiating pause.

    Returns
    -------
    hist_data : DataFrame (str) or Panel (array-like object, DataFrame)
    """
    return _get_data_from(symbols, start, end, None, retry_count, pause,
                          chunksize, _get_data_one)


def _get_data_one(sym, start, end, interval, retry_count, pause):
    """
    Get historical data for the given name from google.
    Date format is datetime

    Returns a DataFrame.
    """
    start, end = _sanitize_dates(start, end)

    # www.google.com/finance/historical?q=GOOG&startdate=Jun+9%2C+2011&enddate=Jun+8%2C+2013&output=csv
    params = {
        'q': sym,
        'startdate': start.strftime('%b %d, %Y'),
        'enddate': end.strftime('%b %d, %Y'),
        'output': "csv"
    }
    url = _encode_url(_URL, params)
    return _retry_read_url(url, retry_count, pause, 'Google')
