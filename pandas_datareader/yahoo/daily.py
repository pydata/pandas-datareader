from pandas_datareader._utils import (
    _retry_read_url, _encode_url, _sanitize_dates, _get_data_from
)

_URL = 'http://ichart.finance.yahoo.com/table.csv'

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
    if adjust_price:
        hist_data = _adjust_prices(hist_data)
    return hist_data

def _adjust_prices(hist_data, price_list=None):
    """
    Return modifed DataFrame or Panel with adjusted prices based on
    'Adj Close' price. Adds 'Adj_Ratio' column.
    """
    if price_list is None:
        price_list = 'Open', 'High', 'Low', 'Close'
    adj_ratio = hist_data['Adj Close'] / hist_data['Close']

    data = hist_data.copy()
    for item in price_list:
        data[item] = hist_data[item] * adj_ratio
    data['Adj_Ratio'] = adj_ratio
    del data['Adj Close']
    return data

def _calc_return_index(price_df):
    """
    Return a returns index from a input price df or series. Initial value
    (typically NaN) is set to 1.
    """
    df = price_df.pct_change().add(1).cumprod()
    mask = df.ix[1].notnull() & df.ix[0].isnull()
    df.ix[0][mask] = 1

    # Check for first stock listings after starting date of index in ret_index
    # If True, find first_valid_index and set previous entry to 1.
    if (~mask).any():
        for sym in mask.index[~mask]:
            tstamp = df[sym].first_valid_index()
            t_idx = df.index.get_loc(tstamp) - 1
            df[sym].ix[t_idx] = 1

    return df

def _get_data_one(sym, start, end, interval, retry_count, pause):
    """
    Get historical data for the given name from yahoo.
    Date format is datetime

    Returns a DataFrame.
    """
    start, end = _sanitize_dates(start, end)
    params = {
        's': sym,
        'a': start.month - 1,
        'b': start.day,
        'c': start.year,
        'd': end.month - 1,
        'e': end.day,
        'f': end.year,
        'g': interval,
        'ignore': '.csv'
    }
    url = _encode_url(_URL, params)
    return _retry_read_url(url, retry_count, pause, 'Yahoo!')
