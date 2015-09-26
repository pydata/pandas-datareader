import time
import warnings
import numpy as np
import datetime as dt

from pandas import to_datetime
import pandas.compat as compat
from pandas.core.common import PandasError
from pandas import Panel, DataFrame
from pandas.io.common import urlopen
from pandas import read_csv
from pandas.compat import StringIO, bytes_to_str
from pandas.util.testing import _network_error_classes

if compat.PY3:
    from urllib.parse import urlencode
else:
    from urllib import urlencode

class SymbolWarning(UserWarning):
    pass

class RemoteDataError(PandasError, IOError):
    pass

def _get_data_from(symbols, start, end, interval, retry_count, pause,
                   chunksize, src_fn):

    # If a single symbol, (e.g., 'GOOG')
    if isinstance(symbols, (compat.string_types, int)):
        hist_data = src_fn(symbols, start, end, interval, retry_count, pause)
    # Or multiple symbols, (e.g., ['GOOG', 'AAPL', 'MSFT'])
    elif isinstance(symbols, DataFrame):
        hist_data = _dl_mult_symbols(symbols.index, start, end, interval, chunksize,
                                     retry_count, pause, src_fn)
    else:
        hist_data = _dl_mult_symbols(symbols, start, end, interval, chunksize,
                                     retry_count, pause, src_fn)
    return hist_data

def _dl_mult_symbols(symbols, start, end, interval, chunksize, retry_count, pause,
                     method):
    stocks = {}
    failed = []
    passed = []
    for sym_group in _in_chunks(symbols, chunksize):
        for sym in sym_group:
            try:
                stocks[sym] = method(sym, start, end, interval, retry_count, pause)
                passed.append(sym)
            except IOError:
                warnings.warn('Failed to read symbol: {0!r}, replacing with '
                              'NaN.'.format(sym), SymbolWarning)
                failed.append(sym)

    if len(passed) == 0:
        raise RemoteDataError("No data fetched using "
                              "{0!r}".format(method.__name__))
    try:
        if len(stocks) > 0 and len(failed) > 0 and len(passed) > 0:
            df_na = stocks[passed[0]].copy()
            df_na[:] = np.nan
            for sym in failed:
                stocks[sym] = df_na
        return Panel(stocks).swapaxes('items', 'minor')
    except AttributeError:
        # cannot construct a panel with just 1D nans indicating no data
        raise RemoteDataError("No data fetched using "
                              "{0!r}".format(method.__name__))


def _sanitize_dates(start, end):
    """
    Return (datetime_start, datetime_end) tuple
    if start is None - default is 2010/01/01
    if end is None - default is today
    """
    start = to_datetime(start)
    end = to_datetime(end)
    if start is None:
        start = dt.datetime(2010, 1, 1)
    if end is None:
        end = dt.datetime.today()
    return start, end

def _in_chunks(seq, size):
    """
    Return sequence in 'chunks' of size defined by size
    """
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def _encode_url(url, params):
    """
    Return encoded url with parameters
    """
    s_params = urlencode(params)
    if s_params:
        return url + '?' + s_params
    else:
        return url

def _retry_read_url(url, retry_count, pause, name):
    """
    Open url (and retry)
    """
    for _ in range(retry_count):

        # kludge to close the socket ASAP
        try:
            with urlopen(url) as resp:
                lines = resp.read()
        except _network_error_classes:
            pass
        else:
            rs = read_csv(StringIO(bytes_to_str(lines)), index_col=0,
                          parse_dates=True, na_values='-')[::-1]
            # Yahoo! Finance sometimes does this awesome thing where they
            # return 2 rows for the most recent business day
            if len(rs) > 2 and rs.index[-1] == rs.index[-2]:  # pragma: no cover
                rs = rs[:-1]

            #Get rid of unicode characters in index name.
            try:
                rs.index.name = rs.index.name.decode('unicode_escape').encode('ascii', 'ignore')
            except AttributeError:
                #Python 3 string has no decode method.
                rs.index.name = rs.index.name.encode('ascii', 'ignore').decode()

            return rs

        time.sleep(pause)

    raise IOError("after %d tries, %s did not "
                  "return a 200 for url %r" % (retry_count, name, url))
