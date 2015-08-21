"""
Module contains tools for collecting data from various remote sources


"""
import warnings
import tempfile
import datetime as dt
import time
import csv

from collections import defaultdict

import numpy as np

from pandas.compat import(
    StringIO, bytes_to_str, range, lmap, zip
)
import pandas.compat as compat
from pandas import Panel, DataFrame, Series, read_csv, concat, to_datetime, DatetimeIndex, DateOffset
from pandas.core.common import is_list_like, PandasError
from pandas.io.common import urlopen
from pandas.tseries.offsets import MonthEnd
from pandas.util.testing import _network_error_classes
from pandas.io.html import read_html


class SymbolWarning(UserWarning):
    pass


class RemoteDataError(PandasError, IOError):
    pass


def DataReader(name, data_source=None, start=None, end=None,
               retry_count=3, pause=0.001):
    """
    Imports data from a number of online sources.

    Currently supports Yahoo! Finance, Google Finance, St. Louis FED (FRED)
    and Kenneth French's data library.

    Parameters
    ----------
    name : str or list of strs
        the name of the dataset. Some data sources (yahoo, google, fred) will
        accept a list of names.
    data_source: str
        the data source ("yahoo", "yahoo-actions", "google", "fred", or "ff")
    start : {datetime, None}
        left boundary for range (defaults to 1/1/2010)
    end : {datetime, None}
        right boundary for range (defaults to today)

    Examples
    ----------

    # Data from Yahoo! Finance
    gs = DataReader("GS", "yahoo")

    # Corporate Actions (Dividend and Split Data) with ex-dates from Yahoo! Finance
    gs = DataReader("GS", "yahoo-actions")

    # Data from Google Finance
    aapl = DataReader("AAPL", "google")

    # Data from FRED
    vix = DataReader("VIXCLS", "fred")

    # Data from Fama/French
    ff = DataReader("F-F_Research_Data_Factors", "famafrench")
    ff = DataReader("F-F_Research_Data_Factors_weekly", "famafrench")
    ff = DataReader("6_Portfolios_2x3", "famafrench")
    ff = DataReader("F-F_ST_Reversal_Factor", "famafrench")
    """
    start, end = _sanitize_dates(start, end)

    if data_source == "yahoo":
        return get_data_yahoo(symbols=name, start=start, end=end,
                              adjust_price=False, chunksize=25,
                              retry_count=retry_count, pause=pause)
    elif data_source == "yahoo-actions":
        return get_data_yahoo_actions(symbol=name, start=start, end=end,
                                      retry_count=retry_count, pause=pause)
    elif data_source == "google":
        return get_data_google(symbols=name, start=start, end=end,
                               adjust_price=False, chunksize=25,
                               retry_count=retry_count, pause=pause)
    elif data_source == "fred":
        return get_data_fred(name, start, end)
    elif data_source == "famafrench":
        return get_data_famafrench(name)


def _sanitize_dates(start, end):
    from pandas.core.datetools import to_datetime
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


def _retry_read_url(url, retry_count, pause, name):
    for _ in range(retry_count):
        time.sleep(pause)

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

    raise IOError("after %d tries, %s did not "
                  "return a 200 for url %r" % (retry_count, name, url))


from .datareaders.google_finance import get_data_google
from .datareaders.google_finance_daily import _get_hist_google
from .datareaders.google_finance_quotes import get_quote_google

from .datareaders.yahoo_finance import get_data_yahoo
from .datareaders.yahoo_finance_daily import _get_hist_yahoo
from .datareaders.yahoo_finance_quotes import get_quote_yahoo
from .datareaders.yahoo_finance_options import Options
from .datareaders.yahoo_finance_actions import get_data_yahoo_actions
from .datareaders.yahoo_finance_components import get_components_yahoo

from .datareaders.fred import get_data_fred
from .datareaders.famafrench import get_data_famafrench

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







