import warnings
import numpy as np
import pandas.compat as compat
from pandas.core.common import PandasError
from pandas import Panel, DataFrame

from pandas_datareader.date_chunks import _in_chunks

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
