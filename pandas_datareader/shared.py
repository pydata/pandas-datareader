import pandas.compat as compat

from .datareaders.google_finance_daily import _get_hist_google
from .datareaders.yahoo_finance_daily import _get_hist_yahoo

_source_functions = {'google': _get_hist_google, 'yahoo': _get_hist_yahoo}


def _get_data_from(symbols, start, end, interval, retry_count, pause,
                   chunksize, source):

    src_fn = _source_functions[source]

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
