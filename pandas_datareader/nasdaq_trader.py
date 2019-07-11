from ftplib import FTP, all_errors
from pandas import read_csv
from pandas_datareader._utils import RemoteDataError
from pandas.compat import StringIO

import time
import warnings

_NASDAQ_TICKER_LOC = '/SymbolDirectory/nasdaqtraded.txt'
_NASDAQ_FTP_SERVER = 'ftp.nasdaqtrader.com'
_TICKER_DTYPE = [('Nasdaq Traded', bool),
                 ('Symbol', str),
                 ('Security Name', str),
                 ('Listing Exchange', str),
                 ('Market Category', str),
                 ('ETF', bool),
                 ('Round Lot Size', float),
                 ('Test Issue', bool),
                 ('Financial Status', str),
                 ('CQS Symbol', str),
                 ('NASDAQ Symbol', str),
                 ('NextShares', bool)]
_CATEGORICAL = ('Listing Exchange', 'Financial Status')

_DELIMITER = '|'
_ticker_cache = None


def _bool_converter(item):
    return item == 'Y'


def _download_nasdaq_symbols(timeout):
    """
    @param timeout: the time to wait for the FTP connection
    """
    try:
        ftp_session = FTP(_NASDAQ_FTP_SERVER, timeout=timeout)
        ftp_session.login()
    except all_errors as err:
        raise RemoteDataError('Error connecting to %r: %s' %
                              (_NASDAQ_FTP_SERVER, err))

    lines = []
    try:
        ftp_session.retrlines('RETR ' + _NASDAQ_TICKER_LOC, lines.append)
    except all_errors as err:
        raise RemoteDataError('Error downloading from %r: %s' %
                              (_NASDAQ_FTP_SERVER, err))
    finally:
        ftp_session.close()

    # Sanity Checking
    if not lines[-1].startswith('File Creation Time:'):
        raise RemoteDataError('Missing expected footer. Found %r' % lines[-1])

    # Convert Y/N to True/False.
    converter_map = dict((col, _bool_converter) for col, t in _TICKER_DTYPE
                         if t is bool)

    # For pandas >= 0.20.0, the Python parser issues a warning if
    # both a converter and dtype are specified for the same column.
    # However, this measure is probably temporary until the read_csv
    # behavior is better formalized.
    with warnings.catch_warnings(record=True):
        data = read_csv(StringIO('\n'.join(lines[:-1])), '|',
                        dtype=_TICKER_DTYPE, converters=converter_map,
                        index_col=1)

    # Properly cast enumerations
    for cat in _CATEGORICAL:
        data[cat] = data[cat].astype('category')

    return data


def get_nasdaq_symbols(retry_count=3, timeout=30, pause=None):
    """
    Get the list of all available equity symbols from Nasdaq.

    Returns
    -------
    nasdaq_tickers : pandas.DataFrame
        DataFrame with company tickers, names, and other properties.
    """
    global _ticker_cache

    if timeout < 0:
        raise ValueError('timeout must be >= 0, not %r' % (timeout,))

    if pause is None:
        pause = timeout / 3
    elif pause < 0:
        raise ValueError('pause must be >= 0, not %r' % (pause,))

    if _ticker_cache is None:
        while retry_count > 0:
            try:
                _ticker_cache = _download_nasdaq_symbols(timeout=timeout)
                retry_count = -1
            except RemoteDataError:
                # retry on any exception
                if retry_count <= 0:
                    raise
                else:
                    retry_count -= 1
                    time.sleep(pause)

    return _ticker_cache
