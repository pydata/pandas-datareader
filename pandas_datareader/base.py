import time
import warnings
import numpy as np
import datetime as dt

import requests
from requests_file import FileAdapter

from pandas import to_datetime
import pandas.compat as compat
from pandas.core.common import PandasError, is_number
from pandas import Panel, DataFrame
from pandas import read_csv
from pandas.compat import StringIO, bytes_to_str
from pandas.util.testing import _network_error_classes

from pandas_datareader._utils import RemoteDataError, SymbolWarning


class _BaseReader(object):

    """

    Parameters
    ----------
        sym : string with a single Single stock symbol (ticker).
        start : string, (defaults to '1/1/2010')
                Starting date, timestamp. Parses many different kind of date
                representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
        end : string, (defaults to today)
                Ending date, timestamp. Same format as starting date.
        retry_count : int, default 3
                Number of times to retry query request.
        pause : int, default 0
                Time, in seconds, of the pause between retries.
        session : Session, default None
                requests.sessions.Session instance to be used
    """

    _chunk_size = 1024 * 1024
    _format = 'string'

    def __init__(self, symbols, start=None, end=None,
                 retry_count=3, pause=0.001, session=None):
        self.symbols = symbols

        start, end = self._sanitize_dates(start, end)
        self.start = start
        self.end = end

        if not isinstance(retry_count, int) or retry_count < 0:
            raise ValueError("'retry_count' must be integer larger than 0")
        self.retry_count = retry_count
        self.pause = pause
        self.session = self._init_session(session, retry_count)

    def _init_session(self, session, retry_count):
        if session is None:
            session = requests.Session()
            session.mount('file://', FileAdapter())
            # do not set requests max_retries here to support arbitrary pause
        return session

    @property
    def url(self):
        # must be overridden in subclass
        raise NotImplementedError

    @property
    def params(self):
        return None

    def read(self):
        """ read data """
        return self._read_one_data(self.url, self.params)

    def _read_one_data(self, url, params):
        """ read one data from specified URL """
        if self._format == 'string':
            out = self._read_url_as_StringIO(url, params=params)
        elif self._format == 'json':
            out = self._get_response(url, params=params).json()
        else:
            raise NotImplementedError(self._format)
        return self._read_lines(out)

    def _read_url_as_StringIO(self, url, params=None):
        """
        Open url (and retry)
        """
        response = self._get_response(url, params=params)
        out = StringIO()
        if isinstance(response.content, compat.binary_type):
            out.write(bytes_to_str(response.content))
        else:
            out.write(response.content)
        out.seek(0)
        return out

    def _get_response(self, url, params=None):
        """ send raw HTTP request to get requests.Response from the specified url
        Parameters
        ----------
        url : str
            target URL
        params : dict or None
            parameters passed to the URL
        """

        # initial attempt + retry
        for i in range(self.retry_count + 1):
            response = self.session.get(url, params=params)
            if response.status_code == requests.codes.ok:
                return response
            time.sleep(self.pause)

        raise RemoteDataError('Unable to read URL: {0}'.format(url))

    def _read_lines(self, out):
        rs = read_csv(out, index_col=0, parse_dates=True, na_values='-')[::-1]
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

    def _sanitize_dates(self, start, end):
        """
        Return (datetime_start, datetime_end) tuple
        if start is None - default is 2010/01/01
        if end is None - default is today
        """
        if is_number(start):
            # regard int as year
            start = dt.datetime(start, 1, 1)
        start = to_datetime(start)

        if is_number(end):
            end = dt.datetime(end, 1, 1)
        end = to_datetime(end)

        if start is None:
            start = dt.datetime(2010, 1, 1)
        if end is None:
            end = dt.datetime.today()
        return start, end


class _DailyBaseReader(_BaseReader):
    """ Base class for Google / Yahoo daily reader """

    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.001, session=None, chunksize=25):
        super(_DailyBaseReader, self).__init__(symbols=symbols,
                                               start=start, end=end,
                                               retry_count=retry_count,
                                               pause=pause, session=session)
        self.chunksize = chunksize

    def _get_params(self, *args, **kwargs):
        raise NotImplementedError

    def read(self):
        """ read data """
        # If a single symbol, (e.g., 'GOOG')
        if isinstance(self.symbols, (compat.string_types, int)):
            df = self._read_one_data(self.url, params=self._get_params(self.symbols))
        # Or multiple symbols, (e.g., ['GOOG', 'AAPL', 'MSFT'])
        elif isinstance(self.symbols, DataFrame):
            df = self._dl_mult_symbols(self.symbols.index)
        else:
            df = self._dl_mult_symbols(self.symbols)
        return df

    def _dl_mult_symbols(self, symbols):
        stocks = {}
        failed = []
        passed = []
        for sym_group in _in_chunks(symbols, self.chunksize):
            for sym in sym_group:
                try:
                    stocks[sym] = self._read_one_data(self.url, self._get_params(sym))
                    passed.append(sym)
                except IOError:
                    msg = 'Failed to read symbol: {0!r}, replacing with NaN.'
                    warnings.warn(msg.format(sym), SymbolWarning)
                    failed.append(sym)

        if len(passed) == 0:
            msg = "No data fetched using {0!r}"
            raise RemoteDataError(msg.format(self.__class__.__name__))
        try:
            if len(stocks) > 0 and len(failed) > 0 and len(passed) > 0:
                df_na = stocks[passed[0]].copy()
                df_na[:] = np.nan
                for sym in failed:
                    stocks[sym] = df_na
            return Panel(stocks).swapaxes('items', 'minor')
        except AttributeError:
            # cannot construct a panel with just 1D nans indicating no data
            msg = "No data fetched using {0!r}"
            raise RemoteDataError(msg.format(self.__class__.__name__))


def _in_chunks(seq, size):
    """
    Return sequence in 'chunks' of size defined by size
    """
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))
