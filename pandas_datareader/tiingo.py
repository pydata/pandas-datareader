import os

import pandas as pd

from pandas_datareader.base import _BaseReader


def get_tiingo_symbols():
    """
    Get the set of stock symbols supported by Tiingo

    Returns
    -------
    symbols : DataFrame
        DataFrame with symbols (ticker), exchange, asset type, currency and
        start and end dates

    Notes
    -----
    Reads https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip
    """
    url = 'https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip'
    return pd.read_csv(url)


class TiingoDailyReader(_BaseReader):
    """
    Historical daily data from Tiingo on equities, ETFs and mutual funds

    Parameters
    ----------
    symbols : {str, List[str]}
        String symbol of like of symbols
    start : str, (defaults to '1/1/2010')
        Starting date, timestamp. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : str, (defaults to today)
        Ending date, timestamp. Same format as starting date.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    freq : {str, None}
        Not used.
    api_key : str, optional
        Tiingo API key . If not provided the environmental variable
        TIINGO_API_KEY is read. The API key is *required*.
    """

    def __init__(self, symbols, start=None, end=None, retry_count=3, pause=0.1,
                 timeout=30, session=None, freq=None, api_key=None):
        super(TiingoDailyReader, self).__init__(symbols, start, end,
                                                retry_count, pause, timeout,
                                                session, freq)
        if isinstance(self.symbols, str):
            self.symbols = [self.symbols]
        self._symbol = ''
        if api_key is None:
            api_key = os.getenv('TIINGO_API_KEY')
        if not api_key or not isinstance(api_key, str):
            raise ValueError('The tiingo API key must be provided either '
                             'through the api_key variable or through the '
                             'environmental variable TIINGO_API_KEY.')
        self.api_key = api_key
        self._concat_axis = 0

    @property
    def url(self):
        """API URL"""
        _url = 'https://api.tiingo.com/tiingo/daily/{ticker}/prices'
        return _url.format(ticker=self._symbol)

    @property
    def params(self):
        """Parameters to use in API calls"""
        return {'startDate': self.start.strftime('%Y-%m-%d'),
                'endDate': self.end.strftime('%Y-%m-%d'),
                'format': 'json'}

    def _get_crumb(self, *args):
        pass

    def _read_one_data(self, url, params):
        """ read one data from specified URL """
        headers = {'Content-Type': 'application/json',
                   'Authorization': 'Token ' + self.api_key}
        out = self._get_response(url, params=params, headers=headers).json()
        return self._read_lines(out)

    def _read_lines(self, out):
        df = pd.DataFrame(out)
        df['symbol'] = self._symbol
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index(['symbol', 'date'])
        return df

    def read(self):
        """Read data from connector"""
        dfs = []
        for symbol in self.symbols:
            self._symbol = symbol
            try:
                dfs.append(self._read_one_data(self.url, self.params))
            finally:
                self.close()
        return pd.concat(dfs, self._concat_axis)


class TiingoMetaDataReader(TiingoDailyReader):
    """
    Read metadata about symbols from Tiingo

    Parameters
    ----------
    symbols : {str, List[str]}
        String symbol of like of symbols
    start : str, (defaults to '1/1/2010')
        Not used.
    end : str, (defaults to today)
        Not used.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    freq : {str, None}
        Not used.
    api_key : str, optional
        Tiingo API key . If not provided the environmental variable
        TIINGO_API_KEY is read. The API key is *required*.
    """

    def __init__(self, symbols, start=None, end=None, retry_count=3, pause=0.1,
                 timeout=30, session=None, freq=None, api_key=None):
        super(TiingoMetaDataReader, self).__init__(symbols, start, end,
                                                   retry_count, pause, timeout,
                                                   session, freq, api_key)
        self._concat_axis = 1

    @property
    def url(self):
        """API URL"""
        _url = 'https://api.tiingo.com/tiingo/daily/{ticker}'
        return _url.format(ticker=self._symbol)

    @property
    def params(self):
        return None

    def _read_lines(self, out):
        s = pd.Series(out)
        s.name = self._symbol
        return s


class TiingoQuoteReader(TiingoDailyReader):
    """
    Read quotes (latest prices) from Tiingo

    Parameters
    ----------
    symbols : {str, List[str]}
        String symbol of like of symbols
    start : str, (defaults to '1/1/2010')
        Not used.
    end : str, (defaults to today)
        Not used.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    freq : {str, None}
        Not used.
    api_key : str, optional
        Tiingo API key . If not provided the environmental variable
        TIINGO_API_KEY is read. The API key is *required*.

    Notes
    -----
    This is a special case of the daily reader which automatically selected
    the latest data available for each symbol.
    """

    @property
    def params(self):
        return None
