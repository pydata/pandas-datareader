import pandas as pd

from pandas_datareader.base import _BaseReader


class RobinhoodQuoteReader(_BaseReader):
    """
    Read quotes from Robinhood

    Parameters
    ----------
    symbols : {str, List[str]}
        String symbol of like of symbols
    start : None
        Quotes are near real-time and so this value is ignored
    end : None
        Quotes are near real-time and so this value is ignored
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    freq : None
        Quotes are near real-time and so this value is ignored
    """
    _format = 'json'

    def __init__(self, symbols, start=None, end=None, retry_count=3, pause=.1,
                 timeout=30, session=None, freq=None):
        super(RobinhoodQuoteReader, self).__init__(symbols, start, end,
                                                   retry_count, pause,
                                                   timeout, session, freq)
        if isinstance(self.symbols, str):
            self.symbols = [self.symbols]
        self._max_symbols = 1630
        self._validate_symbols()
        self._json_results = []

    def _validate_symbols(self):
        if len(self.symbols) > self._max_symbols:
            raise ValueError('A maximum of {0} symbols are supported '
                             'in a single call.'.format(self._max_symbols))

    def _get_crumb(self, *args):
        pass

    @property
    def url(self):
        """API URL"""
        return 'https://api.robinhood.com/quotes/'

    @property
    def params(self):
        """Parameters to use in API calls"""
        symbols = ','.join(self.symbols)
        return {'symbols': symbols}

    def _process_json(self):
        res = pd.DataFrame(self._json_results)
        return res.set_index('symbol').T

    def _read_lines(self, out):
        if 'next' in out:
            self._json_results.extend(out['results'])
            return self._read_one_data(out['next'])
        self._json_results.extend(out['results'])
        return self._process_json()


class RobinhoodHistoricalReader(RobinhoodQuoteReader):
    """
    Read historical values from Robinhood

    Parameters
    ----------
    symbols : {str, List[str]}
        String symbol of like of symbols
    start : None
        Ignored.  See span and interval.
    end : None
        Ignored.  See span and interval.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    freq : None
        Quotes are near real-time and so this value is ignored
    interval : {'day' ,'week', '5minute', '10minute'}
        Interval between historical prices
    span : {'day', 'week', 'year', '5year'}
        Time span relative to now to retrieve.  The available spans are a
        function of interval. See notes

    Notes
    -----
    Only provides up to 1 year of daily data.

    The available spans are a function of interval.

      * day: year
      * week: 5year
      * 5minute: day, week
      * 10minute: day, week
    """
    _format = 'json'

    def __init__(self, symbols, start=None, end=None, retry_count=3, pause=.1,
                 timeout=30, session=None, freq=None, interval='day',
                 span='year'):
        super(RobinhoodHistoricalReader, self).__init__(symbols, start, end,
                                                        retry_count, pause,
                                                        timeout, session, freq)
        interval_span = {'day': ['year'],
                         'week': ['5year'],
                         '10minute': ['day', 'week'],
                         '5minute': ['day', 'week']}
        if interval not in interval_span:
            raise ValueError('Interval must be one of '
                             '{0}'.format(', '.join(interval_span.keys())))
        valid_spans = interval_span[interval]
        if span not in valid_spans:
            raise ValueError('For interval {0}, span must '
                             'be in: {1}'.format(interval, valid_spans))
        self.interval = interval
        self.span = span
        self._max_symbols = 75
        self._validate_symbols()
        self._json_results = []

    @property
    def url(self):
        """API URL"""
        return 'https://api.robinhood.com/quotes/historicals/'

    @property
    def params(self):
        """Parameters to use in API calls"""
        symbols = ','.join(self.symbols)
        pars = {'symbols': symbols,
                'interval': self.interval,
                'span': self.span}

        return pars

    def _process_json(self):
        df = []
        for sym in self._json_results:
            vals = pd.DataFrame(sym['historicals'])
            vals['begins_at'] = pd.to_datetime(vals['begins_at'])
            vals['symbol'] = sym['symbol']
            df.append(vals.set_index(['symbol', 'begins_at']))
        return pd.concat(df, 0)
