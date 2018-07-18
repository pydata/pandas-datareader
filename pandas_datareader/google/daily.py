from pandas_datareader.base import _DailyBaseReader
import math
import re
from datetime import datetime
from io import StringIO
from pandas import read_csv


class GoogleDailyReader(_DailyBaseReader):
    """
    Returns DataFrame of historical stock prices from
    symbols, over date range, start to end. To avoid being penalized by Google
    Finance servers, pauses between downloading 'chunks' of symbols can be
    specified.

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
    interval : integer, (defaults to 86400 = daily)
        Time in seconds between observations.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : int, default 0
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    chunksize : int, default 25
        Number of symbols to download consecutively before intiating pause.
    session : Session, default None
        requests.sessions.Session instance to be used
    """

    def __init__(self, symbols=None, start=None, end=None, interval=86400,
                 retry_count=3, pause=0.001, session=None, chunksize=25):
        super(GoogleDailyReader, self).__init__(symbols, start, end,
                                                retry_count, pause, session,
                                                chunksize)

        self.interval = interval

    @property
    def url(self):
        return 'https://finance.google.com/finance/getprices'

    def _get_params(self, symbol):
        # get data for years covering requested period (fuzzed to prevent zero)
        years_to_get = math.ceil((datetime.today() - self.start).days
                                 / 365.25 + 0.0001)

        params = {'q': symbol,
                  'i': self.interval,
                  'p': str(years_to_get) + 'Y'}
        return params

    def _read_one_data(self, url, params):

        resp = self._get_response(url, params=params)

        colnames = re.search(r'COLUMNS=(.+)', resp.text).group(1).split(',')
        colnames = list(map(str.capitalize, colnames))

        data = re.search(r'DATA=\n(.*)', resp.text, re.DOTALL).group(1)
        prices = read_csv(StringIO(data), names=colnames, header=None)
        prices = prices[~prices['Date'].str.contains('TIMEZONE_OFFSET')]

        dates = []
        for dt in prices['Date']:
            if 'a' in dt:
                base = int(dt[1:])
                date = base
            else:
                date = base + int(dt)*params['i']
            dates.append(date)

        prices.index = dates
        prices.index = prices.index.map(datetime.fromtimestamp)
        prices = prices.drop('Date', axis=1)
        prices = prices[self.start:self.end]

        return prices
