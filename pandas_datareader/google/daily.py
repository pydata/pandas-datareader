from pandas_datareader.base import _DailyBaseReader
from pandas_datareader.exceptions import ImmediateDeprecationError, \
    DEP_ERROR_MSG


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

    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.1, session=None, chunksize=25):
        raise ImmediateDeprecationError(DEP_ERROR_MSG.format('Google finance'))
        super(GoogleDailyReader, self).__init__(symbols, start, end,
                                                retry_count, pause, session,
                                                chunksize)

    @property
    def url(self):
        """API URL"""
        # Thanks user:vnmabus for pointing this out.
        return 'https://finance.google.co.uk/bctzjpnsun/historical'

    def _get_params(self, symbol):
        params = {
            'q': symbol,
            'startdate': self.start.strftime('%b %d, %Y'),
            'enddate': self.end.strftime('%b %d, %Y'),
            'output': "csv"
        }
        return params
