from pandas_datareader.av import AlphaVantage

from datetime import datetime


class AVTimeSeriesReader(AlphaVantage):
    """
    Returns DataFrame of the Alpha Vantage Stock Time Series endpoints

    .. versionadded:: 0.7.0

    Parameters
    ----------
    symbols : string
        Single stock symbol (ticker)
    start : string, (defaults to '1/1/2010')
        Starting date, timestamp. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : string, (defaults to today)
        Ending date, timestamp. Same format as starting date.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : int, default 0.1
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    api_key : str, optional
        AlphaVantage API key . If not provided the environmental variable
        ALPHAVANTAGE_API_KEY is read. The API key is *required*.
    """
    _FUNC_TO_DATA_KEY = {
            "TIME_SERIES_DAILY": "Time Series (Daily)",
            "TIME_SERIES_DAILY_ADJUSTED": "Time Series (Daily)",
            "TIME_SERIES_WEEKLY": "Weekly Time Series",
            "TIME_SERIES_WEEKLY_ADJUSTED": "Weekly Adjusted Time Series",
            "TIME_SERIES_MONTHLY": "Monthly Time Series",
            "TIME_SERIES_MONTHLY_ADJUSTED": "Monthly Adjusted Time Series"
    }

    def __init__(self, symbols=None, function="TIME_SERIES_DAILY",
                 start=None, end=None, retry_count=3, pause=0.1,
                 session=None, chunksize=25, api_key=None):
        super(AVTimeSeriesReader, self).__init__(symbols=symbols, start=start,
                                                 end=end,
                                                 retry_count=retry_count,
                                                 pause=pause, session=session,
                                                 api_key=api_key)

        self.func = function

    @property
    def function(self):
        return self.func

    @property
    def output_size(self):
        """ Used to limit the size of the Alpha Vantage query when
        possible.
        """
        delta = datetime.now() - self.start
        return 'full' if delta.days > 80 else 'compact'

    @property
    def data_key(self):
        return self._FUNC_TO_DATA_KEY[self.function]

    @property
    def params(self):
        return {
            "symbol": self.symbols,
            "function": self.function,
            "apikey": self.api_key,
            "outputsize": self.output_size
        }

    def _read_lines(self, out):
        data = super(AVTimeSeriesReader, self)._read_lines(out)
        start_str = self.start.strftime('%Y-%m-%d')
        end_str = self.end.strftime('%Y-%m-%d')
        data = data.loc[start_str:end_str]
        if data.empty:
            raise ValueError("Please input a valid date range")
        else:
            for column in data.columns:
                if column == 'volume':
                    data[column] = data[column].astype('int64')
                else:
                    data[column] = data[column].astype('float64')
        return data
