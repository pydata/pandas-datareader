import datetime as dt

from pandas_datareader.av import AlphaVantage


class AVTimeSeriesReader(AlphaVantage):
    """
    Returns DataFrame of the Alpha Vantage Stock Time Series endpoints

    .. versionadded:: 0.7.0

    Parameters
    ----------
    symbols : string
        Single stock symbol (ticker)
    start : string, int, date, datetime, Timestamp
        Starting date. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980'). Defaults to
        20 years before current date.
    end : string, int, date, datetime, Timestamp
        Ending date
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
        "TIME_SERIES_MONTHLY_ADJUSTED": "Monthly Adjusted Time Series",
        "TIME_SERIES_INTRADAY": "Time Series (1min)",
        "FX_DAILY": "Time Series FX (Daily)",
    }

    def __init__(
        self,
        symbols=None,
        function="TIME_SERIES_DAILY",
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        session=None,
        chunksize=25,
        api_key=None,
    ):
        self._func = function
        super(AVTimeSeriesReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        )

    @property
    def default_start_date(self):
        d_days = 3 if self.intraday else 365 * 20
        return dt.datetime.today() - dt.timedelta(days=d_days)

    @property
    def function(self):
        return self._func

    @property
    def intraday(self):
        return True if self.function == "TIME_SERIES_INTRADAY" else False

    @property
    def forex(self):
        return True if self.function == "FX_DAILY" else False

    @property
    def output_size(self):
        """Used to limit the size of the Alpha Vantage query when
        possible.
        """
        delta = dt.datetime.now() - self.start
        return "compact" if delta.days < 80 and not self.intraday else "full"

    @property
    def data_key(self):
        return self._FUNC_TO_DATA_KEY[self.function]

    @property
    def params(self):
        p = {
            "function": self.function,
            "apikey": self.api_key,
            "outputsize": self.output_size,
        }
        if self.intraday:
            p.update({"interval": "1min"})
        if self.forex:
            p.update({"from_symbol": self.symbols.split("/")[0]})
            p.update({"to_symbol": self.symbols.split("/")[1]})
        else:
            p.update({"symbol": self.symbols})
        return p

    def _read_lines(self, out):
        data = super(AVTimeSeriesReader, self)._read_lines(out)
        # reverse since alphavantage returns descending by date
        data = data[::-1]
        start_str = self.start.strftime("%Y-%m-%d")
        end_str = self.end.strftime("%Y-%m-%d")
        data = data.loc[start_str:end_str]
        if data.empty:
            raise ValueError("Please input a valid date range")
        else:
            for column in data.columns:
                if column == "volume":
                    data[column] = data[column].astype("int64")
                else:
                    data[column] = data[column].astype("float64")
        return data
