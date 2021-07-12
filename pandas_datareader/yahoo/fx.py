import json
import time
import warnings

from pandas import DataFrame, Series, concat, to_datetime

from pandas_datareader._utils import RemoteDataError, SymbolWarning
from pandas_datareader.compat import string_types
from pandas_datareader.yahoo.daily import YahooDailyReader


class YahooFXReader(YahooDailyReader):
    """
    Returns DataFrame of historical prices for currencies

    Parameters
    ----------
    symbols : string, array-like object (list, tuple, Series), or DataFrame
        Single stock symbol (ticker), array-like object of symbols or
        DataFrame with index containing stock symbols.
    start : string, int, date, datetime, Timestamp
        Starting date, timestamp. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980').
        Defaults to '1/1/2010'.
    end : string, int, date, datetime, Timestamp
        Ending date, timestamp. Same format as starting date. Defaults to today.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : int, default 0.1
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    chunksize : int, default 25 (NOT IMPLEMENTED)
        Number of symbols to download consecutively before intiating pause.
    interval : string, default '1d'
        Valid values are '1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y',
            '10y', 'ytd', 'max'
    """

    def _get_params(self, symbol):
        unix_start = int(time.mktime(self.start.timetuple()))
        day_end = self.end.replace(hour=23, minute=59, second=59)
        unix_end = int(time.mktime(day_end.timetuple()))

        params = {
            "symbol": symbol + "=X",
            "period1": unix_start,
            "period2": unix_end,
            "interval": self.interval,  # deal with this
            "includePrePost": "true",
            "events": "div|split|earn",
            "corsDomain": "finance.yahoo.com",
        }
        return params

    def read(self):
        """Read data"""
        try:
            # If a single symbol, (e.g., 'GOOG')
            if isinstance(self.symbols, (string_types, int)):
                df = self._read_one_data(self.symbols)

            # Or multiple symbols, (e.g., ['GOOG', 'AAPL', 'MSFT'])
            elif isinstance(self.symbols, DataFrame):
                df = self._dl_mult_symbols(self.symbols.index)
            else:
                df = self._dl_mult_symbols(self.symbols)

            if "Date" in df:
                df = df.set_index("Date")

            if "Volume" in df:
                df = df.drop("Volume", axis=1)

            return df.sort_index().dropna(how="all")
        finally:
            self.close()

    def _read_one_data(self, symbol):
        """read one data from specified URL"""
        url = "https://query1.finance.yahoo.com/v8/finance/chart/{}=X".format(symbol)
        params = self._get_params(symbol)

        resp = self._get_response(url, params=params)
        jsn = json.loads(resp.text)

        data = jsn["chart"]["result"][0]
        df = DataFrame(data["indicators"]["quote"][0])
        df.insert(0, "date", to_datetime(Series(data["timestamp"]), unit="s").dt.date)
        df.columns = map(str.capitalize, df.columns)
        return df

    def _dl_mult_symbols(self, symbols):
        stocks = {}
        failed = []
        passed = []
        for sym in symbols:
            try:
                df = self._read_one_data(sym)
                df["PairCode"] = sym
                stocks[sym] = df
                passed.append(sym)
            except IOError:
                msg = "Failed to read symbol: {0!r}, replacing with NaN."
                warnings.warn(msg.format(sym), SymbolWarning)
                failed.append(sym)

        if len(passed) == 0:
            msg = "No data fetched using {0!r}"
            raise RemoteDataError(msg.format(self.__class__.__name__))
        else:
            return concat(stocks).set_index(["PairCode", "Date"])
