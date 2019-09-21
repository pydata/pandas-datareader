import datetime
import json
import os

from dateutil.relativedelta import relativedelta
import pandas as pd

from pandas_datareader.base import _DailyBaseReader

# Data provided for free by IEX
# Data is furnished in compliance with the guidelines promulgated in the IEX
# API terms of service and manual
# See https://iextrading.com/api-exhibit-a/ for additional information
# and conditions of use


class IEXDailyReader(_DailyBaseReader):

    """
    Returns DataFrame of historical stock prices
    from symbols, over date range, start to end. To avoid being penalized by
    IEX servers, pauses between downloading 'chunks' of symbols can be
    specified.

    Parameters
    ----------
    symbols : string, array-like object (list, tuple, Series), or DataFrame
        Single stock symbol (ticker), array-like object of symbols or
        DataFrame with index containing stock symbols.
    start : string, int, date, datetime, Timestamp
        Starting date. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980'). Defaults to
        15 years before current date
    end : string, int, date, datetime, Timestamp
        Ending date
    retry_count : int, default 3
        Number of times to retry query request.
    pause : int, default 0.1
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    chunksize : int, default 25
        Number of symbols to download consecutively before intiating pause.
    session : Session, default None
        requests.sessions.Session instance to be used
    api_key: str
        IEX Cloud Secret Token
    """

    def __init__(
        self,
        symbols=None,
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        session=None,
        chunksize=25,
        api_key=None,
    ):
        if api_key is None:
            api_key = os.getenv("IEX_API_KEY")
        if not api_key or not isinstance(api_key, str):
            raise ValueError(
                "The IEX Cloud API key must be provided either "
                "through the api_key variable or through the "
                " environment variable IEX_API_KEY"
            )
        # Support for sandbox environment (testing purposes)
        if os.getenv("IEX_SANDBOX") == "enable":
            self.sandbox = True
        else:
            self.sandbox = False
        self.api_key = api_key
        super(IEXDailyReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            chunksize=chunksize,
        )

    @property
    def default_start_date(self):
        today = datetime.date.today()
        return today - datetime.timedelta(days=365 * 15)

    @property
    def url(self):
        """API URL"""
        if self.sandbox is True:
            return "https://sandbox.iexapis.com/stable/stock/market/batch"
        else:
            return "https://cloud.iexapis.com/stable/stock/market/batch"

    @property
    def endpoint(self):
        """API endpoint"""
        return "chart"

    def _get_params(self, symbol):
        chart_range = self._range_string_from_date()
        if isinstance(symbol, list):
            symbolList = ",".join(symbol)
        else:
            symbolList = symbol
        params = {
            "symbols": symbolList,
            "types": self.endpoint,
            "range": chart_range,
            "token": self.api_key,
        }
        return params

    def _range_string_from_date(self):
        delta = relativedelta(self.start, datetime.datetime.now())
        years = delta.years * -1
        if 5 <= years <= 15:
            return "max"
        if 2 <= years < 5:
            return "5y"
        elif 1 <= years < 2:
            return "2y"
        elif 0 <= years < 1:
            delta_days = (datetime.datetime.now() - self.start).days
            if 0 <= delta_days < 6:
                return "5d"
            elif 6 <= delta_days < 28:
                return "1m"
            elif 28 <= delta_days < 84:
                return "3m"
            elif 84 <= delta_days < 168:
                return "6m"

            return "1y"
        else:
            raise ValueError("Invalid date specified. Must be within past 15 years.")

    def read(self):
        """Read data"""
        try:
            return self._read_one_data(self.url, self._get_params(self.symbols))
        finally:
            self.close()

    def _read_lines(self, out):
        data = out.read()
        json_data = json.loads(data)
        result = {}
        if type(self.symbols) is str:
            syms = [self.symbols]
        else:
            syms = self.symbols
        for symbol in syms:
            d = json_data.pop(symbol)["chart"]
            df = pd.DataFrame(d)
            df.set_index("date", inplace=True)
            values = ["open", "high", "low", "close", "volume"]
            df = df[values]
            sstart = self.start.strftime("%Y-%m-%d")
            send = self.end.strftime("%Y-%m-%d")
            df = df.loc[sstart:send]
            result.update({symbol: df})
        if len(result) > 1:
            result = pd.concat(result).unstack(level=0)
            result.columns.names = ["Attributes", "Symbols"]
            return result
        return result[self.symbols]
