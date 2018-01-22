import datetime
import json

import pandas as pd

from dateutil.relativedelta import relativedelta
from pandas_datareader.base import _DailyBaseReader

# Data provided for free by IEX
# Data is furnished in compliance with the guidelines promulgated in the IEX
# API terms of service and manual
# See https://iextrading.com/api-exhibit-a/ for additional information
# and conditions of use


class IEXDailyReader(_DailyBaseReader):

    """
    Returns DataFrame/Panel of historical stock prices from symbols, over date
    range, start to end. To avoid being penalized by Google Finance servers,
    pauses between downloading 'chunks' of symbols can be specified.

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
                 pause=0.35, session=None, chunksize=25):
        super(IEXDailyReader, self).__init__(symbols=symbols, start=start,
                                             end=end, retry_count=retry_count,
                                             pause=pause, session=session,
                                             chunksize=chunksize)

    @property
    def url(self):
        """API URL"""
        return 'https://api.iextrading.com/1.0/stock/market/batch'

    @property
    def endpoint(self):
        """API endpoint"""
        return "chart"

    def _get_params(self, symbol):
        chart_range = self._range_string_from_date()
        print(chart_range)
        if isinstance(symbol, list):
            symbolList = ','.join(symbol)
        else:
            symbolList = symbol
        params = {
            "symbols": symbolList,
            "types": self.endpoint,
            "range": chart_range,
        }
        return params

    def _range_string_from_date(self):
        delta = relativedelta(self.start, datetime.datetime.now())
        if 2 <= (delta.years * -1) <= 5:
            return "5y"
        elif 1 <= (delta.years * -1) <= 2:
            return "2y"
        elif 0 <= (delta.years * -1) < 1:
            return "1y"
        else:
            raise ValueError(
                "Invalid date specified. Must be within past 5 years.")

    def read(self):
        """Read data"""
        try:
            return self._read_one_data(self.url,
                                       self._get_params(self.symbols))
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
            sstart = self.start.strftime('%Y-%m-%d')
            send = self.end.strftime('%Y-%m-%d')
            df = df.loc[sstart:send]
            result.update({symbol: df})
        if len(result) > 1:
            return result
        return result[self.symbols]
