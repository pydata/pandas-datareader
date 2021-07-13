from collections import OrderedDict
import json

from pandas import DataFrame

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import string_types
from pandas_datareader.yahoo.headers import DEFAULT_HEADERS

_DEFAULT_PARAMS = {
    "lang": "en-US",
    "corsDomain": "finance.yahoo.com",
    ".tsrc": "finance",
}


class YahooQuotesReader(_BaseReader):

    """Get current yahoo quote"""

    def __init__(
        self,
        symbols=None,
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        session=None,
    ):
        super().__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )
        if session is not None:
            self.headers = session.headers
        else:
            self.headers = DEFAULT_HEADERS

    @property
    def url(self):
        return "https://query1.finance.yahoo.com/v7/finance/quote"

    def read(self):
        if isinstance(self.symbols, string_types):
            return self._read_one_data(self.url, self.params(self.symbols))
        else:
            data = OrderedDict()
            for symbol in self.symbols:
                data[symbol] = self._read_one_data(self.url, self.params(symbol)).loc[
                    symbol
                ]
            return DataFrame.from_dict(data, orient="index")

    def params(self, symbol):
        """Parameters to use in API calls"""
        # Construct the code request string.
        params = {"symbols": symbol}
        params.update(_DEFAULT_PARAMS)
        return params

    def _read_lines(self, out):
        data = json.loads(out.read())["quoteResponse"]["result"][0]
        idx = data.pop("symbol")
        data["price"] = data["regularMarketPrice"]
        return DataFrame(data, index=[idx])
