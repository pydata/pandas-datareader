from collections import OrderedDict
import json

from pandas import DataFrame

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import string_types

_DEFAULT_PARAMS = {
    "lang": "en-US",
    "corsDomain": "finance.yahoo.com",
    ".tsrc": "finance",
}


class YahooQuotesReader(_BaseReader):

    """Get current yahoo quote"""

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
