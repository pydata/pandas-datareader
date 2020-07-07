import numpy as np
import pandas as pd

from pandas_datareader.av import AlphaVantage
from pandas_datareader.exceptions import (
    DEP_ERROR_MSG,
    ImmediateDeprecationError,
)


class AVQuotesReader(AlphaVantage):
    """
    Returns DataFrame of Alpha Vantage Realtime Stock quotes for a symbol or
    list of symbols.

    Parameters
    ----------
    symbols : string, array-like object (list, tuple, Series), or DataFrame
        Single stock symbol (ticker), array-like object of symbols or
        DataFrame with index containing stock symbols.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : int, default 0.1
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    """

    def __init__(
        self, symbols=None, retry_count=3, pause=0.1, session=None, api_key=None
    ):
        raise ImmediateDeprecationError(DEP_ERROR_MSG.format("AVQuotesReader"))

        if isinstance(symbols, str):
            syms = [symbols]
        elif isinstance(symbols, list):
            if len(symbols) > 100:
                raise ValueError("Up to 100 symbols at once are allowed.")
            else:
                syms = symbols
        super(AVQuotesReader, self).__init__(
            symbols=syms,
            start=None,
            end=None,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        )

    @property
    def function(self):
        return "BATCH_STOCK_QUOTES"

    @property
    def data_key(self):
        return "Stock Quotes"

    @property
    def params(self):
        return {
            "symbols": ",".join(self.symbols),
            "function": self.function,
            "apikey": self.api_key,
        }

    def _read_lines(self, out):
        result = []
        quotes = out[self.data_key]
        for quote in quotes:
            df = pd.DataFrame(quote, index=[0])
            df.columns = [col[3:] for col in df.columns]
            df.set_index("symbol", inplace=True)
            df["price"] = df["price"].astype("float64")
            try:
                df["volume"] = df["volume"].astype("int64")
            except ValueError:
                df["volume"] = [np.nan * len(self.symbols)]
            result.append(df)
        if len(result) != len(self.symbols):
            raise ValueError("Not all symbols downloaded. Check valid " "ticker(s).")
        else:
            return pd.concat(result)
