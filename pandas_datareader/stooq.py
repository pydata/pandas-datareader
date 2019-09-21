from pandas_datareader.base import _DailyBaseReader


class StooqDailyReader(_DailyBaseReader):

    """
    Returns DataFrame/dict of Dataframes of historical stock prices from
    symbols, over date range, start to end.

    Parameters
    ----------
    symbols : string, array-like object (list, tuple, Series), or DataFrame
        Single stock symbol (ticker), array-like object of symbols or
        DataFrame with index containing stock symbols.
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
    chunksize : int, default 25
        Number of symbols to download consecutively before initiating pause.
    session : Session, default None
        requests.sessions.Session instance to be used
    freq: string, d, w, m ,q, y for daily, weekly, monthly, quarterly, yearly

    Notes
    -----
    See `Stooq <https://stooq.com>`__
    """

    @property
    def url(self):
        """API URL"""
        return "https://stooq.com/q/d/l/"

    def _get_params(self, symbol, country="US"):
        symbol_parts = symbol.split(".")
        if not symbol.startswith("^"):
            if len(symbol_parts) == 1:
                symbol = ".".join([symbol, country])
            elif symbol_parts[1].lower() == "pl":
                symbol = symbol_parts[0]
            else:
                if symbol_parts[1].lower() not in ["de", "hk", "hu", "jp", "uk", "us"]:
                    symbol = ".".join([symbol, "US"])

        params = {
            "s": symbol,
            "i": self.freq or "d",
            "d1": self.start.strftime("%Y%m%d"),
            "d2": self.end.strftime("%Y%m%d"),
        }

        return params
