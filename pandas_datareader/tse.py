import pandas as pd

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import is_list_like
from pandas_datareader._utils import RemoteDataError, SymbolWarning


class TSEReader(_BaseReader):
    """
    Tehran stock exchange daily data

    Returns DataFrame of historical data from the Tehran Stock Exchange
    open data service, over date range, start to end.

    Parameters
    ----------
    symbols : {int, str, List[str], List[int]}
        The symbols can be persian symbol code or instrument id.
        This argument can be obtained from tsetmc.com site.
    start : string, int, date, datetime, Timestamp
        Starting date. Parses many different kind of date
        default value is 5 years ago
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : string, int, date, datetime, Timestamp
        Ending date
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used.
    adjust_price : bool, default False
        If True, adjusts all prices in hist_data ('Open', 'High', 'Low',
        'Close') based on 'Adj Close' nad 'Yesterday' price.
    interval: string, d, w, m for daily, weekly, monthly
    """

    def __init__(
        self,
        symbols=None,
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        session=None,
        adjust_price=False,
        interval="d",
    ):
        super().__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )

        # Ladder up the wait time between subsequent requests to improve
        # probability of a successful retry
        self.pause_multiplier = 2.5

        self.adjust_price = adjust_price
        self.interval = interval

        if self.interval not in ["d", "w", "m"]:
            raise ValueError(
                "Invalid interval: valid values are  'd', 'w' and 'm'. "
            )

    @property
    def url(self):
        """API URL"""

        return ("http://www.tsetmc.com/tsev2/data/"
                "Export-txt.aspx?t=i&a=1&b=0&i={}")

    def read(self):
        """
        Read data from connector
        """
        try:
            return self._read()
        finally:
            self.close()

    def _read(self):
        """
        read data from many URLs if necessary and
        joins into one DataFrame
        """
        indexes = self._symbol_search_request(self.symbols)

        urls = [self.url.format(indexes[n]) for n in indexes]

        def _req(url, n):
            return self._read_single_request(n, url, self.params)

        dfs = {n: _req(url, n) for url, n in zip(urls, indexes)}

        return dfs

    def _read_single_request(self, symbol, url, params):
        """read one data from specified URL"""

        out = self._read_url_as_StringIO(url, params=None)
        try:
            df = pd.read_csv(out)
        except ValueError:
            out.seek(0)
            msg = out.read()
            raise RemoteDataError(
                "message: {}, symbol: {}".format(msg, symbol)
            ) from None

        df = df.iloc[::-1]
        HISTORY_FIELD_MAPPINGS = {
            "<DTYYYYMMDD>": "date",
            "<FIRST>": "open",
            "<HIGH>": "high",
            "<LOW>": "low",
            "<LAST>": "close",
            "<VOL>": "volume",
            "<VALUE>": "value",
            "<OPENINT>": "count",
            "<CLOSE>": "adjClose",
            "<OPEN>": "yesterday",
        }
        df = df.rename(columns=HISTORY_FIELD_MAPPINGS)
        df = df.reindex(HISTORY_FIELD_MAPPINGS.values(), axis=1)

        if "date" in df:
            df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
            df = df.set_index("date")
            df = df[self.start:self.end]
        return df

    def _symbol_search_request(self, symbols):
        """read one data from specified URL"""
        MARKET_WATCH_INIT_URL = (
            "http://www.tsetmc.com/tsev2/data/MarketWatchInit.aspx?h=0&r=0"
        )
        if not is_list_like(symbols):
            names = [symbols]
        else:
            names = symbols
        out = self._read_url_as_StringIO(MARKET_WATCH_INIT_URL, params=None)
        out.seek(0)
        msg = out.read()
        # response contain different groups for different data
        response_groups = msg.split("@")
        if len(response_groups) < 3:
            raise RemoteDataError(
                "response groups: {}, symbol: {}".format(
                    len(response_groups),
                    symbols
                )
            ) from None

        symbols_data = response_groups[2].split(";")

        market_symbols = {}
        for symbol_data in symbols_data:
            data = symbol_data.split(",")
            market_symbols[
                self._replace_arabic(data[2]).replace('\u200c', '')
            ] = self._replace_arabic(data[0])

        indexes = {}
        for name in names:
            try:
                if name.isnumeric():
                    indexes[name] = name
                else:
                    indexes[name] = market_symbols[name]
            except KeyError:
                raise SymbolWarning(
                    "{} not found".format(name)
                ) from None

        return indexes

    def _replace_arabic(self, string: str):
        return string.replace('ك', 'ک').replace('ي', 'ی').strip()
