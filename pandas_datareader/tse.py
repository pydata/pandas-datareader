import pandas as pd

from pandas_datareader.base import _DailyBaseReader
from pandas_datareader._utils import RemoteDataError, SymbolWarning

_TSE_TICKER_URL = "http://www.tsetmc.com/tsev2/data/Export-txt.aspx"
_TSE_MARKET_WATCH_INIT_URL = (
    "http://www.tsetmc.com/tsev2/data/MarketWatchInit.aspx?h=0&r=0"
)
_TSE_FIELD_MAPPINGS = {
    "<DTYYYYMMDD>": "Date",
    "<FIRST>": "Open",
    "<HIGH>": "High",
    "<LOW>": "Low",
    "<LAST>": "Close",
    "<VOL>": "Volume",
    "<VALUE>": "Value",
    "<OPENINT>": "Count",
    "<CLOSE>": "AdjClose",
    "<OPEN>": "Yesterday",
}
_tse_ticker_cache = None


class TSEReader(_DailyBaseReader):
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
        'Close') based on 'Adj Close' and 'Yesterday' price.
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
        chunksize=1,
        interval="d",
    ):
        super().__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            chunksize=chunksize,
        )

        # Ladder up the wait time between subsequent requests to improve
        # probability of a successful retry
        self.pause_multiplier = 2.5

        self.adjust_price = adjust_price
        self.interval = interval

        if self.interval not in ["d", "w", "m"]:
            raise ValueError("Invalid interval: valid values are  'd', 'w' and 'm'. ")

    @property
    def url(self):
        """API URL"""
        return _TSE_TICKER_URL

    def _get_params(self, symbol):
        # This needed because yahoo returns data shifted by 4 hours ago.
        index = self._symbol_search_request(symbol)

        params = {
            "t": "i",
            "a": 1,
            "b": 0,
            "i": index,
        }
        return params

    def _read_one_data(self, url, params):
        """read one data from specified URL"""

        out = self._read_url_as_StringIO(url, params)
        try:
            df = pd.read_csv(out)
        except ValueError:
            out.seek(0)
            msg = out.read()
            raise RemoteDataError(
                "message: {}, symbol: {}".format(msg, params.i)
            ) from None

        df = df.iloc[::-1].reset_index(drop=True)
        df = df.rename(columns=_TSE_FIELD_MAPPINGS)
        df = df.reindex(_TSE_FIELD_MAPPINGS.values(), axis=1)

        if self.adjust_price:
            df = _adjust_prices(df)

        if "Date" in df:
            df["Date"] = pd.to_datetime(df["Date"], format="%Y%m%d")
            df = df.set_index("Date")
            df = df[self.start : self.end]
            if self.interval == "w":
                ohlc = df["Close"].resample("w-sat").ohlc()
                ohlc["volume"] = df["Volume"].resample("w-sat").sum()
                df = ohlc
            elif self.interval == "m":
                ohlc = df["Close"].resample("m").ohlc()
                ohlc["volume"] = df["Volume"].resample("m").sum()
                df = ohlc

        return df

    def _symbol_search_request(self, symbol):
        """read one data from specified URL"""
        global _tse_ticker_cache

        if _tse_ticker_cache is None:
            out = self._read_url_as_StringIO(_TSE_MARKET_WATCH_INIT_URL, params=None)
            out.seek(0)
            msg = out.read()
            # response contain different groups for different data
            response_groups = msg.split("@")
            if len(response_groups) < 3:
                raise RemoteDataError(
                    "response groups: {}, symbol: {}".format(
                        len(response_groups), symbol
                    )
                ) from None

            symbols_data = response_groups[2].split(";")

            _tse_ticker_cache = {}
            for symbol_data in symbols_data:
                data = symbol_data.split(",")
                _tse_ticker_cache[
                    self._replace_arabic(data[2]).replace("\u200c", "")
                ] = self._replace_arabic(data[0])

        try:
            if symbol.isnumeric():
                index = symbol
            else:
                index = _tse_ticker_cache[symbol]
        except KeyError:
            raise SymbolWarning("{} not found".format(symbol)) from None

        return index

    def _replace_arabic(self, string: str):
        return string.replace("ك", "ک").replace("ي", "ی").strip()


def _adjust_prices(hist_data, price_list=None):
    """
    Return modifed DataFrame with adjusted prices based on
    'Adj Close' and 'Yesterday'  price
    """
    if hist_data.empty:
        return hist_data
    if not isinstance(hist_data.index, pd.core.indexes.range.RangeIndex):
        raise TypeError(
            "Error in adjusting price; index type must be RangeIndex"
        ) from None
    if price_list is None:
        price_list = ["Open", "High", "Low", "Close", "AdjClose", "Yesterday"]

    data = hist_data.copy()
    step = data.index.step
    diff = list(data.index[data.shift(1).AdjClose != data.Yesterday])
    if len(diff) > 0:
        diff.pop(0)
    ratio = 1
    ratio_list = []
    for i in diff[::-1]:
        ratio *= data.loc[i, "Yesterday"] / data.shift(1).loc[i, "AdjClose"]
        ratio_list.insert(0, ratio)
    for i, k in enumerate(diff):
        if i == 0:
            start = data.index.start
        else:
            start = diff[i - 1]
        end = diff[i] - step
        data.loc[start:end, price_list] = round(
            data.loc[start:end, price_list] * ratio_list[i]
        )

    return data
