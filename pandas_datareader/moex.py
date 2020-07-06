import datetime as dt

import pandas as pd

from pandas_datareader.base import _DailyBaseReader
from pandas_datareader.compat import (
    StringIO,
    binary_type,
    concat,
    is_list_like,
)


class MoexReader(_DailyBaseReader):
    """
    Returns a DataFrame of historical stock prices from symbols from Moex

    Parameters
    ----------
    symbols : str, an array-like object (list, tuple, Series), or a DataFrame
        A single stock symbol (secid), an array-like object of symbols or
        a DataFrame with an index containing stock symbols.
    start : string, int, date, datetime, Timestamp
        Starting date. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980'). Defaults to
        20 years before current date.
    end : string, int, date, datetime, Timestamp
        Ending date
    retry_count : int, default 3
        The number of times to retry query request.
    pause : int, default 0.1
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    chunksize : int, default 25
        The number of symbols to download consecutively before intiating pause.
    session : Session, default None
        requests.sessions.Session instance to be used

    Notes
    -----
    To avoid being penalized by Moex servers, pauses more than 0.1s between
    downloading 'chunks' of symbols can be specified.
    """

    def __init__(self, *args, **kwargs):
        super(MoexReader, self).__init__(*args, **kwargs)
        self.start = self.start.date()
        self.end_dt = self.end
        self.end = self.end.date()

        if isinstance(self.symbols, pd.DataFrame):
            self.symbols = self.symbols.index.tolist()
        elif not is_list_like(self.symbols):
            self.symbols = [self.symbols]

        self.__markets_n_engines = {}  # dicts for tuples of engines and markets

    __url_metadata = "https://iss.moex.com/iss/securities/{symbol}.csv"
    __url_data = (
        "https://iss.moex.com/iss/history/engines/{engine}/"
        "markets/{market}/securities/{symbol}.csv"
    )

    @property
    def url(self):
        """Return a list of API URLs per symbol"""

        if not self.__markets_n_engines:
            raise Exception(
                "Accessing url property before invocation "
                "of read() or _get_metadata() methods"
            )

        return [
            self.__url_data.format(engine=engine, market=market, symbol=s)
            for s in self.symbols
            if s in self.__markets_n_engines
            for market, engine in self.__markets_n_engines[s]
        ]

    def _get_params(self, start):
        """Return a dict for REST API GET request parameters"""

        params = {
            "iss.only": "history",
            "iss.dp": "point",
            "iss.df": "%Y-%m-%d",
            "iss.tf": "%H:%M:%S",
            "iss.dtf": "%Y-%m-%d %H:%M:%S",
            "iss.json": "extended",
            "callback": "JSON_CALLBACK",
            "from": start,
            "till": self.end_dt.strftime("%Y-%m-%d"),
            "limit": 100,
            "start": 0,
            "sort_order": "TRADEDATE",
            "sort_order_desc": "asc",
        }
        return params

    def _get_metadata(self):
        """Get markets and engines for the given symbols"""

        markets_n_engines = {}

        for symbol in self.symbols:
            response = self._get_response(self.__url_metadata.format(symbol=symbol))
            text = self._sanitize_response(response)
            if len(text) == 0:
                service = self.__class__.__name__
                raise IOError(
                    "{} request returned no data; check URL for invalid "
                    "inputs: {}".format(service, self.__url_metadata)
                )
            if isinstance(text, binary_type):
                text = text.decode("windows-1251")

            header_str = "secid;boardid;"
            get_data = False
            for s in text.splitlines():
                if s.startswith(header_str):
                    get_data = True
                    continue
                if get_data and s != "":
                    fields = s.split(";")

                    if symbol not in markets_n_engines:
                        markets_n_engines[symbol] = list()

                    markets_n_engines[symbol].append(
                        (fields[5], fields[7])
                    )  # market and engine
            if symbol not in markets_n_engines:
                raise IOError(
                    "{} request returned no metadata: {}\n"
                    "Typo in the security symbol `{}`?".format(
                        self.__class__.__name__,
                        self.__url_metadata.format(symbol=symbol),
                        symbol,
                    )
                )
            if symbol in markets_n_engines:
                markets_n_engines[symbol] = list(set(markets_n_engines[symbol]))
        return markets_n_engines

    def read(self):
        """Read data"""

        try:
            self.__markets_n_engines = self._get_metadata()

            urls = self.url  # generate urls per symbols
            dfs = []  # an array of pandas dataframes per symbol to concatenate

            for i in range(len(urls)):
                out_list = []
                date_column = None

                while True:  # read in a loop with small date intervals
                    if len(out_list) > 0:
                        if date_column is None:
                            date_column = out_list[0].split(";").index("TRADEDATE")

                        # get the last downloaded date
                        start_str = out_list[-1].split(";", 4)[date_column]
                        start = dt.datetime.strptime(start_str, "%Y-%m-%d").date()
                    else:
                        start_str = self.start.strftime("%Y-%m-%d")
                        start = self.start

                    if start > self.end or start > dt.date.today():
                        break

                    params = self._get_params(start_str)
                    strings_out = self._read_url_as_String(
                        urls[i], params
                    ).splitlines()[2:]
                    strings_out = list(filter(lambda x: x.strip(), strings_out))

                    if len(out_list) == 0:
                        out_list = strings_out
                        if len(strings_out) < 101:  # all data received - break
                            break
                    else:
                        out_list += strings_out[1:]  # remove a CSV head line
                        if len(strings_out) < 100:  # all data recevied - break
                            break

                if len(out_list) > 0:
                    str_io = StringIO("\r\n".join(out_list))
                    dfs.append(self._read_lines(str_io))  # add a new DataFrame
        finally:
            self.close()

        if len(dfs) == 0:
            raise IOError(
                "{} returned no data; "
                "check URL or correct a date".format(self.__class__.__name__)
            )
        elif len(dfs) > 1:
            return concat(dfs, axis=0, join="outer", sort=True)
        else:
            return dfs[0]

    def _read_url_as_String(self, url, params=None):
        """Open an url (and retry)"""

        response = self._get_response(url, params=params)
        text = self._sanitize_response(response)
        if len(text) == 0:
            service = self.__class__.__name__
            raise IOError(
                "{} request returned no data; check URL for invalid "
                "inputs: {}".format(service, self.url)
            )
        if isinstance(text, binary_type):
            text = text.decode("windows-1251")
        return text

    def _read_lines(self, input):
        """Return a pandas DataFrame from input"""

        return pd.read_csv(
            input,
            index_col="TRADEDATE",
            parse_dates=True,
            sep=";",
            na_values=("-", "null"),
        )
