# flake8: noqa

import datetime as dt

from pandas import read_csv, compat
from pandas.compat import StringIO

from pandas_datareader.base import _DailyBaseReader


class MoexReader(_DailyBaseReader):
    """
    Returns DataFrame of historical stock prices from symbols from Moex

    Parameters
    ----------
    symbols : str, array-like object (list, tuple, Series), or DataFrame
        Single stock symbol (ticker), array-like object of symbols or
        DataFrame with index containing stock symbols.
    start : str, (defaults to '1/1/2010')
        Starting date, timestamp. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : str, (defaults to today)
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

    Notes
    -----
    To avoid being penalized by Moex servers, pauses between downloading
    'chunks' of symbols can be specified.
    """

    def __init__(self, *args, **kwargs):
        super(MoexReader, self).__init__(*args, **kwargs)
        self.start = self.start.date()
        self.end_dt = self.end
        self.end = self.end.date()
        if not isinstance(self.symbols, compat.string_types):
            raise ValueError("Support for multiple symbols is not yet implemented.")

    __url_metadata = "https://iss.moex.com/iss/securities/{symbol}.csv"
    __url_data = "https://iss.moex.com/iss/history/engines/{engine}/" \
                 "markets/{market}/securities/{symbol}.csv"

    @property
    def url(self):
        """API URL"""
        return self.__url_data.format(
            engine=self.__engine,
            market=self.__market,
            symbol=self.symbols
        )

    def _get_params(self, start):
        params = {
            'iss.only': 'history',
            'iss.dp': 'point',
            'iss.df': '%Y-%m-%d',
            'iss.tf': '%H:%M:%S',
            'iss.dft': '%Y-%m-%d %H:%M:%S',
            'iss.json': 'extended',
            'callback': 'JSON_CALLBACK',
            'from': start,
            'till': self.end_dt.strftime('%Y-%m-%d'),
            'limit': 100,
            'start': 1,
            'sort_order': 'TRADEDATE',
            'sort_order_desc': 'asc'
        }
        return params

    def _get_metadata(self):
        """ get a market and an engine for a given symbol """
        response = self._get_response(
            self.__url_metadata.format(symbol=self.symbols)
        )
        text = self._sanitize_response(response)
        if len(text) == 0:
            service = self.__class__.__name__
            raise IOError("{} request returned no data; check URL for invalid "
                          "inputs: {}".format(service, self.__url_metadata))
        if isinstance(text, compat.binary_type):
            text = text.decode('windows-1251')
        else:
            text = text

        header_str = 'secid;boardid;'
        get_data = False
        for s in text.splitlines():
            if s.startswith(header_str):
                get_data = True
                continue
            if get_data and s != '':
                fields = s.split(';')
                return fields[5], fields[7]
        service = self.__class__.__name__
        raise IOError("{} request returned no metadata: {}\n"
                      "Typo in security symbol `{}`?".format(
            service,
            self.__url_metadata.format(symbol=self.symbols),
            self.symbols
        )
        )

    def read(self):
        """Read data"""
        try:
            self.__market, self.__engine = self._get_metadata()

            out_list = []
            date_column = None
            while True:  # read in loop with small date intervals
                if len(out_list) > 0:
                    if date_column is None:
                        date_column = out_list[0].split(';').index('TRADEDATE')

                    # get the last downloaded date
                    start_str = out_list[-1].split(';', 4)[date_column]
                    start = dt.datetime.strptime(start_str, '%Y-%m-%d').date()
                else:
                    start_str = self.start.strftime('%Y-%m-%d')
                    start = self.start

                if start >= self.end or start >= dt.date.today():
                    break

                params = self._get_params(start_str)
                strings_out = self._read_url_as_String(self.url, params) \
                                  .splitlines()[2:]
                strings_out = list(filter(lambda x: x.strip(), strings_out))

                if len(out_list) == 0:
                    out_list = strings_out
                    if len(strings_out) < 101:
                        break
                else:
                    out_list += strings_out[1:]  # remove CSV head line
                    if len(strings_out) < 100:
                        break
            str_io = StringIO('\r\n'.join(out_list))
            df = self._read_lines(str_io)
            return df
        finally:
            self.close()

    def _read_url_as_String(self, url, params=None):
        """ Open url (and retry) """
        response = self._get_response(url, params=params)
        text = self._sanitize_response(response)
        if len(text) == 0:
            service = self.__class__.__name__
            raise IOError("{} request returned no data; check URL for invalid "
                          "inputs: {}".format(service, self.url))
        if isinstance(text, compat.binary_type):
            out = text.decode('windows-1251')
        else:
            out = text
        return out

    def _read_lines(self, input):
        """ return pandas DataFrame from input """
        rs = read_csv(input, index_col='TRADEDATE', parse_dates=True, sep=';',
                      na_values=('-', 'null'))
        # Get rid of unicode characters in index name.
        try:
            rs.index.name = rs.index.name.decode(
                'unicode_escape').encode('ascii', 'ignore')
        except AttributeError:
            # Python 3 string has no decode method.
            rs.index.name = rs.index.name.encode('ascii', 'ignore').decode()
        return rs
