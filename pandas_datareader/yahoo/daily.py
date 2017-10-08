import re
import time
import warnings
import numpy as np
from pandas import Panel
from pandas_datareader.base import (_DailyBaseReader, _in_chunks)
from pandas_datareader._utils import (RemoteDataError, SymbolWarning)


class YahooDailyReader(_DailyBaseReader):

    """
    Returns DataFrame/Panel of historical stock prices from symbols, over date
    range, start to end. To avoid being penalized by Yahoo! Finance servers,
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
    session : Session, default None
        requests.sessions.Session instance to be used
    adjust_price : bool, default False
        If True, adjusts all prices in hist_data ('Open', 'High', 'Low',
        'Close') based on 'Adj Close' price. Adds 'Adj_Ratio' column and drops
        'Adj Close'.
    ret_index : bool, default False
        If True, includes a simple return index 'Ret_Index' in hist_data.
    chunksize : int, default 25
        Number of symbols to download consecutively before intiating pause.
    interval : string, default 'd'
        Time interval code, valid values are 'd' for daily, 'w' for weekly,
        'm' for monthly and 'v' for dividend.
    """

    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.35, session=None, adjust_price=False,
                 ret_index=False, chunksize=25, interval='d'):
        super(YahooDailyReader, self).__init__(symbols=symbols,
                                               start=start, end=end,
                                               retry_count=retry_count,
                                               pause=pause, session=session,
                                               chunksize=chunksize)
        # Ladder up the wait time between subsequent requests to improve
        # probability of a successful retry
        self.pause_multiplier = 2.5

        self.headers = {
            'Connection': 'keep-alive',
            'Expires': str(-1),
            'Upgrade-Insecure-Requests': str(1),
            # Google Chrome:
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'  # noqa
        }

        self.adjust_price = adjust_price
        self.ret_index = ret_index
        self.interval = interval

        if self.interval not in ['d', 'wk', 'mo', 'm', 'w']:
            raise ValueError("Invalid interval: valid values are  'd', 'wk' and 'mo'. 'm' and 'w' have been implemented for "  # noqa
                             "backward compatibility. 'v' has been moved to the yahoo-actions or yahoo-dividends APIs.")  # noqa
        elif self.interval in ['m', 'mo']:
            self.pdinterval = 'm'
            self.interval = 'mo'
        elif self.interval in ['w', 'wk']:
            self.pdinterval = 'w'
            self.interval = 'wk'

        self.interval = '1' + self.interval
        self.crumb = self._get_crumb(retry_count)

    @property
    def service(self):
        return 'history'

    @property
    def url(self):
        return 'https://query1.finance.yahoo.com/v7/finance/download/{}'\
            .format(self.symbols)

    @staticmethod
    def yurl(symbol):
        return 'https://query1.finance.yahoo.com/v7/finance/download/{}'\
            .format(symbol)

    def _get_params(self, symbol):
        unix_start = int(time.mktime(self.start.timetuple()))
        day_end = self.end.replace(hour=23, minute=59, second=59)
        unix_end = int(time.mktime(day_end.timetuple()))

        params = {
            'period1': unix_start,
            'period2': unix_end,
            'interval': self.interval,
            'events': self.service,
            'crumb': self.crumb
        }
        return params

    def read(self):
        """ read one data from specified URL """
        try:
            df = super(YahooDailyReader, self).read()
            if self.ret_index:
                df['Ret_Index'] = _calc_return_index(df['Adj Close'])
            if self.adjust_price:
                df = _adjust_prices(df)
            return df.sort_index().dropna(how='all')
        finally:
            self.close()

    def _dl_mult_symbols(self, symbols):
        stocks = {}
        failed = []
        passed = []
        for sym_group in _in_chunks(symbols, self.chunksize):
            for sym in sym_group:
                try:
                    stocks[sym] = self._read_one_data(self.yurl(sym),
                                                      self._get_params(sym))
                    passed.append(sym)
                except IOError:
                    msg = 'Failed to read symbol: {0!r}, replacing with NaN.'
                    warnings.warn(msg.format(sym), SymbolWarning)
                    failed.append(sym)

        if len(passed) == 0:
            msg = "No data fetched using {0!r}"
            raise RemoteDataError(msg.format(self.__class__.__name__))
        try:
            if len(stocks) > 0 and len(failed) > 0 and len(passed) > 0:
                df_na = stocks[passed[0]].copy()
                df_na[:] = np.nan
                for sym in failed:
                    stocks[sym] = df_na
            return Panel(stocks).swapaxes('items', 'minor')
        except AttributeError:
            # cannot construct a panel with just 1D nans indicating no data
            msg = "No data fetched using {0!r}"
            raise RemoteDataError(msg.format(self.__class__.__name__))

    def _get_crumb(self, retries):
        # Scrape a history page for a valid crumb ID:
        tu = "https://finance.yahoo.com/quote/{}/history".format(self.symbols)
        response = self._get_response(tu,
                                      params=self.params, headers=self.headers)
        out = str(self._sanitize_response(response))
        # Matches: {"crumb":"AlphaNumeric"}
        rpat = '"CrumbStore":{"crumb":"([^"]+)"}'

        crumb = re.findall(rpat, out)[0]
        return crumb.encode('ascii').decode('unicode-escape')


def _adjust_prices(hist_data, price_list=None):
    """
    Return modifed DataFrame or Panel with adjusted prices based on
    'Adj Close' price. Adds 'Adj_Ratio' column.
    """
    if price_list is None:
        price_list = 'Open', 'High', 'Low', 'Close'
    adj_ratio = hist_data['Adj Close'] / hist_data['Close']

    data = hist_data.copy()
    for item in price_list:
        data[item] = hist_data[item] * adj_ratio
    data['Adj_Ratio'] = adj_ratio
    del data['Adj Close']
    return data


def _calc_return_index(price_df):
    """
    Return a returns index from a input price df or series. Initial value
    (typically NaN) is set to 1.
    """
    df = price_df.pct_change().add(1).cumprod()
    mask = df.iloc[1].notnull() & df.iloc[0].isnull()
    df.loc[df.index[0], mask] = 1

    # Check for first stock listings after starting date of index in ret_index
    # If True, find first_valid_index and set previous entry to 1.
    if (~mask).any():
        for sym in mask.index[~mask]:
            sym_idx = df.columns.get_loc(sym)
            tstamp = df[sym].first_valid_index()
            t_idx = df.index.get_loc(tstamp) - 1
            df.iloc[t_idx, sym_idx] = 1

    return df
