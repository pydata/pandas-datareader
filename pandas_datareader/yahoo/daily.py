import re
import json
import time
import warnings
from pandas import (DataFrame, to_datetime, concat)
from pandas_datareader.base import _DailyBaseReader
from pandas_datareader._utils import (RemoteDataError, SymbolWarning)
from pandas.core.indexes.numeric import Int64Index
import pandas.compat as compat


class YahooDailyReader(_DailyBaseReader):

    """
    Returns a dictionary of DataFrames with historical stock prices, dividends,
    and splits from symbols, over date range, start to end. To avoid being
    penalized by Yahoo! Finance servers, pauses between downloading 'chunks' of
    symbols can be specified.

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
                 ret_index=False, chunksize=1, interval='d',
                 get_actions=True):
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
        self.get_actions = get_actions

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

    def _get_params(self, symbol):
        unix_start = int(time.mktime(self.start.timetuple()))
        day_end = self.end.replace(hour=23, minute=59, second=59)
        unix_end = int(time.mktime(day_end.timetuple()))

        params = {
            'period1': unix_start,
            'period2': unix_end,
            'interval': self.interval,
            'frequency': self.interval,
            'filter': 'history'
        }
        return params

    def read(self):
        """Read data"""
        try:
            # If a single symbol, (e.g., 'GOOG')
            if isinstance(self.symbols, (compat.string_types, int)):
                dfs = self._read_one_data(self.symbols)

            # Or multiple symbols, (e.g., ['GOOG', 'AAPL', 'MSFT'])
            elif isinstance(self.symbols, DataFrame):
                dfs = self._dl_mult_symbols(self.symbols.index)
            else:
                dfs = self._dl_mult_symbols(self.symbols)

            for k in dfs:
                if isinstance(dfs[k].index, Int64Index):
                    dfs[k] = dfs[k].set_index('Date')
                dfs[k] = dfs[k].sort_index().dropna(how='all')

            if self.ret_index:
                dfs['prices']['Ret_Index'] = \
                    _calc_return_index(dfs['prices']['Adj Close'])
            if self.adjust_price:
                dfs['prices'] = _adjust_prices(dfs['prices'])

            return dfs
        finally:
            self.close()

    def _read_one_data(self, symbol):
        """ read one data from specified symbol """
        url = 'https://finance.yahoo.com/quote/{}/history'.format(symbol)
        params = self._get_params(symbol)

        resp = self._get_response(url, params=params)
        ptrn = r'root\.App\.main = (.*?);\n}\(this\)\);'
        try:
            j = json.loads(re.search(ptrn, resp.text, re.DOTALL).group(1))
            data = j['context']['dispatcher']['stores']['HistoricalPriceStore']
        except KeyError:
            msg = 'No data fetched for symbol {} using {}'
            raise RemoteDataError(msg.format(symbol, self.__class__.__name__))

        # price data
        prices = DataFrame(data['prices'])
        prices.columns = map(str.capitalize, prices.columns)
        prices['Date'] = to_datetime(prices['Date'], unit='s').dt.date

        prices = prices[prices['Data'].isnull()]
        prices = prices[['Date', 'High', 'Low', 'Open', 'Close', 'Volume',
                         'Adjclose']]
        prices = prices.rename(columns={'Adjclose': 'Adj Close'})

        dfs = {'prices': prices}

        # dividends & splits data
        if self.get_actions:
            actions = DataFrame(data['eventsData'])
            actions.columns = map(str.capitalize, actions.columns)
            actions['Date'] = to_datetime(actions['Date'], unit='s').dt.date

            types = actions['Type'].unique()
            if 'DIVIDEND' in types:
                divs = actions[actions.Type == 'DIVIDEND'].copy()
                divs = divs[['Date', 'Amount']].reset_index(drop=True)
                dfs['dividends'] = divs

            if 'SPLIT' in types:
                splits = actions[actions.Type == 'SPLIT'].copy()
                splits['SplitRatio'] = splits['Splitratio'].apply(
                    lambda x: eval(x))
                splits = splits[['Date', 'Denominator', 'Numerator',
                                 'SplitRatio']]
                splits = splits.reset_index(drop=True)
                dfs['splits'] = splits

        return dfs

    def _dl_mult_symbols(self, symbols):
        stocks = {}
        failed = []
        passed = []
        for sym in symbols:
            try:
                dfs = self._read_one_data(sym)
                for k in dfs:
                    dfs[k]['Ticker'] = sym
                    if k not in stocks:
                        stocks[k] = []
                    stocks[k].append(dfs[k])
                passed.append(sym)
            except IOError:
                msg = 'Failed to read symbol: {0!r}, replacing with NaN.'
                warnings.warn(msg.format(sym), SymbolWarning)
                failed.append(sym)

        if len(passed) == 0:
            msg = "No data fetched using {0!r}"
            raise RemoteDataError(msg.format(self.__class__.__name__))
        else:
            for k in stocks:
                dfs[k] = concat(stocks[k]).set_index(['Ticker', 'Date'])
            return dfs


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
