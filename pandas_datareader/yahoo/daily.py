from __future__ import division

import json
import re
import time
from pandas import (DataFrame, to_datetime, notnull, isnull)
from pandas_datareader._utils import RemoteDataError
from pandas_datareader.base import _DailyBaseReader


class YahooDailyReader(_DailyBaseReader):
    """
    Returns DataFrame of with historical over date range,
    start to end.
    To avoid being penalized by Yahoo! Finance servers, pauses between
    downloading 'chunks' of symbols can be specified.

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
        'm' for monthly.
    get_actions : bool, default False
        If True, adds Dividend and Split columns to dataframe.
    adjust_dividends: bool, default false
        If True, adjusts dividends for splits.
    """

    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.35, session=None, adjust_price=False,
                 ret_index=False, chunksize=1, interval='d',
                 get_actions=False, adjust_dividends=False):
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
        self._get_actions = get_actions

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
        self.adjust_dividends = adjust_dividends

    @property
    def get_actions(self):
        return self._get_actions

    @property
    def url(self):
        return 'https://finance.yahoo.com/quote/{}/history'

    # Test test_get_data_interval() crashed because of this issue, probably
    # whole yahoo part of package wasn't
    # working properly
    def _get_params(self, symbol):
        # This needed because yahoo returns data shifted by 4 hours ago.
        four_hours_in_seconds = 14400
        unix_start = int(time.mktime(self.start.timetuple()))
        unix_start += four_hours_in_seconds
        day_end = self.end.replace(hour=23, minute=59, second=59)
        unix_end = int(time.mktime(day_end.timetuple()))
        unix_end += four_hours_in_seconds

        params = {
            'period1': unix_start,
            'period2': unix_end,
            'interval': self.interval,
            'frequency': self.interval,
            'filter': 'history',
            'symbol': symbol
        }
        return params

    def _read_one_data(self, url, params):
        """ read one data from specified symbol """

        symbol = params['symbol']
        del params['symbol']
        url = url.format(symbol)

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
        prices.columns = [col.capitalize() for col in prices.columns]
        prices['Date'] = to_datetime(
            to_datetime(prices['Date'], unit='s').dt.date)

        if 'Data' in prices.columns:
            prices = prices[prices['Data'].isnull()]
        prices = prices[['Date', 'High', 'Low', 'Open', 'Close', 'Volume',
                         'Adjclose']]
        prices = prices.rename(columns={'Adjclose': 'Adj Close'})

        prices = prices.set_index('Date')
        prices = prices.sort_index().dropna(how='all')

        if self.ret_index:
            prices['Ret_Index'] = \
                _calc_return_index(prices['Adj Close'])
        if self.adjust_price:
            prices = _adjust_prices(prices)

        # dividends & splits data
        if self.get_actions and data['eventsData']:

            actions = DataFrame(data['eventsData'])
            actions.columns = [col.capitalize() for col in actions.columns]
            actions['Date'] = to_datetime(
                to_datetime(actions['Date'], unit='s').dt.date)

            types = actions['Type'].unique()
            if 'DIVIDEND' in types:
                divs = actions[actions.Type == 'DIVIDEND'].copy()
                divs = divs[['Date', 'Amount']].reset_index(drop=True)
                divs = divs.set_index('Date')
                divs = divs.rename(columns={'Amount': 'Dividends'})
                prices = prices.join(divs, how='outer')

            if 'SPLIT' in types:
                splits = actions[actions.Type == 'SPLIT'].copy()
                splits['SplitRatio'] = splits['Splitratio'].apply(
                    lambda x: eval(x))
                splits = splits.reset_index(drop=True)
                splits = splits.set_index('Date')
                splits['Splits'] = 1.0 / splits['SplitRatio']
                prices = prices.join(splits['Splits'], how='outer')

                if 'DIVIDEND' in types and self.adjust_dividends:
                    # Adjust dividends to deal with splits
                    adj = prices['Splits'].sort_index(ascending=False).fillna(
                        1).cumprod()
                    prices['Dividends'] = prices['Dividends'] * adj

        return prices


def _adjust_prices(hist_data, price_list=None):
    """
    Return modifed DataFrame with adjusted prices based on
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
    mask = notnull(df.iloc[1]) & isnull(df.iloc[0])
    if mask:
        df.loc[df.index[0]] = 1

    # Check for first stock listings after starting date of index in ret_index
    # If True, find first_valid_index and set previous entry to 1.
    if not mask:
        tstamp = df.first_valid_index()
        t_idx = df.index.get_loc(tstamp) - 1
        df.iloc[t_idx] = 1

    return df
