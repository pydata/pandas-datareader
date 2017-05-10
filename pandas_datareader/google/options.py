import datetime as dt
import re
import json

from pandas import DataFrame, MultiIndex

from pandas_datareader.base import _OptionBaseReader


class Options(_OptionBaseReader):
    """
    ***Experimental***
    This class fetches call/put data for a given stock/expiry month.

    It is instantiated with a string representing the ticker symbol.

    The class has the following methods:
        get_options_data(month, year, expiry)
        get_call_data(month, year, expiry)
        get_put_data(month, year, expiry)
        get_near_stock_price(opt_frame, above_below)
        get_all_data(call, put)
        get_forward_data(months, call, put) (deprecated)

    Examples
    --------
    # Instantiate object with ticker
    >>> aapl = Options('aapl')

    # Fetch next expiry call data
    >>> calls = aapl.get_call_data()

    # Can now access aapl.calls instance variable
    >>> aapl.calls

    # Fetch next expiry put data
    >>> puts = aapl.get_put_data()

    # Can now access aapl.puts instance variable
    >>> aapl.puts

    # cut down the call data to be 3 below and 3 above the stock price.
    >>> cut_calls = aapl.get_near_stock_price(call=True, above_below=3)

    # Fetch call and put data with expiry from now to 8 months out
    >>> forward_data = aapl.get_forward_data(8, call=True, put=True)

    # Fetch all call and put data
    >>> all_data = aapl.get_all_data()
    """

    _OPTIONS_BASE_URL = "http://www.google.com/finance/option_chain?q={sym}" \
                        "&expd={day}&expm={month}&expy={year}&output=json"

    def get_options_data(self, month=None, year=None, expiry=None):
        """
        ***Experimental***
        Gets call/put data for the stock with the expiration data in the
        given month and year

        Parameters
        ----------
        month : number, int, optional (default=None)
            The month the options expire. This should be either 1 or 2
            digits.

        year : number, int, optional (default=None)
            The year the options expire. This should be a 4 digit int.

        expiry : date-like or convertible or
                 list-like object, optional (default=None)
            The date (or dates) when options expire (defaults to current month)

        Returns
        -------
        pandas.DataFrame
            A DataFrame with requested options data.

            Index:
                Strike: Option strike, int
                Expiry: Option expiry, Timestamp
                Type: Call or Put, string
                Symbol: Option symbol as reported on Google, string
            Columns:
                Last: Last option price, float
                Chg: Change from prior day, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp

        Notes
        -----
        Note: Format of returned data frame is dependent on
              Google and may change.

            >>> goog = Options('goog', 'google')  # Create object
            >>> goog.get_options_data(expiry=goog.expiry_dates[0])  # Get data

        """
        if month is not None or year is not None:
            raise NotImplementedError('month and year parameters '
                                      'cannot be used')
        if expiry is None:
            raise ValueError('expiry has to be set')
        d = self._load_data(expiry)
        return self._process_data(d, expiry)

    @property
    def expiry_dates(self):
        """
        Returns a list of available expiry dates
        """
        try:
            return self._expiry_dates
        except AttributeError:
            # has to be a non-valid date, to trigger returning 'expirations'
            d = self._load_data(dt.datetime(2016, 1, 3))
            self._expiry_dates = [dt.date(x['y'], x['m'], x['d'])
                                  for x in d['expirations']]
        return self._expiry_dates

    def _load_data(self, expiry):
        url = self._OPTIONS_BASE_URL.format(
            sym=self.symbol, day=expiry.day,
            month=expiry.month, year=expiry.year)
        s = re.sub(r'(\w+):', '"\\1":', self._read_url_as_StringIO(url).read())
        return json.loads(s)

    def _process_data(self, jd, expiry):
        """
        Parse JSON data into the DataFrame

        """
        now = dt.datetime.now()

        columns = ['Last', 'Bid', 'Ask', 'Chg', 'PctChg', 'Vol',
                   'Open_Int', 'Root', 'Underlying_Price', 'Quote_Time']
        indexes = ['Strike', 'Expiry', 'Type', 'Symbol']
        rows_list, index = self._process_rows(jd, now, expiry)
        df = DataFrame(rows_list, columns=columns,
                       index=MultiIndex.from_tuples(index, names=indexes))

        # Make dtype consistent, requires float64 as there can be NaNs
        df['Vol'] = df['Vol'].astype('float64')
        df['Open_Int'] = df['Open_Int'].astype('float64')

        return df.sort_index()

    def _process_rows(self, jd, now, expiry):
        rows_list = []
        index = []
        for key, typ in [['calls', 'call'], ['puts', 'put']]:
            for row in jd[key]:
                d = {}
                for dkey, rkey, ntype in [('Last', 'p', float),
                                          ('Bid', 'b', float),
                                          ('Ask', 'a', float),
                                          ('Chg', 'c', float),
                                          ('PctChg', 'cp', float),
                                          ('Vol', 'vol', int),
                                          ('Open_Int', 'oi', int)]:
                    try:
                        d[dkey] = ntype(row[rkey].replace(',', ''))
                    except (KeyError, ValueError):
                        pass
                d['Root'] = self.symbol
                d['Underlying_Price'] = jd['underlying_price']
                d['Quote_Time'] = now
                rows_list.append(d)
                index.append((float(row['strike'].replace(',', '')),
                              expiry, typ, row['s']))
        return rows_list, index
