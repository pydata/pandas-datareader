import warnings
import datetime as dt
import numpy as np

from pandas.io.html import read_html
from pandas import to_datetime
from pandas import concat, DatetimeIndex, Series
from pandas.tseries.offsets import MonthEnd
from pandas.util.testing import _network_error_classes
from pandas.io.parsers import TextParser
from pandas import DataFrame

from pandas_datareader._utils import RemoteDataError
from pandas_datareader.base import _BaseReader

# Items needed for options class
CUR_MONTH = dt.datetime.now().month
CUR_YEAR = dt.datetime.now().year
CUR_DAY = dt.datetime.now().day

def _two_char(s):
    return '{0:0>2}'.format(s)

def _unpack(row, kind='td'):
    return [val.text_content().strip() for val in row.findall(kind)]

def _parse_options_data(table):
    header = table.findall('thead/tr')
    header = _unpack(header[0], kind='th')
    rows = table.findall('tbody/tr')
    data = [_unpack(r) for r in rows]
    if len(data) > 0:
        return TextParser(data, names=header).get_chunk()
    else: #Empty table
        return DataFrame(columns=header)

class Options(_BaseReader):
    """
    ***Experimental***
    This class fetches call/put data for a given stock/expiry month.

    It is instantiated with a string representing the ticker symbol.

    The class has the following methods:
        get_options_data:(month, year, expiry)
        get_call_data:(month, year, expiry)
        get_put_data: (month, year, expiry)
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

    _OPTIONS_BASE_URL = 'http://finance.yahoo.com/q/op?s={sym}'
    _FINANCE_BASE_URL = 'http://finance.yahoo.com'

    def __init__(self, symbol, session=None):
        """ Instantiates options_data with a ticker saved as symbol """
        self.symbol = symbol.upper()
        super(Options, self).__init__(symbols=symbol, session=session)

    def get_options_data(self, month=None, year=None, expiry=None):
        """
        ***Experimental***
        Gets call/put data for the stock with the expiration data in the
        given month and year

        Parameters
        ----------
        month : number, int, optional(default=None)
            The month the options expire. This should be either 1 or 2
            digits.

        year : number, int, optional(default=None)
            The year the options expire. This should be a 4 digit int.

        expiry : date-like or convertible or list-like object, optional (default=None)
            The date (or dates) when options expire (defaults to current month)

        Returns
        -------
        pandas.DataFrame
            A DataFrame with requested options data.

            Index:
                Strike: Option strike, int
                Expiry: Option expiry, Timestamp
                Type: Call or Put, string
                Symbol: Option symbol as reported on Yahoo, string
            Columns:
                Last: Last option price, float
                Chg: Change from prior day, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares, otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp

        Notes
        -----
        Note: Format of returned data frame is dependent on Yahoo and may change.

        When called, this function will add instance variables named
        calls and puts. See the following example:

            >>> aapl = Options('aapl', 'yahoo')  # Create object
            >>> aapl.calls  # will give an AttributeError
            >>> aapl.get_options()  # Get data and set ivars
            >>> aapl.calls  # Doesn't throw AttributeError

        Also note that aapl.calls and appl.puts will always be the calls
        and puts for the next expiry. If the user calls this method with
        a different expiry, the ivar will be named callsYYMMDD or putsYYMMDD,
        where YY, MM and DD are, respectively, two digit representations of
        the year, month and day for the expiry of the options.

        """
        return concat([f(month, year, expiry)
                       for f in (self.get_put_data,
                                 self.get_call_data)]).sortlevel()

    def _get_option_frames_from_yahoo(self, expiry):
        url = self._yahoo_url_from_expiry(expiry)
        option_frames = self._option_frames_from_url(url)
        frame_name = '_frames' + self._expiry_to_string(expiry)
        setattr(self, frame_name, option_frames)
        return option_frames

    @staticmethod
    def _expiry_to_string(expiry):
        m1 = _two_char(expiry.month)
        d1 = _two_char(expiry.day)
        return str(expiry.year)[-2:] + m1 + d1

    def _yahoo_url_from_expiry(self, expiry):
        try:
            expiry_links = self._expiry_links

        except AttributeError: # pragma: no cover
            _, expiry_links = self._get_expiry_dates_and_links()

        return self._FINANCE_BASE_URL + expiry_links[expiry]

    def _option_frames_from_url(self, url):

        root = self._parse_url(url)
        calls = root.xpath('//*[@id="optionsCallsTable"]/div[2]/div/table')[0]
        puts = root.xpath('//*[@id="optionsPutsTable"]/div[2]/div/table')[0]

        if not hasattr(self, 'underlying_price'):
            try:
                self.underlying_price, self.quote_time = self._underlying_price_and_time_from_url(url)
            except IndexError:
                self.underlying_price, self.quote_time = np.nan, np.nan

        calls = _parse_options_data(calls)
        puts = _parse_options_data(puts)

        calls = self._process_data(calls, 'call')
        puts = self._process_data(puts, 'put')

        return {'calls': calls, 'puts': puts}

    def _underlying_price_and_time_from_url(self, url):
        root = self._parse_url(url)
        underlying_price = self._underlying_price_from_root(root)
        quote_time = self._quote_time_from_root(root)
        return underlying_price, quote_time

    @staticmethod
    def _underlying_price_from_root(root):
        underlying_price = root.xpath('.//*[@class="time_rtq_ticker Fz-30 Fw-b"]')[0]\
            .getchildren()[0].text
        underlying_price = underlying_price.replace(',', '') #GH11

        try:
            underlying_price = float(underlying_price)
        except ValueError:
            underlying_price = np.nan

        return underlying_price

    @staticmethod
    def _quote_time_from_root(root):
        #Gets the time of the quote, note this is actually the time of the underlying price.
        try:
            quote_time_text = root.xpath('.//*[@class="time_rtq Fz-m"]')[0].getchildren()[1].getchildren()[0].text
            ##TODO: Enable timezone matching when strptime can match EST with %Z
            quote_time_text = quote_time_text.split(' ')[0]
            quote_time = dt.datetime.strptime(quote_time_text, "%I:%M%p")
            quote_time = quote_time.replace(year=CUR_YEAR, month=CUR_MONTH, day=CUR_DAY)
        except ValueError:
            quote_time = np.nan

        return quote_time

    def _get_option_data(self, expiry, name):
        frame_name = '_frames' + self._expiry_to_string(expiry)

        try:
            frames = getattr(self, frame_name)
        except AttributeError:
            frames = self._get_option_frames_from_yahoo(expiry)

        option_data = frames[name]
        if expiry != self.expiry_dates[0]:
            name += self._expiry_to_string(expiry)

        setattr(self, name, option_data)
        return option_data

    def get_call_data(self, month=None, year=None, expiry=None):
        """
        ***Experimental***
        Gets call/put data for the stock with the expiration data in the
        given month and year

        Parameters
        ----------
        month : number, int, optional(default=None)
            The month the options expire. This should be either 1 or 2
            digits.

        year : number, int, optional(default=None)
            The year the options expire. This should be a 4 digit int.

        expiry : date-like or convertible or list-like object, optional (default=None)
            The date (or dates) when options expire (defaults to current month)

        Returns
        -------
        call_data: pandas.DataFrame
            A DataFrame with requested options data.

            Index:
                Strike: Option strike, int
                Expiry: Option expiry, Timestamp
                Type: Call or Put, string
                Symbol: Option symbol as reported on Yahoo, string
            Columns:
                Last: Last option price, float
                Chg: Change from prior day, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares, otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp

        Notes
        -----
        Note: Format of returned data frame is dependent on Yahoo and may change.

        When called, this function will add instance variables named
        calls and puts. See the following example:

            >>> aapl = Options('aapl', 'yahoo')  # Create object
            >>> aapl.calls  # will give an AttributeError
            >>> aapl.get_call_data()  # Get data and set ivars
            >>> aapl.calls  # Doesn't throw AttributeError

        Also note that aapl.calls will always be the calls for the next
        expiry. If the user calls this method with a different month
        or year, the ivar will be named callsYYMMDD where YY, MM and DD are,
        respectively, two digit representations of the year, month and day
        for the expiry of the options.
        """
        expiry = self._try_parse_dates(year, month, expiry)
        return self._get_data_in_date_range(expiry, call=True, put=False)

    def get_put_data(self, month=None, year=None, expiry=None):
        """
        ***Experimental***
        Gets put data for the stock with the expiration data in the
        given month and year

        Parameters
        ----------
        month : number, int, optional(default=None)
            The month the options expire. This should be either 1 or 2
            digits.

        year : number, int, optional(default=None)
            The year the options expire. This should be a 4 digit int.

        expiry : date-like or convertible or list-like object, optional (default=None)
            The date (or dates) when options expire (defaults to current month)

        Returns
        -------
        put_data: pandas.DataFrame
            A DataFrame with requested options data.

            Index:
                Strike: Option strike, int
                Expiry: Option expiry, Timestamp
                Type: Call or Put, string
                Symbol: Option symbol as reported on Yahoo, string
            Columns:
                Last: Last option price, float
                Chg: Change from prior day, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares, otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp

        Notes
        -----
        Note: Format of returned data frame is dependent on Yahoo and may change.

        When called, this function will add instance variables named
        puts. See the following example:

            >>> aapl = Options('aapl')  # Create object
            >>> aapl.puts  # will give an AttributeError
            >>> aapl.get_put_data()  # Get data and set ivars
            >>> aapl.puts  # Doesn't throw AttributeError

                    return self.__setattr__(self, str(str(x) + str(y)))

        Also note that aapl.puts will always be the puts for the next
        expiry. If the user calls this method with a different month
        or year, the ivar will be named putsYYMMDD where YY, MM and DD are,
        respectively, two digit representations of the year, month and day
        for the expiry of the options.
        """
        expiry = self._try_parse_dates(year, month, expiry)
        return self._get_data_in_date_range(expiry, put=True, call=False)

    def get_near_stock_price(self, above_below=2, call=True, put=False,
                             month=None, year=None, expiry=None):
        """
        ***Experimental***
        Returns a data frame of options that are near the current stock price.

        Parameters
        ----------
        above_below : number, int, optional (default=2)
            The number of strike prices above and below the stock price that
            should be taken

        call : bool
            Tells the function whether or not it should be using calls

        put : bool
            Tells the function weather or not it should be using puts

        month : number, int, optional(default=None)
            The month the options expire. This should be either 1 or 2
            digits.

        year : number, int, optional(default=None)
            The year the options expire. This should be a 4 digit int.

        expiry : date-like or convertible or list-like object, optional (default=None)
            The date (or dates) when options expire (defaults to current month)

        Returns
        -------
        chopped: DataFrame
            The resultant DataFrame chopped down to be 2 * above_below + 1 rows
            desired. If there isn't data as far out as the user has asked for
            then

         Note: Format of returned data frame is dependent on Yahoo and may change.

        """
        expiry = self._try_parse_dates(year, month, expiry)
        data = self._get_data_in_date_range(expiry, call=call, put=put)
        return self.chop_data(data, above_below, self.underlying_price)

    def chop_data(self, df, above_below=2, underlying_price=None):
        """Returns a data frame only options that are near the current stock price."""

        if not underlying_price:
            try:
                underlying_price = self.underlying_price
            except AttributeError:
                underlying_price = np.nan

        max_strike = max(df.index.get_level_values('Strike'))
        min_strike = min(df.index.get_level_values('Strike'))

        if not np.isnan(underlying_price) and min_strike < underlying_price < max_strike:
            start_index = np.where(df.index.get_level_values('Strike')
                                   > underlying_price)[0][0]

            get_range = slice(start_index - above_below,
                              start_index + above_below + 1)
            df = df[get_range].dropna(how='all')

        return df

    def _try_parse_dates(self, year, month, expiry):
        """
        Validates dates provided by user.  Ensures the user either provided both a month and a year or an expiry.

        Parameters
        ----------
        year : int
            Calendar year

        month : int
            Calendar month

        expiry : date-like or convertible, (preferred)
            Expiry date

        Returns
        -------
        list of expiry dates (datetime.date)
        """

        #Checks if the user gave one of the month or the year but not both and did not provide an expiry:
        if (month is not None and year is None) or (month is None and year is not None) and expiry is None:
            msg = "You must specify either (`year` and `month`) or `expiry` " \
                  "or none of these options for the next expiry."
            raise ValueError(msg)

        if expiry is not None:
            if hasattr(expiry, '__iter__'):
                expiry = [self._validate_expiry(exp) for exp in expiry]
            else:
                expiry = [self._validate_expiry(expiry)]

            if len(expiry) == 0:
                raise ValueError('No expiries available for given input.')

        elif year is None and month is None:
            #No arguments passed, provide next expiry
            year = CUR_YEAR
            month = CUR_MONTH
            expiry = dt.date(year, month, 1)
            expiry = [self._validate_expiry(expiry)]

        else:
            #Year and month passed, provide all expiries in that month
            expiry = [expiry for expiry in self.expiry_dates if expiry.year == year and expiry.month == month]
            if len(expiry) == 0:
                raise ValueError('No expiries available in %s-%s' % (year, month))

        return expiry

    def _validate_expiry(self, expiry):
        """Ensures that an expiry date has data available on Yahoo
        If the expiry date does not have options that expire on that day, return next expiry"""

        expiry_dates = self.expiry_dates
        expiry = to_datetime(expiry)
        if hasattr(expiry, 'date'):
            expiry = expiry.date()

        if expiry in expiry_dates:
            return expiry
        else:
            index = DatetimeIndex(expiry_dates).order()
            return index[index.date >= expiry][0].date()

    def get_forward_data(self, months, call=True, put=False, near=False,
            above_below=2): # pragma: no cover
        """
        ***Experimental***
        Gets either call, put, or both data for months starting in the current
        month and going out in the future a specified amount of time.

        Parameters
        ----------
        months : number, int
            How many months to go out in the collection of the data. This is
            inclusive.

        call : bool, optional (default=True)
            Whether or not to collect data for call options

        put : bool, optional (default=False)
            Whether or not to collect data for put options.

        near : bool, optional (default=False)
            Whether this function should get only the data near the
            current stock price. Uses Options.get_near_stock_price

        above_below : number, int, optional (default=2)
            The number of strike prices above and below the stock price that
            should be taken if the near option is set to True

        Returns
        -------
        pandas.DataFrame
            A DataFrame with requested options data.

            Index:
                Strike: Option strike, int
                Expiry: Option expiry, Timestamp
                Type: Call or Put, string
                Symbol: Option symbol as reported on Yahoo, string
            Columns:
                Last: Last option price, float
                Chg: Change from prior day, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares, otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp

                Note: Format of returned data frame is dependent on Yahoo and may change.

        """
        warnings.warn("get_forward_data() is deprecated", FutureWarning)
        end_date = dt.date.today() + MonthEnd(months)
        dates = (date for date in self.expiry_dates if date <= end_date.date())
        data = self._get_data_in_date_range(dates, call=call, put=put)
        if near:
            data = self.chop_data(data, above_below=above_below)
        return data

    def get_all_data(self, call=True, put=True):
        """
        ***Experimental***
        Gets either call, put, or both data for all available months starting
        in the current month.

        Parameters
        ----------
        call : bool, optional (default=True)
            Whether or not to collect data for call options

        put : bool, optional (default=True)
            Whether or not to collect data for put options.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with requested options data.

            Index:
                Strike: Option strike, int
                Expiry: Option expiry, Timestamp
                Type: Call or Put, string
                Symbol: Option symbol as reported on Yahoo, string
            Columns:
                Last: Last option price, float
                Chg: Change from prior day, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares, otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp

        Note: Format of returned data frame is dependent on Yahoo and may change.

        """

        expiry_dates = self.expiry_dates
        return self._get_data_in_date_range(dates=expiry_dates, call=call, put=put)

    def _get_data_in_date_range(self, dates, call=True, put=True):

        to_ret = Series({'calls': call, 'puts': put})
        to_ret = to_ret[to_ret].index
        data = []

        for name in to_ret:
            for expiry_date in dates:
                nam = name + self._expiry_to_string(expiry_date)
                try:  # Try to access on the instance
                    frame = getattr(self, nam)
                except AttributeError:
                    frame = self._get_option_data(expiry=expiry_date, name=name)
                data.append(frame)

        return concat(data).sortlevel()

    @property
    def expiry_dates(self):
        """
        Returns a list of available expiry dates
        """
        try:
            expiry_dates = self._expiry_dates
        except AttributeError:
            expiry_dates, _ = self._get_expiry_dates_and_links()
        return expiry_dates

    def _get_expiry_dates_and_links(self):
        """
        Gets available expiry dates.

        Returns
        -------
        Tuple of:
        List of datetime.date objects
        Dict of datetime.date objects as keys and corresponding links
        """

        url = self._OPTIONS_BASE_URL.format(sym=self.symbol)
        root = self._parse_url(url)

        try:
            links = root.xpath('//*[@id="options_menu"]/form/select/option')
        except IndexError: # pragma: no cover
            raise RemoteDataError('Expiry dates not available')

        expiry_dates = [dt.datetime.strptime(element.text, "%B %d, %Y").date() for element in links]
        links = [element.attrib['data-selectbox-link'] for element in links]

        if len(expiry_dates) == 0:
            raise RemoteDataError('Data not available') # pragma: no cover

        expiry_links = dict(zip(expiry_dates, links))
        self._expiry_links = expiry_links
        self._expiry_dates = expiry_dates
        return expiry_dates, expiry_links

    def _parse_url(self, url):
        """
        Downloads and parses a URL, returns xml root.

        """
        try:
            from lxml.html import parse
        except ImportError: # pragma: no cover
            raise ImportError("Please install lxml if you want to use the "
                              "{0!r} class".format(self.__class__.__name__))
        doc = parse(self._read_url_as_StringIO(url))
        root = doc.getroot()
        if root is None: # pragma: no cover
            raise RemoteDataError("Parsed URL {0!r} has no root"
                                      "element".format(url))
        return root

    def _process_data(self, frame, type):
        """
        Adds columns for Expiry, IsNonstandard (ie: deliverable is not 100 shares)
        and Tag (the tag indicating what is actually deliverable, None if standard).

        """
        frame.columns = ['Strike', 'Symbol', 'Last', 'Bid', 'Ask', 'Chg', 'PctChg', 'Vol', 'Open_Int', 'IV']
        frame["Rootexp"] = frame.Symbol.str[0:-9]
        frame["Root"] = frame.Rootexp.str[0:-6]
        frame["Expiry"] = to_datetime(frame.Rootexp.str[-6:])
        #Removes dashes in equity ticker to map to option ticker.
        #Ex: BRK-B to BRKB140517C00100000
        frame["IsNonstandard"] = frame['Root'] != self.symbol.replace('-', '')
        del frame["Rootexp"]
        frame["Underlying"] = self.symbol
        try:
            frame['Underlying_Price'] = self.underlying_price
            frame["Quote_Time"] = self.quote_time
        except AttributeError: # pragma: no cover
            frame['Underlying_Price'] = np.nan
            frame["Quote_Time"] = np.nan
        frame.rename(columns={'Open Int': 'Open_Int'}, inplace=True)
        frame['IV'] = frame['IV'].str.replace(',','').str.strip('%').astype(float)/100
        frame['PctChg'] = frame['PctChg'].str.replace(',','').str.strip('%').astype(float)/100
        frame['Type'] = type
        frame.set_index(['Strike', 'Expiry', 'Type', 'Symbol'], inplace=True)

        return frame
