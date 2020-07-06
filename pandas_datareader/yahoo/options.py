import datetime as dt
import json
import warnings

import numpy as np
from pandas import (
    DataFrame,
    DatetimeIndex,
    MultiIndex,
    Series,
    concat,
    to_datetime,
)
from pandas.io.json import read_json
from pandas.tseries.offsets import MonthEnd

from pandas_datareader._utils import RemoteDataError
from pandas_datareader.base import _OptionBaseReader

# Items needed for options class
CUR_MONTH = dt.datetime.now().month
CUR_YEAR = dt.datetime.now().year
CUR_DAY = dt.datetime.now().day


def _parse_options_data(jd):
    return read_json(jd)


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

    _OPTIONS_BASE_URL = "https://query1.finance.yahoo.com/" "v7/finance/options/{sym}"

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
                Symbol: Option symbol as reported on Yahoo, string
            Columns:
                Last: Last option price, float
                Chg: Change from prior day, float
                PctChg: Change from prior day in percent, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares,
                               otherwise False
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp
                Last_Trade_Date: Time of the last trade for this expiry
                                 and strike, Timestamp
                IV: Implied volatility, float
                JSON: Parsed json object, json
                    Useful to extract other returned key/value pairs as needed

        Notes
        -----
        Note: Format of returned DataFrame is dependent
              on Yahoo and may change.

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
        return concat(
            [f(month, year, expiry) for f in (self.get_put_data, self.get_call_data)]
        ).sort_index()

    def _option_from_url(self, url):

        jd = self._parse_url(url)
        result = jd["optionChain"]["result"]
        try:
            calls = result["options"]["calls"]
            puts = result["options"]["puts"]
        except IndexError:
            raise RemoteDataError("Option json not available " "for url: %s" % url)

        self.underlying_price = (
            result["quote"]["regularMarketPrice"]
            if result["quote"]["marketState"] == "PRE"
            else result["quote"]["preMarketPrice"]
        )
        quote_unix_time = (
            result["quote"]["regularMarketTime"]
            if result["quote"]["marketState"] == "PRE"
            else result["quote"]["preMarketTime"]
        )
        self.quote_time = dt.datetime.fromtimestamp(quote_unix_time)

        calls = _parse_options_data(calls)
        puts = _parse_options_data(puts)

        calls = self._process_data(calls)
        puts = self._process_data(puts)

        return {"calls": calls, "puts": puts}

    def _get_option_data(self, expiry, name):
        frame_name = "_frames" + self._expiry_to_string(expiry)

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

        expiry : date-like or convertible or
                 list-like object, optional (default=None)
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
                PctChg: Change from prior day in percent, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares,
                               otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp
                Last_Trade_Date: Time of the last trade for this expiry
                                 and strike, Timestamp
                IV: Implied volatility, float
                JSON: Parsed json object, json
                    Useful to extract other returned key/value pairs as needed

        Notes
        -----
        Note: Format of returned DataFrame is dependent
              on Yahoo and may change.

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

        expiry : date-like or convertible or
                 list-like object, optional (default=None)
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
                PctChg: Change from prior day in percent, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares,
                               otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp
                Last_Trade_Date: Time of the last trade for this expiry
                                 and strike, Timestamp
                IV: Implied volatility, float
                JSON: Parsed json object, json
                    Useful to extract other returned key/value pairs as needed

        Notes
        -----
        Note: Format of returned DataFrame is dependent
              on Yahoo and may change.

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

    def get_near_stock_price(
        self, above_below=2, call=True, put=False, month=None, year=None, expiry=None
    ):
        """
        ***Experimental***
        Returns a DataFrame of options that are near the current stock price.

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

        expiry : date-like or convertible or list-like object,
                 optional (default=None)
            The date (or dates) when options expire (defaults to current month)

        Returns
        -------
        chopped: DataFrame
            The resultant DataFrame chopped down to be 2 * above_below + 1 rows
            desired. If there isn't data as far out as the user has asked for
            then

            Index:
                Strike: Option strike, int
                Expiry: Option expiry, Timestamp
                Type: Call or Put, string
                Symbol: Option symbol as reported on Yahoo, string
            Columns:
                Last: Last option price, float
                Chg: Change from prior day, float
                PctChg: Change from prior day in percent, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares,
                               otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp
                Last_Trade_Date: Time of the last trade for this expiry
                                 and strike, Timestamp
                IV: Implied volatility, float
                JSON: Parsed json object, json
                    Useful to extract other returned key/value pairs as needed

         Note: Format of returned DataFrame is dependent
               on Yahoo and may change.

        """
        expiry = self._try_parse_dates(year, month, expiry)
        data = self._get_data_in_date_range(expiry, call=call, put=put)
        underlying_price = data.Underlying_Price[0]
        return self._chop_data(data, above_below, underlying_price)

    def _chop_data(self, df, above_below=2, underlying_price=None):
        """
        Returns a DataFrame only options that are near the current stock price.
        """

        if not underlying_price:
            try:
                underlying_price = self.underlying_price
            except AttributeError:
                underlying_price = np.nan

        max_strike = max(df.index.get_level_values("Strike"))
        min_strike = min(df.index.get_level_values("Strike"))

        if (
            not np.isnan(underlying_price)
            and min_strike < underlying_price < max_strike
        ):
            start_index = np.where(
                df.index.get_level_values("Strike") > underlying_price
            )[0][0]

            get_range = slice(start_index - above_below, start_index + above_below + 1)
            df = df[get_range].dropna(how="all")

        return df

    def _try_parse_dates(self, year, month, expiry):
        """
        Validates dates provided by user. Ensures the user either provided
        both a month and a year or an expiry.

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

        # Checks if the user gave one of the month or the year
        # but not both and did not provide an expiry:
        if (
            (month is not None and year is None)
            or (month is None and year is not None)
            and expiry is None
        ):
            msg = (
                "You must specify either (`year` and `month`) or `expiry` "
                "or none of these options for the next expiry."
            )
            raise ValueError(msg)

        if expiry is not None:
            if hasattr(expiry, "__iter__"):
                expiry = [self._validate_expiry(exp) for exp in expiry]
            else:
                expiry = [self._validate_expiry(expiry)]

            if len(expiry) == 0:
                raise ValueError("No expiries available for given input.")

        elif year is None and month is None:
            # No arguments passed, provide next expiry
            year = CUR_YEAR
            month = CUR_MONTH
            expiry = dt.date(year, month, 1)
            expiry = [self._validate_expiry(expiry)]

        else:
            # Year and month passed, provide all expiries in that month
            expiry = [
                expiry
                for expiry in self.expiry_dates
                if expiry.year == year and expiry.month == month
            ]
            if len(expiry) == 0:
                raise ValueError("No expiries available " "in %s-%s" % (year, month))

        return expiry

    def _validate_expiry(self, expiry):
        """
        Ensures that an expiry date has data available on Yahoo.

        If the expiry date does not have options that expire on that day,
        return next expiry.
        """

        expiry_dates = self.expiry_dates
        expiry = to_datetime(expiry)
        if hasattr(expiry, "date"):
            expiry = expiry.date()

        if expiry in expiry_dates:
            return expiry
        else:
            index = DatetimeIndex(expiry_dates).sort_values()
            return index[index.date >= expiry][0].date()

    def get_forward_data(
        self, months, call=True, put=False, near=False, above_below=2
    ):  # pragma: no cover
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
                PctChg: Change from prior day in percent, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares,
                               otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp
                Last_Trade_Date: Time of the last trade for this expiry
                                 and strike, Timestamp
                IV: Implied volatility, float
                JSON: Parsed json object, json
                    Useful to extract other returned key/value pairs as needed

                Note: Format of returned DataFrame is dependent
                      on Yahoo and may change.

        """
        warnings.warn("get_forward_data() is deprecated", FutureWarning)
        end_date = dt.date.today() + MonthEnd(months)
        dates = [date for date in self.expiry_dates if date <= end_date.date()]
        data = self._get_data_in_date_range(dates, call=call, put=put)
        if near:
            data = self._chop_data(data, above_below=above_below)
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
                PctChg: Change from prior day in percent, float
                Bid: Bid price, float
                Ask: Ask price, float
                Vol: Volume traded, int64
                Open_Int: Open interest, int64
                IsNonstandard: True if the the deliverable is not 100 shares,
                               otherwise false
                Underlying: Ticker of the underlying security, string
                Underlying_Price: Price of the underlying security, float64
                Quote_Time: Time of the quote, Timestamp
                Last_Trade_Date: Time of the last trade for this expiry
                                 and strike, Timestamp
                IV: Implied volatility, float
                JSON: Parsed json object, json
                    Useful to extract other returned key/value pairs as needed

        Note: Format of returned DataFrame is dependent
              on Yahoo and may change.
        """

        return self._load_data()

    def _get_data_in_date_range(self, dates, call=True, put=True):

        to_ret = Series({"call": call, "put": put})
        to_ret = to_ret[to_ret].index

        df = self._load_data(dates)
        types = [typ for typ in to_ret]

        df_filtered_by_type = df[df.index.map(lambda x: x[2] in types).tolist()]
        df_filtered_by_expiry = df_filtered_by_type[
            df_filtered_by_type.index.get_level_values("Expiry").isin(dates)
        ]
        return df_filtered_by_expiry

    @property
    def underlying_price(self):
        """
        Returns the underlying price.
        """
        try:
            underlying_price = self._underlying_price
        except AttributeError:
            data = self.get_options_data()
            underlying_price = data.Underlying_Price[0]
        return underlying_price

    @property
    def quote_time(self):
        """
        Returns the quote time.
        """
        try:
            quote_time = self._quote_time
        except AttributeError:
            data = self.get_options_data()
            quote_time = data.Quote_Time[0]
        return quote_time

    @property
    def expiry_dates(self):
        """
        Returns a list of available expiry dates
        """
        try:
            expiry_dates = self._expiry_dates
        except AttributeError:
            expiry_dates = self._get_expiry_dates()
        return expiry_dates

    def _get_expiry_dates(self):
        """
        Gets available expiry dates.

        Returns
        -------
        List of datetime.date objects
        """

        url = self._OPTIONS_BASE_URL.format(sym=self.symbol)
        jd = self._parse_url(url)

        expiry_dates = [
            dt.datetime.utcfromtimestamp(ts).date()
            for ts in jd["optionChain"]["result"][0]["expirationDates"]
        ]

        if len(expiry_dates) == 0:
            raise RemoteDataError("Data not available")  # pragma: no cover

        self._expiry_dates = expiry_dates
        return expiry_dates

    def _parse_url(self, url):
        """

        Downloads and parses a URL into a json object.
        Parameters
        ----------
        url : String
            The url to load and parse

        Returns
        -------
        A JSON object
        """
        jd = json.loads(self._read_url_as_StringIO(url).read())
        if jd is None:  # pragma: no cover
            raise RemoteDataError(
                "Parsed URL {0!r} is not " "a valid json object".format(url)
            )
        return jd

    def _process_data(self, jd):
        """
        Parse JSON data into the DataFrame

        Also adds computed columns for:
            IsNonstandard (ie: deliverable is not 100 shares)

        Parameters
        ----------
        jd : json object
            a json object containing the data to process

        Returns
        -------
        pandas.DataFrame
            A DataFrame with requested options data.
        """

        columns = [
            "Last",
            "Bid",
            "Ask",
            "Chg",
            "PctChg",
            "Vol",
            "Open_Int",
            "IV",
            "Root",
            "IsNonstandard",
            "Underlying",
            "Underlying_Price",
            "Quote_Time",
            "Last_Trade_Date",
            "JSON",
        ]
        indexes = ["Strike", "Expiry", "Type", "Symbol"]
        rows_list, index = self._process_rows(jd)
        if len(rows_list) > 0:
            df = DataFrame(
                rows_list,
                columns=columns,
                index=MultiIndex.from_tuples(index, names=indexes),
            )
        else:
            df = DataFrame(columns=columns)

        df["IsNonstandard"] = df["Root"] != self.symbol.replace("-", "")

        # Make dtype consistent, requires float64 as there can be NaNs
        df["Vol"] = df["Vol"].astype("float64")
        df["Open_Int"] = df["Open_Int"].astype("float64")

        return df.sort_index()

    def _process_rows(self, jd):
        rows_list = []
        index = []

        # handle no results
        if len(jd["optionChain"]["result"]) <= 0:
            return rows_list, index

        quote = jd["optionChain"]["result"][0]["quote"]
        for option in jd["optionChain"]["result"][0]["options"]:
            for typ in ["calls", "puts"]:
                options_by_type = option[typ]
                for option_by_strike in options_by_type:
                    d = {}
                    for dkey, rkey, ntype in [
                        ("Last", "lastPrice", float),
                        ("Bid", "bid", float),
                        ("Ask", "ask", float),
                        ("Chg", "change", float),
                        ("PctChg", "percentChange", float),
                        ("Vol", "volume", int),
                        ("Open_Int", "openInterest", int),
                        ("IV", "impliedVolatility", float),
                        ("Last_Trade_Date", "lastTradeDate", int),
                    ]:
                        try:
                            d[dkey] = ntype(option_by_strike[rkey])
                        except (KeyError, ValueError):
                            pass
                    d["JSON"] = option_by_strike
                    d["Root"] = option_by_strike["contractSymbol"][:-15]
                    d["Underlying"] = self.symbol

                    d["Underlying_Price"] = quote["regularMarketPrice"]
                    quote_unix_time = quote["regularMarketTime"]
                    if quote["marketState"] == "PRE" and "preMarketPrice" in quote:
                        d["Underlying_Price"] = quote["preMarketPrice"]
                        quote_unix_time = quote["preMarketTime"]
                    elif (
                        quote["marketState"] == "POSTPOST"
                        and "postMarketPrice" in quote
                    ):
                        d["Underlying_Price"] = quote["postMarketPrice"]
                        quote_unix_time = quote["postMarketTime"]
                    d["Quote_Time"] = dt.datetime.utcfromtimestamp(quote_unix_time)

                    self._underlying_price = d["Underlying_Price"]
                    self._quote_time = d["Quote_Time"]

                    d["Last_Trade_Date"] = dt.datetime.utcfromtimestamp(
                        d["Last_Trade_Date"]
                    )

                    rows_list.append(d)
                    index.append(
                        (
                            float(option_by_strike["strike"]),
                            dt.datetime.utcfromtimestamp(
                                option_by_strike["expiration"]
                            ),
                            typ.replace("s", ""),
                            option_by_strike["contractSymbol"],
                        )
                    )
        return rows_list, index

    def _load_data(self, exp_dates=None):
        """
        Loads options data.

        Parameters
        ----------
        exp_dates : tuple of datetimes, optional(default=None)
            The expiry dates to load options data for. If none is specified,
            uses all expiry dates available for the symbol.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with requested options data.
        """
        data = []
        epoch = dt.datetime.utcfromtimestamp(0)

        try:
            if exp_dates is None:
                exp_dates = self._get_expiry_dates()
            exp_unix_times = [
                int(
                    (
                        dt.datetime(exp_date.year, exp_date.month, exp_date.day) - epoch
                    ).total_seconds()
                )
                for exp_date in exp_dates
            ]
            for exp_date in exp_unix_times:
                url = (self._OPTIONS_BASE_URL + "?date={exp_date}").format(
                    sym=self.symbol, exp_date=exp_date
                )
                jd = self._parse_url(url)
                data.append(self._process_data(jd))
            return concat(data).sort_index()
        finally:
            self.close()
