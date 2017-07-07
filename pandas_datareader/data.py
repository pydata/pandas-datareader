"""
Module contains tools for collecting data from various remote sources
"""

import warnings

from pandas_datareader.google.daily import GoogleDailyReader
from pandas_datareader.google.quotes import GoogleQuotesReader

from pandas_datareader.yahoo.daily import YahooDailyReader
from pandas_datareader.yahoo.quotes import YahooQuotesReader
from pandas_datareader.yahoo.actions import (YahooActionReader, YahooDivReader)
from pandas_datareader.yahoo.components import _get_data as get_components_yahoo  # noqa
from pandas_datareader.yahoo.options import Options as YahooOptions
from pandas_datareader.google.options import Options as GoogleOptions

from pandas_datareader.iex.deep import Deep as IEXDeep
from pandas_datareader.iex.tops import LastReader as IEXLasts
from pandas_datareader.iex.tops import TopsReader as IEXTops

from pandas_datareader.eurostat import EurostatReader
from pandas_datareader.fred import FredReader
from pandas_datareader.famafrench import FamaFrenchReader
from pandas_datareader.oecd import OECDReader
from pandas_datareader.edgar import EdgarIndexReader
from pandas_datareader.enigma import EnigmaReader
from pandas_datareader.oanda import get_oanda_currency_historical_rates
from pandas_datareader.nasdaq_trader import get_nasdaq_symbols


def get_data_fred(*args, **kwargs):
    return FredReader(*args, **kwargs).read()


def get_data_famafrench(*args, **kwargs):
    return FamaFrenchReader(*args, **kwargs).read()


def get_data_google(*args, **kwargs):
    return GoogleDailyReader(*args, **kwargs).read()


def get_data_yahoo(*args, **kwargs):
    return YahooDailyReader(*args, **kwargs).read()


def get_data_enigma(*args, **kwargs):
    return EnigmaReader(*args, **kwargs).read()


def get_data_yahoo_actions(*args, **kwargs):
    return YahooActionReader(*args, **kwargs).read()


def get_quote_yahoo(*args, **kwargs):
    return YahooQuotesReader(*args, **kwargs).read()


def get_quote_google(*args, **kwargs):
    return GoogleQuotesReader(*args, **kwargs).read()


def get_tops_iex(*args, **kwargs):
    return IEXTops(*args, **kwargs).read()


def get_last_iex(*args, **kwargs):
    return IEXLasts(*args, **kwargs).read()


def get_markets_iex(*args, **kwargs):
    """
    Returns near-real time volume data across markets segregated by tape
    and including a percentage of overall volume during the session

    This endpoint does not accept any parameters.

    Reference: https://www.iextrading.com/developer/docs/#markets

    :return: DataFrame
    """
    from pandas_datareader.iex.market import MarketReader
    return MarketReader(*args, **kwargs).read()


def get_dailysummary_iex(*args, **kwargs):
    """
    Returns a summary of daily market volume statistics. Without parameters,
    this will return the most recent trading session by default.

    :param start:
        A datetime object - the beginning of the date range.
    :param end:
        A datetime object - the end of the date range.

    Reference: https://www.iextrading.com/developer/docs/#historical-daily

    :return: DataFrame
    """
    from pandas_datareader.iex.stats import DailySummaryReader
    return DailySummaryReader(*args, **kwargs).read()


def get_summary_iex(*args, **kwargs):
    """
    Returns an aggregated monthly summary of market volume and a variety of
    related metrics for trades by lot size, security market cap, and venue.
    In the absence of parameters, this will return month-to-date statistics.
    For ranges spanning multiple months, this will return one row per month.

    :param start:
        A datetime object - the beginning of the date range.
    :param end:
        A datetime object - the end of the date range.

    :return: DataFrame
    """
    from pandas_datareader.iex.stats import MonthlySummaryReader
    return MonthlySummaryReader(*args, **kwargs).read()


def get_records_iex(*args, **kwargs):
    """
    Returns the record value, record date, recent value, and 30-day average for
    market volume, # of symbols traded, # of routed trades and notional value.
    This function accepts no additional parameters.

    Reference: https://www.iextrading.com/developer/docs/#records

    :return: DataFrame
    """
    from pandas_datareader.iex.stats import RecordsReader
    return RecordsReader(*args, **kwargs).read()


def get_recent_iex(*args, **kwargs):
    """
    Returns market volume and trade routing statistics for recent sessions.
    Also reports IEX's relative market share, lit share volume and a boolean
    halfday indicator.

    Reference: https://www.iextrading.com/developer/docs/#recent

    :return: DataFrame
    """
    from pandas_datareader.iex.stats import RecentReader
    return RecentReader(*args, **kwargs).read()


def get_iex_symbols(*args, **kwargs):
    """
    Returns a list of all equity symbols available for trading on IEX. Accepts
    no additional parameters.

    Reference: https://www.iextrading.com/developer/docs/#symbols

    :return: DataFrame
    """
    from pandas_datareader.iex.ref import SymbolsReader
    return SymbolsReader(*args, **kwargs).read()


def get_iex_book(*args, **kwargs):
    """
    Returns an array of dictionaries with depth of book data from IEX for up to
    10 securities at a time. Returns a dictionary of the bid and ask books.

    :param symbols:
        A string or list of strings of valid tickers
    :param service:
        'book': Live depth of book data
        'op-halt-status': Checks to see if the exchange has instituted a halt
        'security-event': Denotes individual security related event
        'ssr-status': Short Sale Price Test restrictions, per reg 201 of SHO
        'system-event': Relays current feed status (i.e. market open)
        'trades': Retrieves recent executions, trade size/price and flags
        'trade-breaks': Lists execution breaks for the current trading session
        'trading-status': Returns status and cause codes for securities

    :return: Object
    """
    from pandas_datareader.iex.deep import Deep
    return Deep(*args, **kwargs).read()


def DataReader(name, data_source=None, start=None, end=None,
               retry_count=3, pause=0.001, session=None, access_key=None):
    """
    Imports data from a number of online sources.

    Currently supports Yahoo! Finance, Google Finance, St. Louis FED (FRED),
    Kenneth French's data library, and the SEC's EDGAR Index.

    Parameters
    ----------
    name : str or list of strs
        the name of the dataset. Some data sources (yahoo, google, fred) will
        accept a list of names.
    data_source: {str, None}
        the data source ("yahoo", "yahoo-actions", "yahoo-dividends",
        "google", "fred", "ff", or "edgar-index")
    start : {datetime, None}
        left boundary for range (defaults to 1/1/2010)
    end : {datetime, None}
        right boundary for range (defaults to today)
    retry_count : {int, 3}
        Number of times to retry query request.
    pause : {numeric, 0.001}
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    session : Session, default None
            requests.sessions.Session instance to be used
    access_key : (str, None)
        Optional parameter to specify an API key for certain data sources.

    Examples
    ----------

    # Data from Yahoo! Finance
    gs = DataReader("GS", "yahoo")

    # Corporate Actions (Dividend and Split Data)
    # with ex-dates from Yahoo! Finance
    gs = DataReader("GS", "yahoo-actions")

    # Data from Google Finance
    aapl = DataReader("AAPL", "google")

    # Price and volume data from IEX
    tops = DataReader(["GS", "AAPL"], "iex-tops")
    # Top of book executions from IEX
    gs = DataReader("GS", "iex-last")
    # Real-time depth of book data from IEX
    gs = DataReader("GS", "iex-book")

    # Data from FRED
    vix = DataReader("VIXCLS", "fred")

    # Data from Fama/French
    ff = DataReader("F-F_Research_Data_Factors", "famafrench")
    ff = DataReader("F-F_Research_Data_Factors_weekly", "famafrench")
    ff = DataReader("6_Portfolios_2x3", "famafrench")
    ff = DataReader("F-F_ST_Reversal_Factor", "famafrench")

    # Data from EDGAR index
    ed = DataReader("full", "edgar-index")
    ed2 = DataReader("daily", "edgar-index")
    """
    if data_source == "yahoo":
        return YahooDailyReader(symbols=name, start=start, end=end,
                                adjust_price=False, chunksize=25,
                                retry_count=retry_count, pause=pause,
                                session=session).read()

    elif data_source == "yahoo-actions":
        return YahooActionReader(symbols=name, start=start, end=end,
                                 retry_count=retry_count, pause=pause,
                                 session=session).read()

    elif data_source == "yahoo-dividends":
        return YahooDivReader(symbols=name, start=start, end=end,
                              adjust_price=False, chunksize=25,
                              retry_count=retry_count, pause=pause,
                              session=session, interval='d').read()

    elif data_source == "google":
        return GoogleDailyReader(symbols=name, start=start, end=end,
                                 chunksize=25,
                                 retry_count=retry_count, pause=pause,
                                 session=session).read()
    elif data_source == "iex-tops":
        return IEXTops(symbols=name, start=start, end=end,
                       retry_count=retry_count, pause=pause,
                       session=session).read()

    elif data_source == "iex-last":
        return IEXLasts(symbols=name, start=start, end=end,
                        retry_count=retry_count, pause=pause,
                        session=session).read()

    elif data_source == "iex-book":
        return IEXDeep(symbols=name, service="book", start=start, end=end,
                       retry_count=retry_count, pause=pause,
                       session=session).read()

    elif data_source == "enigma":
        return EnigmaReader(datapath=name, api_key=access_key).read()

    elif data_source == "fred":
        return FredReader(symbols=name, start=start, end=end,
                          retry_count=retry_count, pause=pause,
                          session=session).read()

    elif data_source == "famafrench":
        return FamaFrenchReader(symbols=name, start=start, end=end,
                                retry_count=retry_count, pause=pause,
                                session=session).read()

    elif data_source == "oecd":
        return OECDReader(symbols=name, start=start, end=end,
                          retry_count=retry_count, pause=pause,
                          session=session).read()

    elif data_source == "eurostat":
        return EurostatReader(symbols=name, start=start, end=end,
                              retry_count=retry_count, pause=pause,
                              session=session).read()

    elif data_source == "edgar-index":
        return EdgarIndexReader(symbols=name, start=start, end=end,
                                retry_count=retry_count, pause=pause,
                                session=session).read()

    elif data_source == "oanda":
        return get_oanda_currency_historical_rates(
            start, end,
            quote_currency="USD", base_currency=name,
            reversed=True, session=session
        )

    elif data_source == 'nasdaq':
        if name != 'symbols':
            raise ValueError("Only the string 'symbols' is supported for "
                             "Nasdaq, not %r" % (name,))
        return get_nasdaq_symbols(retry_count=retry_count, pause=pause)

    else:
        msg = "data_source=%r is not implemented" % data_source
        raise NotImplementedError(msg)


def Options(symbol, data_source=None, session=None):
    if data_source is None:
        warnings.warn("Options(symbol) is deprecated, use Options(symbol,"
                      " data_source) instead", FutureWarning, stacklevel=2)
        data_source = "yahoo"
    if data_source == "yahoo":
        return YahooOptions(symbol, session=session)
    elif data_source == "google":
        return GoogleOptions(symbol, session=session)
    else:
        raise NotImplementedError("currently only yahoo and google supported")
