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

from pandas_datareader.eurostat import EurostatReader
from pandas_datareader.fred import FredReader
from pandas_datareader.famafrench import FamaFrenchReader
from pandas_datareader.oecd import OECDReader
from pandas_datareader.edgar import EdgarIndexReader
from pandas_datareader.enigma import EnigmaReader
from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
from pandas_datareader.quandl import QuandlReader


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


def get_data_quandl(*args, **kwargs):
    return QuandlReader(*args, **kwargs).read()


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

    Examples
    ----------

    # Data from Yahoo! Finance
    gs = DataReader("GS", "yahoo")

    # Corporate Actions (Dividend and Split Data)
    # with ex-dates from Yahoo! Finance
    gs = DataReader("GS", "yahoo-actions")

    # Data from Google Finance
    aapl = DataReader("AAPL", "google")

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
    elif data_source == 'nasdaq':
        if name != 'symbols':
            raise ValueError("Only the string 'symbols' is supported for "
                             "Nasdaq, not %r" % (name,))
        return get_nasdaq_symbols(retry_count=retry_count, pause=pause)
    elif data_source == "quandl":
        return QuandlReader(symbols=name, start=start, end=end,
                            retry_count=retry_count, pause=pause,
                            session=session).read()
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
