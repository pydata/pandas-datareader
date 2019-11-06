"""
Module contains tools for collecting data from various remote sources
"""

# flake8: noqa

import warnings

from pandas.util._decorators import deprecate_kwarg

from pandas_datareader.av.forex import AVForexReader
from pandas_datareader.av.quotes import AVQuotesReader
from pandas_datareader.av.sector import AVSectorPerformanceReader
from pandas_datareader.av.time_series import AVTimeSeriesReader
from pandas_datareader.bankofcanada import BankOfCanadaReader
from pandas_datareader.econdb import EcondbReader
from pandas_datareader.enigma import EnigmaReader
from pandas_datareader.eurostat import EurostatReader
from pandas_datareader.exceptions import DEP_ERROR_MSG, ImmediateDeprecationError
from pandas_datareader.famafrench import FamaFrenchReader
from pandas_datareader.fred import FredReader
from pandas_datareader.iex.daily import IEXDailyReader
from pandas_datareader.iex.deep import Deep as IEXDeep
from pandas_datareader.iex.tops import LastReader as IEXLasts, TopsReader as IEXTops
from pandas_datareader.moex import MoexReader
from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
from pandas_datareader.oecd import OECDReader
from pandas_datareader.quandl import QuandlReader
from pandas_datareader.robinhood import RobinhoodHistoricalReader, RobinhoodQuoteReader
from pandas_datareader.stooq import StooqDailyReader
from pandas_datareader.tiingo import (
    TiingoDailyReader,
    TiingoIEXHistoricalReader,
    TiingoQuoteReader,
)
from pandas_datareader.yahoo.actions import YahooActionReader, YahooDivReader
from pandas_datareader.yahoo.components import _get_data as get_components_yahoo
from pandas_datareader.yahoo.daily import YahooDailyReader
from pandas_datareader.yahoo.options import Options as YahooOptions
from pandas_datareader.yahoo.quotes import YahooQuotesReader

__all__ = [
    "get_components_yahoo",
    "get_data_enigma",
    "get_data_famafrench",
    "get_data_fred",
    "get_data_moex",
    "get_data_quandl",
    "get_data_yahoo",
    "get_data_yahoo_actions",
    "get_nasdaq_symbols",
    "get_quote_yahoo",
    "get_tops_iex",
    "get_summary_iex",
    "get_records_iex",
    "get_recent_iex",
    "get_markets_iex",
    "get_last_iex",
    "get_iex_symbols",
    "get_iex_book",
    "get_dailysummary_iex",
    "get_data_stooq",
    "get_data_robinhood",
    "get_quotes_robinhood",
    "DataReader",
]


def get_data_alphavantage(*args, **kwargs):
    return AVTimeSeriesReader(*args, **kwargs).read()


def get_data_fred(*args, **kwargs):
    return FredReader(*args, **kwargs).read()


def get_data_famafrench(*args, **kwargs):
    return FamaFrenchReader(*args, **kwargs).read()


def get_data_yahoo(*args, **kwargs):
    return YahooDailyReader(*args, **kwargs).read()


def get_data_enigma(*args, **kwargs):
    return EnigmaReader(*args, **kwargs).read()


def get_quote_av(*args, **kwargs):
    return AVQuotesReader(*args, **kwargs).read()


def get_data_yahoo_actions(*args, **kwargs):
    return YahooActionReader(*args, **kwargs).read()


def get_quote_yahoo(*args, **kwargs):
    return YahooQuotesReader(*args, **kwargs).read()


def get_data_quandl(*args, **kwargs):
    return QuandlReader(*args, **kwargs).read()


def get_data_moex(*args, **kwargs):
    return MoexReader(*args, **kwargs).read()


def get_data_stooq(*args, **kwargs):
    return StooqDailyReader(*args, **kwargs).read()


def get_tops_iex(*args, **kwargs):
    return IEXTops(*args, **kwargs).read()


def get_last_iex(*args, **kwargs):
    return IEXLasts(*args, **kwargs).read()


def get_data_robinhood(*args, **kwargs):
    return RobinhoodHistoricalReader(*args, **kwargs).read()


def get_quotes_robinhood(*args, **kwargs):
    return RobinhoodQuoteReader(*args, **kwargs).read()


def get_data_tiingo(*args, **kwargs):
    return TiingoDailyReader(*args, **kwargs).read()


def get_iex_data_tiingo(*args, **kwargs):
    return TiingoIEXHistoricalReader(*args, **kwargs).read()


def get_quotes_tiingo(*args, **kwargs):
    return TiingoQuoteReader(*args, **kwargs).read()


def get_exchange_rate_av(*args, **kwargs):
    return AVForexReader(*args, **kwargs).read()


def get_sector_performance_av(*args, **kwargs):
    return AVSectorPerformanceReader(*args, **kwargs).read()


def get_markets_iex(*args, **kwargs):
    """
    Returns near-real time volume data across markets segregated by tape
    and including a percentage of overall volume during the session

    This endpoint does not accept any parameters.

    Reference: https://www.iextrading.com/developer/docs/#markets

    Returns
    -------
    DataFrame
    """
    from pandas_datareader.iex.market import MarketReader

    return MarketReader(*args, **kwargs).read()


def get_dailysummary_iex(*args, **kwargs):
    """
    Returns a summary of daily market volume statistics. Without parameters,
    this will return the most recent trading session by default.

    Parameters
    ----------
    start : string, int, date, datetime, Timestamp
        The beginning of the date range.
    end : string, int, date, datetime, Timestamp
        The end of the date range.

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

    start : string, int, date, datetime, Timestamp
        A datetime object - the beginning of the date range.
    end : string, int, date, datetime, Timestamp
        A datetime object - the end of the date range.

    Returns
    -------
    DataFrame
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

    Returns
    -------
    DataFrame
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

    Parameters
    ----------
    symbols : str, List[str]
        A string or list of strings of valid tickers
    service : str
        One of:

        - 'book': Live depth of book data
        - 'op-halt-status': Checks to see if the exchange has instituted a halt
        - 'security-event': Denotes individual security related event
        - 'ssr-status': Short Sale Price Test restrictions, per reg 201 of SHO
        - 'system-event': Relays current feed status (i.e. market open)
        - 'trades': Retrieves recent executions, trade size/price and flags
        - 'trade-breaks': Lists execution breaks for the current trading session
        - 'trading-status': Returns status and cause codes for securities

    Returns
    -------
    DataFrame
    """
    return IEXDeep(*args, **kwargs).read()


@deprecate_kwarg("access_key", "api_key")
def DataReader(
    name,
    data_source=None,
    start=None,
    end=None,
    retry_count=3,
    pause=0.1,
    session=None,
    api_key=None,
):
    """
    Imports data from a number of online sources.

    Currently supports Google Finance, St. Louis FED (FRED),
    and Kenneth French's data library, among others.

    Parameters
    ----------
    name : str or list of strs
        the name of the dataset. Some data sources (IEX, fred) will
        accept a list of names.
    data_source: {str, None}
        the data source ("iex", "fred", "ff")
    start : string, int, date, datetime, Timestamp
        left boundary for range (defaults to 1/1/2010)
    end : string, int, date, datetime, Timestamp
        right boundary for range (defaults to today)
    retry_count : {int, 3}
        Number of times to retry query request.
    pause : {numeric, 0.001}
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    api_key : (str, None)
        Optional parameter to specify an API key for certain data sources.

    Examples
    ----------
    # Data from Google Finance
    aapl = DataReader("AAPL", "iex")

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
    """
    expected_source = [
        "yahoo",
        "iex",
        "iex-tops",
        "iex-last",
        "iex-last",
        "bankofcanada",
        "stooq",
        "iex-book",
        "enigma",
        "fred",
        "famafrench",
        "oecd",
        "eurostat",
        "nasdaq",
        "quandl",
        "moex",
        "robinhood",
        "tiingo",
        "yahoo-actions",
        "yahoo-dividends",
        "av-forex",
        "av-daily",
        "av-daily-adjusted",
        "av-weekly",
        "av-weekly-adjusted",
        "av-monthly",
        "av-monthly-adjusted",
        "av-intraday",
        "econdb",
    ]

    if data_source not in expected_source:
        msg = "data_source=%r is not implemented" % data_source
        raise NotImplementedError(msg)

    if data_source == "yahoo":
        return YahooDailyReader(
            symbols=name,
            start=start,
            end=end,
            adjust_price=False,
            chunksize=25,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    elif data_source == "iex":
        return IEXDailyReader(
            symbols=name,
            start=start,
            end=end,
            chunksize=25,
            api_key=api_key,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    elif data_source == "iex-tops":
        return IEXTops(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    elif data_source == "iex-last":
        return IEXLasts(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    elif data_source == "bankofcanada":
        return BankOfCanadaReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()
    elif data_source == "stooq":
        return StooqDailyReader(
            symbols=name,
            chunksize=25,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    elif data_source == "iex-book":
        return IEXDeep(
            symbols=name,
            service="book",
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    elif data_source == "enigma":
        return EnigmaReader(dataset_id=name, api_key=api_key).read()

    elif data_source == "fred":
        return FredReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    elif data_source == "famafrench":
        return FamaFrenchReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    elif data_source == "oecd":
        return OECDReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()
    elif data_source == "eurostat":
        return EurostatReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()
    elif data_source == "nasdaq":
        if name != "symbols":
            raise ValueError(
                "Only the string 'symbols' is supported for " "Nasdaq, not %r" % (name,)
            )
        return get_nasdaq_symbols(retry_count=retry_count, pause=pause)

    elif data_source == "quandl":
        return QuandlReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()
    elif data_source == "moex":
        return MoexReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()
    elif data_source == "robinhood":
        return RobinhoodHistoricalReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()
    elif data_source == "tiingo":
        return TiingoDailyReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    elif data_source == "yahoo-actions":
        return YahooActionReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    elif data_source == "yahoo-dividends":
        return YahooDivReader(
            symbols=name,
            start=start,
            end=end,
            adjust_price=False,
            chunksize=25,
            retry_count=retry_count,
            pause=pause,
            session=session,
            interval="d",
        ).read()

    elif data_source == "av-forex":
        return AVForexReader(
            symbols=name,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    elif data_source == "av-daily":
        return AVTimeSeriesReader(
            symbols=name,
            function="TIME_SERIES_DAILY",
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    elif data_source == "av-daily-adjusted":
        return AVTimeSeriesReader(
            symbols=name,
            function="TIME_SERIES_DAILY_ADJUSTED",
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    elif data_source == "av-weekly":
        return AVTimeSeriesReader(
            symbols=name,
            function="TIME_SERIES_WEEKLY",
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    elif data_source == "av-weekly-adjusted":
        return AVTimeSeriesReader(
            symbols=name,
            function="TIME_SERIES_WEEKLY_ADJUSTED",
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    elif data_source == "av-monthly":
        return AVTimeSeriesReader(
            symbols=name,
            function="TIME_SERIES_MONTHLY",
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    elif data_source == "av-monthly-adjusted":
        return AVTimeSeriesReader(
            symbols=name,
            function="TIME_SERIES_MONTHLY_ADJUSTED",
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    elif data_source == "av-intraday":
        return AVTimeSeriesReader(
            symbols=name,
            function="TIME_SERIES_INTRADAY",
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    elif data_source == "econdb":
        return EcondbReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    else:
        msg = "data_source=%r is not implemented" % data_source
        raise NotImplementedError(msg)


def Options(symbol, data_source=None, session=None):
    if data_source is None:
        warnings.warn(
            "Options(symbol) is deprecated, use Options(symbol,"
            " data_source) instead",
            FutureWarning,
            stacklevel=2,
        )
        data_source = "yahoo"
    if data_source == "yahoo":
        raise ImmediateDeprecationError(DEP_ERROR_MSG.format("Yahoo Options"))
        return YahooOptions(symbol, session=session)
    else:
        raise NotImplementedError("currently only yahoo supported")
