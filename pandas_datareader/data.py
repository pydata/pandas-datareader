"""
Module contains tools for collecting data from various remote sources
"""

# flake8: noqa

import warnings

from pandas_datareader._decorators import deprecate_kwarg
from pandas_datareader.bankofcanada import BankOfCanadaReader
from pandas_datareader.econdb import EcondbReader
from pandas_datareader.eurostat import EurostatReader
from pandas_datareader.exceptions import DEP_ERROR_MSG, ImmediateDeprecationError
from pandas_datareader.famafrench import FamaFrenchReader
from pandas_datareader.fred import FredReader
from pandas_datareader.oecd import OECDReader

__all__ = [
    "get_data_econdb",
    "get_data_famafrench",
    "get_data_fred",
    "DataReader",
]


def get_data_fred(*args, **kwargs):
    return FredReader(*args, **kwargs).read()


def get_data_famafrench(*args, **kwargs):
    return FamaFrenchReader(*args, **kwargs).read()


def get_data_econdb(*args, **kwargs):
    return EcondbReader(*args, **kwargs).read()


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

    Currently supports macroeconomic, central-bank, and factor-oriented
    data sources.

    Parameters
    ----------
    name : str or list of strs
        the name of the dataset. Some data sources (fred) will
        accept a list of names.
    data_source: {str, None}
        the data source ("fred", "famafrench", "oecd", "eurostat", "econdb")
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
    # Data from FRED
    vix = DataReader("VIXCLS", "fred")

    # Data from Fama/French
    ff = DataReader("F-F_Research_Data_Factors", "famafrench")
    ff = DataReader("F-F_Research_Data_Factors_weekly", "famafrench")
    ff = DataReader("6_Portfolios_2x3", "famafrench")
    ff = DataReader("F-F_ST_Reversal_Factor", "famafrench")
    """
    expected_source = [
        "bankofcanada",
        "fred",
        "famafrench",
        "oecd",
        "eurostat",
        "econdb",
    ]

    if data_source not in expected_source:
        msg = "data_source=%r is not implemented" % data_source
        raise NotImplementedError(msg)

    if data_source == "bankofcanada":
        return BankOfCanadaReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "fred":
        return FredReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "famafrench":
        return FamaFrenchReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()

    if data_source == "oecd":
        return OECDReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()
    if data_source == "eurostat":
        return EurostatReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        ).read()
    if data_source == "econdb":
        return EcondbReader(
            symbols=name,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
            api_key=api_key,
        ).read()

    msg = "data_source=%r is not implemented" % data_source
    raise NotImplementedError(msg)


def Options(symbol, data_source=None, session=None):
    if data_source is None:
        warnings.warn(
            "Options(symbol) is deprecated and no longer part of the public macro/factor API.",
            FutureWarning,
            stacklevel=2,
        )
    raise ImmediateDeprecationError(DEP_ERROR_MSG.format("Options"))
