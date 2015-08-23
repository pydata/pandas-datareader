"""
Module contains tools for collecting data from various remote sources


"""

from pandas_datareader.commons.date_chunks import _sanitize_dates
from pandas_datareader.google import get_data_google
from pandas_datareader.google.quotes import get_quote_google

from pandas_datareader.yahoo import get_data_yahoo
from pandas_datareader.yahoo.quotes import get_quote_yahoo
from pandas_datareader.yahoo.options import Options
from pandas_datareader.yahoo.actions import get_data_yahoo_actions
from pandas_datareader.yahoo.components import get_components_yahoo

from pandas_datareader.fred import get_data_fred
from pandas_datareader.famafrench import get_data_famafrench

def DataReader(name, data_source=None, start=None, end=None,
               retry_count=3, pause=0.001):
    """
    Imports data from a number of online sources.

    Currently supports Yahoo! Finance, Google Finance, St. Louis FED (FRED)
    and Kenneth French's data library.

    Parameters
    ----------
    name : str or list of strs
        the name of the dataset. Some data sources (yahoo, google, fred) will
        accept a list of names.
    data_source: str
        the data source ("yahoo", "yahoo-actions", "google", "fred", or "ff")
    start : {datetime, None}
        left boundary for range (defaults to 1/1/2010)
    end : {datetime, None}
        right boundary for range (defaults to today)

    Examples
    ----------

    # Data from Yahoo! Finance
    gs = DataReader("GS", "yahoo")

    # Corporate Actions (Dividend and Split Data) with ex-dates from Yahoo! Finance
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
    """
    start, end = _sanitize_dates(start, end)

    if data_source == "yahoo":
        return get_data_yahoo(symbols=name, start=start, end=end,
                              adjust_price=False, chunksize=25,
                              retry_count=retry_count, pause=pause)
    elif data_source == "yahoo-actions":
        return get_data_yahoo_actions(symbol=name, start=start, end=end,
                                      retry_count=retry_count, pause=pause)
    elif data_source == "google":
        return get_data_google(symbols=name, start=start, end=end,
                               chunksize=25, retry_count=retry_count, pause=pause)
    elif data_source == "fred":
        return get_data_fred(name, start, end)
    elif data_source == "famafrench":
        return get_data_famafrench(name)
    else:
        raise NotImplementedError(
                "data_source=%r is not implemented" % data_source)
