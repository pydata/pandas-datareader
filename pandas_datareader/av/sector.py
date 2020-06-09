import pandas as pd

from pandas_datareader._utils import RemoteDataError
from pandas_datareader.av import AlphaVantage


class AVSectorPerformanceReader(AlphaVantage):
    """
    Returns DataFrame of the Alpha Vantage Sector Performances SECTOR data.

    .. versionadded:: 0.7.0

    Parameters
    ----------
    symbols : string, array-like object (list, tuple, Series)
        Single currency pair (formatted 'FROM/TO') or list of the same.
    retry_count : int, default 3
        Number of times to retry query request.
    pause : int, default 0.1
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    api_key : str, optional
        Alpha Vantage API key . If not provided the environmental variable
        ALPHAVANTAGE_API_KEY is read. The API key is *required*.
    """

    @property
    def function(self):
        return "SECTOR"

    def _read_lines(self, out):
        if "Information" in out:
            raise RemoteDataError()
        else:
            out.pop("Meta Data")
        df = pd.DataFrame(out)
        columns = ["RT", "1D", "5D", "1M", "3M", "YTD", "1Y", "3Y", "5Y", "10Y"]
        df.columns = columns
        return df
