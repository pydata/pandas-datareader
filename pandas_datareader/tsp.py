from pandas_datareader.base import _BaseReader


class TSPReader(_BaseReader):

    """
    Returns DataFrame of historical TSP fund prices from symbols, over date
    range, start to end.

    Parameters
    ----------
    symbols : str, array-like object (list, tuple, Series), or DataFrame
        Single stock symbol (ticker), array-like object of symbols or
        DataFrame with index containing stock symbols.
    start : string, int, date, datetime, Timestamp
        Starting date. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980'). Defaults to
        20 years before current date.
    end : string, int, date, datetime, Timestamp
        Ending date
    retry_count : int, default 3
        Number of times to retry query request.
    pause : int, default 0.1
        Time, in seconds, to pause between consecutive queries of chunks. If
        single value given for symbol, represents the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used
    """

    def __init__(
        self,
        symbols=("Linc", "L2020", "L2030", "L2040", "L2050", "G", "F", "C", "S", "I"),
        start=None,
        end=None,
        retry_count=3,
        pause=0.1,
        session=None,
    ):
        super(TSPReader, self).__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )
        self._format = "string"

    @property
    def url(self):
        """API URL"""
        return "https://www.tsp.gov/InvestmentFunds/FundPerformance/index.html"

    def read(self):
        """ read one data from specified URL """
        df = super(TSPReader, self).read()
        df.columns = map(lambda x: x.strip(), df.columns)
        return df

    @property
    def params(self):
        """Parameters to use in API calls"""
        return {
            "startdate": self.start.strftime("%m/%d/%Y"),
            "enddate": self.end.strftime("%m/%d/%Y"),
            "fundgroup": self.symbols,
            "whichButton": "CSV",
        }

    @staticmethod
    def _sanitize_response(response):
        """
        Clean up the response string
        """
        text = response.text.strip()
        if text[-1] == ",":
            return text[0:-1]
        return text
