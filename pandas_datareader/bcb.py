import pandas as pd

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import is_list_like
from pandas_datareader._utils import RemoteDataError


class BCBReader(_BaseReader):
    """
    Returns DataFrame of historical data from the Brazilian Central Bank
    open data service, over date range, start to end.


    Parameters
    ----------
    symbols : {int, str, List[str], List[int]}
        The symbols are integer codes related to available time series.
        This argument can be obtained in the SGS system site.
        In this site searches can be executed in order to find out the desired
        series and use the series code in the symbols argument.
    start : string, int, date, datetime, Timestamp
        Starting date. Parses many different kind of date
        representations (e.g., 'JAN-01-2010', '1/1/10', 'Jan, 1, 1980')
    end : string, int, date, datetime, Timestamp
        Ending date
    retry_count : int, default 3
        Number of times to retry query request.
    pause : float, default 0.1
        Time, in seconds, of the pause between retries.
    session : Session, default None
        requests.sessions.Session instance to be used.
    freq : {str, None}
        Ignored
    """

    @property
    def url(self):
        """API URL"""
        return "http://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados"

    @property
    def params(self):
        """Parameters to use in API calls"""
        params = {
            "formato": "json",
            "dataInicial": self.start.strftime("%d/%m/%Y"),
            "dataFinal": self.end.strftime("%d/%m/%Y"),
        }
        return params

    def read(self):
        """Read data from connector

        Returns
        -------
        data : DataFrame
            If multiple names are passed for "series" then the index of the
            DataFrame is the outer join of the indicies of each series.
        """
        try:
            return self._read()
        finally:
            self.close()

    def _read(self):
        """read data from many URLs if necessary and joins into one DataFrame"""
        if not is_list_like(self.symbols):
            names = [self.symbols]
        else:
            names = self.symbols

        urls = [self.url.format(n) for n in names]

        def _req(url, n):
            return self._read_single_request(n, url, self.params)

        dfs = [_req(url, n) for url, n in zip(urls, names)]
        df = pd.concat(dfs, axis=1, join="outer")
        return df

    def _read_single_request(self, symbol, url, params):
        """read one data from specified URL"""
        out = self._read_url_as_StringIO(url, params=params)
        try:
            df = pd.read_json(out)
        except ValueError:
            out.seek(0)
            msg = out.read()
            raise RemoteDataError(
                "message: {}, symbol: {}".format(msg, symbol)
            ) from None

        cns = {"data": "date", "valor": str(symbol), "datafim": "end_date"}
        df = df.rename(columns=cns)

        if "date" in df:
            df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y")
        if "end_date" in df:
            df["end_date"] = pd.to_datetime(df["end_date"], format="%d/%m/%Y")

        return df.set_index("date")
