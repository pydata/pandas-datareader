import datetime as dt
import tempfile

from pandas import read_csv, to_datetime

from pandas_datareader.base import _BaseReader

_URL = "https://faculty.iima.ac.in/iffm/Indian-Fama-French-Momentum/"
_URL_PREFIX = "DATA/"
_URL_SUFFIX = "_SurvivorshipBiasAdjusted.csv"


def _parse_date_famafrench(x):
    x = x.strip()
    try:
        return dt.datetime.strptime(x, "%Y%m")
    except Exception:
        pass
    return to_datetime(x)


class FamaFrenchIndiaReader(_BaseReader):
    """
    Get data for the given name from the Fama/French India data libary.

    Credits to the IIM Ahemdabad
    https://faculty.iima.ac.in/iffm/Indian-Fama-French-Momentum/
    """

    @property
    def url(self):
        """API URL"""
        return f"{_URL}{_URL_PREFIX}{self.symbols}{_URL_SUFFIX}"

    def read(self):
        """
        Read data

        Returns
        -------
        df : DataFrame
            A data frame that is indexed by the date
        """
        return super().read()

    def _read_one_data(self, url, params):
        params = {
            "index_col": 0,
            "parse_dates": True,
        }

        resp = self._get_response(url)

        with tempfile.TemporaryFile() as tmpf:
            tmpf.write(resp.content)
            # seek to beginning so pandas can start reading the file
            tmpf.seek(0)

            df = read_csv(tmpf, **params)

            # this dataset defines (Market - Risk-Free) rate as `MF` instead
            # of `Mkt-RF`, so renaming it for better convenience.
            df.rename(columns={"MF": "Mkt-RF"}, inplace=True)

            if self.start is not None:
                df = df[self.start :]
            elif self.start is not None and self.end is not None:
                df = df[self.start : self.end]
            elif self.end is not None:
                df = df[: self.end]

            return df
        return None
