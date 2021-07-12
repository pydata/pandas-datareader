import pandas as pd

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import string_types
from pandas_datareader.io import read_jsdmx


class OECDReader(_BaseReader):
    """Get data for the given name from OECD."""

    _format = "json"

    @property
    def url(self):
        """API URL"""
        url = "http://stats.oecd.org/SDMX-JSON/data"

        if not isinstance(self.symbols, string_types):
            raise ValueError("data name must be string")

        # API: https://data.oecd.org/api/sdmx-json-documentation/
        return "{0}/{1}/all/all?".format(url, self.symbols)

    def _read_lines(self, out):
        """read one data from specified URL"""
        df = read_jsdmx(out)
        try:
            idx_name = df.index.name  # hack for pandas 0.16.2
            df.index = pd.to_datetime(df.index, errors="ignore")
            for col in df:
                df[col] = pd.to_numeric(df[col], errors="ignore")
            df = df.sort_index()
            df = df.truncate(self.start, self.end)
            df.index.name = idx_name
        except ValueError:
            pass
        return df
