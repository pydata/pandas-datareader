import datetime as dt

import pandas as pd
from pandas.core.common import is_list_like
import pandas.compat as compat
from pandas import concat, read_csv

from pandas_datareader.io import read_jsdmx
from pandas_datareader.base import _BaseReader


class OECDReader(_BaseReader):

    """Get data for the given name from OECD."""

    @property
    def url(self):
        url = 'http://stats.oecd.org/SDMX-JSON/data'

        if not isinstance(self.symbols, compat.string_types):
            raise ValueError('data name must be string')

        # API: https://data.oecd.org/api/sdmx-json-documentation/
        return '{0}/{1}/all/all?'.format(url, self.symbols)

    def _read_one_data(self, url, params):
        """ read one data from specified URL """
        resp = self._get_response(url)
        df = read_jsdmx(resp.json())
        try:
            idx_name = df.index.name # hack for pandas 0.16.2
            df.index = pd.to_datetime(df.index)
            df = df.sort_index()
            df = df.truncate(self.start, self.end)
            df.index.name = idx_name
        except ValueError:
            pass
        return df
