from __future__ import unicode_literals

import pandas as pd

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import string_types
from pandas_datareader.io.sdmx import _read_sdmx_dsd, read_sdmx


class EurostatReader(_BaseReader):
    """Get data for the given name from Eurostat."""

    _URL = "http://ec.europa.eu/eurostat/SDMX/diss-web/rest"

    @property
    def url(self):
        """API URL"""
        if not isinstance(self.symbols, string_types):
            raise ValueError("data name must be string")

        q = "{0}/data/{1}/?startperiod={2}&endperiod={3}"
        return q.format(self._URL, self.symbols, self.start.year, self.end.year)

    @property
    def dsd_url(self):
        """API DSD URL"""
        if not isinstance(self.symbols, string_types):
            raise ValueError("data name must be string")

        return "{0}/datastructure/ESTAT/DSD_{1}".format(self._URL, self.symbols)

    def _read_one_data(self, url, params):
        resp_dsd = self._get_response(self.dsd_url)
        dsd = _read_sdmx_dsd(resp_dsd.content)

        resp = self._get_response(url)
        data = read_sdmx(resp.content, dsd=dsd)

        try:
            data.index = pd.to_datetime(data.index)
            data = data.sort_index()
        except ValueError:
            pass

        try:
            data = data.truncate(self.start, self.end)
        except TypeError:
            pass

        return data
