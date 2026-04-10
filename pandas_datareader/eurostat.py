import pandas as pd

from pandas_datareader.base import _BaseReader
from pandas_datareader.io.sdmx import _read_sdmx_dsd, read_sdmx


class EurostatReader(_BaseReader):
    """Get data for the given name from Eurostat."""

    _URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"

    @property
    def url(self):
        """API URL"""
        if not isinstance(self.symbols, str):
            raise ValueError("data name must be string")

        q = "{0}/data/{1}?startPeriod={2}&endPeriod={3}"
        return q.format(self._URL, self.symbols, self.start.year, self.end.year)

    @property
    def dsd_url(self):
        """API DSD URL"""
        if not isinstance(self.symbols, str):
            raise ValueError("data name must be string")

        return f"{self._URL}/datastructure/ESTAT/{self.symbols}/latest?references=descendants"

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

        return self._format_columns(data)

    @staticmethod
    def _format_columns(data):
        if not isinstance(data.columns, pd.MultiIndex):
            return data

        columns = data.columns
        level_names = list(columns.names)

        priority = {
            "currency": 0,
            "indic_bt": 0,
            "statinfo": 1,
            "unit": 2,
            "siec": 3,
            "s_adj": 3,
            "tax": 4,
            "cpa2_1": 4,
            "nrg_cons": 5,
            "geo": 98,
            "freq": 99,
        }
        order = sorted(
            range(len(level_names)),
            key=lambda i: (priority.get(level_names[i], 50), i),
        )
        if order != list(range(len(level_names))):
            columns = columns.reorder_levels(order)
            level_names = [level_names[i] for i in order]

        columns = columns.set_names([name.upper() for name in level_names])
        data.columns = columns
        return data
