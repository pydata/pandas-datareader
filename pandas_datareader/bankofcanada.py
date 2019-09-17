from __future__ import unicode_literals

from pandas_datareader.base import _BaseReader
from pandas_datareader.compat import string_types


class BankOfCanadaReader(_BaseReader):
    """Get data for the given name from Bank of Canada.

    Notes
    -----
    See `Bank of Canada <https://www.bankofcanada.ca/rates/>`__"""

    _URL = "http://www.bankofcanada.ca/valet/observations"

    @property
    def url(self):
        """API URL"""
        if not isinstance(self.symbols, string_types):
            raise ValueError("data name must be string")

        return "{0}/{1}/csv".format(self._URL, self.symbols)

    @property
    def params(self):
        """Parameters to use in API calls"""
        return {
            "start_date": self.start.strftime("%Y-%m-%d"),
            "end_date": self.end.strftime("%Y-%m-%d"),
        }

    @staticmethod
    def _sanitize_response(response):
        """
        Clean up the response string
        """
        data = response.text.split("OBSERVATIONS")[1]
        return data.split("ERRORS")[0].strip()
