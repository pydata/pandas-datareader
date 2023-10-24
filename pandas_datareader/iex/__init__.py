import json
from urllib.parse import urlencode

import pandas as pd

from pandas_datareader.base import _BaseReader

# Data provided for free by IEX
# Data is furnished in compliance with the guidelines promulgated in the IEX
# API terms of service and manual
# See https://iextrading.com/api-exhibit-a/ for additional information
# and conditions of use


class IEX(_BaseReader):
    """
    Serves as the base class for all IEX API services.
    """

    _format = "json"

    def __init__(
        self, symbols=None, start=None, end=None, retry_count=3, pause=0.1, session=None
    ):
        super().__init__(
            symbols=symbols,
            start=start,
            end=end,
            retry_count=retry_count,
            pause=pause,
            session=session,
        )

    @property
    def service(self):
        """Service endpoint"""
        # This property will be overridden by the subclass
        raise NotImplementedError("IEX API service not specified.")

    @property
    def url(self):
        """API URL"""
        qstring = urlencode(self._get_params(self.symbols))
        return f"https://api.iextrading.com/1.0/{self.service}?{qstring}"

    def read(self):
        """Read data"""
        df = super().read()
        if isinstance(df, pd.DataFrame):
            df = df.squeeze()
            if not isinstance(df, pd.DataFrame):
                df = pd.DataFrame(df)
        return df

    def _get_params(self, symbols):
        p = {}
        if isinstance(symbols, list):
            p["symbols"] = ",".join(symbols)
        elif isinstance(symbols, str):
            p["symbols"] = symbols
        return p

    def _output_error(self, out):
        """If IEX returns a non-200 status code, we need to notify the user of
        the error returned.

        Parameters
        ----------
        out: bytes
            The raw output from an HTTP request
        """
        try:
            content = json.loads(out.text)
        except Exception as exc:
            raise TypeError("Failed to interpret response as JSON.") from exc

        for key, string in content.items():
            e = f"IEX Output error encountered: {string}"
            if key == "error":
                raise Exception(e)

    def _read_lines(self, out):
        """IEX's output does not need anything complex, so we're overriding to
        use Pandas' default interpreter

        Parameters
        ----------
        out: bytes
            The raw output from an HTTP request

        Returns
        -------
        DataFrame
        """

        # IEX will return a blank line for invalid tickers:
        if isinstance(out, list):
            out = [x for x in out if x is not None]
        return pd.DataFrame(out) if len(out) > 0 else pd.DataFrame()
