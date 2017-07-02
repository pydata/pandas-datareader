import zlib
import os
import time
from pandas.compat import StringIO

import pandas.compat as compat
import pandas as pd
import requests

from pandas_datareader.base import _BaseReader


class EnigmaReader(_BaseReader):
    """
    Collects Enigma data located at the specified datapath and
    returns a pandas DataFrame.

    Usage (high-level):
    ```
        import pandas_datareader as pdr
        df = pdr.get_data_enigma('enigma.inspections.restaurants.fl')

        # in the event that ENIGMA_API_KEY does not exist in your env,
        # it can be supplied as the second arg:
        df = prd.get_data_enigma('enigma.inspections.restaurants.fl',
        ...                      'ARIAMFHKJMISF38UT')
    ```

    Usage:
    ```
        df = EnigmaReader(datapath='enigma.inspections.restaurants.fl',
        ...               api_key='ARIAMFHKJMISF38UT').read()
    ```
    """

    def __init__(self,
                 datapath=None,
                 api_key=None,
                 retry_count=5,
                 pause=0.250,
                 session=None):

        super(EnigmaReader, self).__init__(symbols=[],
                                           retry_count=retry_count,
                                           pause=pause)
        if api_key is None:
            self._api_key = os.getenv('ENIGMA_API_KEY')
            if self._api_key is None:
                raise ValueError("Please provide an Enigma API key or set "
                                 "the ENIGMA_API_KEY environment variable\n"
                                 "If you do not have an API key, you can get "
                                 "one here: https://app.enigma.io/signup")
        else:
            self._api_key = api_key

        self._datapath = datapath
        if not isinstance(self._datapath, compat.string_types):
            raise ValueError(
                "The Enigma datapath must be a string (ex: "
                "'enigma.inspections.restaurants.fl')")

    @property
    def url(self):
        return 'https://api.enigma.io/v2/export/{}/{}'.format(self._api_key,
                                                              self._datapath)

    @property
    def export_key(self):
        return 'export_url'

    @property
    def _head_key(self):
        return 'head_url'

    def _request(self, url):
        self.session.headers.update({'User-Agent': 'pandas-datareader'})
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp

    def _decompress_export(self, compressed_export_data):
        return zlib.decompress(compressed_export_data, 16 + zlib.MAX_WBITS)

    def extract_export_url(self, delay=10, max_attempts=10):
        """
        Performs an HTTP HEAD request on 'head_url' until it returns a `200`.
        This allows the Enigma API time to export the requested data.
        """
        resp = self._request(self.url)
        attempts = 0
        while True:
            try:
                requests.head(resp.json()[self._head_key]).raise_for_status()
            except Exception as e:
                attempts += 1
                if attempts > max_attempts:
                    raise e
                time.sleep(delay)
                continue
            return resp.json()[self.export_key]

    def read(self):
        try:
            return self._read()
        finally:
            self.close()

    def _read(self):
        export_gzipped_req = self._request(self.extract_export_url())
        decompressed_data = self._decompress_export(
            export_gzipped_req.content).decode("utf-8")
        return pd.read_csv(StringIO(decompressed_data))
