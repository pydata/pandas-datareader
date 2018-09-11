import os

from pandas_datareader.base import _BaseReader
from pandas_datareader._utils import RemoteDataError

import pandas as pd

AV_BASE_URL = 'https://www.alphavantage.co/query'


class AlphaVantage(_BaseReader):
    """
    Base class for all Alpha Vantage queries

    Notes
    -----
    See `Alpha Vantage <https://www.alphavantage.co/>`__
    """
    _format = 'json'

    def __init__(self, symbols=None, start=None, end=None, retry_count=3,
                 pause=0.1, session=None, api_key=None):
        super(AlphaVantage, self).__init__(symbols=symbols, start=start,
                                           end=end, retry_count=retry_count,
                                           pause=pause, session=session)
        if api_key is None:
            api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        if not api_key or not isinstance(api_key, str):
            raise ValueError('The AlphaVantage API key must be provided '
                             'either through the api_key variable or '
                             'through the environment varaible '
                             'ALPHAVANTAGE_API_KEY')
        self.api_key = api_key

    @property
    def url(self):
        """ API URL """
        return AV_BASE_URL

    @property
    def params(self):
        return {
            'function': self.function,
            'apikey': self.api_key
        }

    @property
    def function(self):
        """ Alpha Vantage endpoint function"""
        raise NotImplementedError

    @property
    def data_key(self):
        """ Key of data returned from Alpha Vantage """
        raise NotImplementedError

    def _read_lines(self, out):
        try:
            df = pd.DataFrame.from_dict(out[self.data_key], orient='index')
        except KeyError:
            if "Error Message" in out:
                raise ValueError("The requested symbol {} could not be "
                                 "retrived. Check valid ticker"
                                 ".".format(self.symbols))
            else:
                raise RemoteDataError()
        df = df[sorted(df.columns)]
        df.columns = [id[3:] for id in df.columns]
        return df
