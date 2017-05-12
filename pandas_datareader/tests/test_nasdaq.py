import pandas_datareader.data as web

from pandas_datareader._utils import RemoteDataError
from pandas_datareader._testing import skip_on_exception


class TestNasdaqSymbols(object):

    @skip_on_exception(RemoteDataError)
    def test_get_symbols(self):
        symbols = web.DataReader('symbols', 'nasdaq')
        assert 'IBM' in symbols.index
