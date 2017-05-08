import pytest
import pandas.util.testing as tm
import pandas_datareader.data as web

from pandas_datareader._utils import RemoteDataError


class TestNasdaqSymbols(tm.TestCase):

    def test_get_symbols(self):
        try:
            symbols = web.DataReader('symbols', 'nasdaq')
        except RemoteDataError as e:
            pytest.skip(e)

        assert 'IBM' in symbols.index
