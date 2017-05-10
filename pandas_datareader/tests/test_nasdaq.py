import pytest
import pandas_datareader.data as web

from pandas_datareader._utils import RemoteDataError


class TestNasdaqSymbols(object):

    def test_get_symbols(self):
        try:
            symbols = web.DataReader('symbols', 'nasdaq')
        except RemoteDataError as e:
            pytest.skip(e)

        assert 'IBM' in symbols.index
