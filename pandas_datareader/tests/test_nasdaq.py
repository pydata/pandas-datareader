import pytest
import pandas_datareader.data as web

from pandas_datareader._utils import RemoteDataError


class TestNasdaqSymbols(object):

    @pytest.mark.xfail(RemoteDataError, reason="remote data exception")
    def test_get_symbols(self):
        symbols = web.DataReader('symbols', 'nasdaq')
        assert 'IBM' in symbols.index
