import pandas_datareader.data as web


class TestNasdaqSymbols(object):

    def test_get_symbols(self):
        symbols = web.DataReader('symbols', 'nasdaq')
        assert 'IBM' in symbols.index
