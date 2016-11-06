import nose

import pandas.util.testing as tm

import pandas_datareader.data as web
from pandas_datareader._utils import RemoteDataError


class TestNasdaqSymbols(tm.TestCase):

    def test_get_symbols(self):
        try:
            symbols = web.DataReader('symbols', 'nasdaq')
        except RemoteDataError as e:
            raise nose.SkipTest(e)

        assert 'IBM' in symbols.index


if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)
