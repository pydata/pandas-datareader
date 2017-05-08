from datetime import datetime
from pandas import DataFrame
from pandas_datareader.data import *

import nose
import pandas.util.testing as tm
from pandas.util.testing import (assert_series_equal, assert_frame_equal)


class TestIEX(tm.TestCase):
    @classmethod
    def setUpClass(cls):
        super(TestIEX, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(TestIEX, cls).tearDownClass()

    def test_read_iex(self):
        gs = DataReader("GS", "iex-last")
        assert isinstance(gs, DataFrame)

    def test_historical(self):
        df = get_summary_iex(start=datetime(2017, 4, 1), end=datetime(2017, 4, 30))
        self.assertEqual(df["averageDailyVolume"].iloc[0], 137650908.9)

    def test_false_ticker(self):
        df = get_last_iex("INVALID TICKER")
        assert_frame_equal(df, DataFrame())

    def test_daily(self):
        df = get_data_iex(start=datetime(2017, 5, 5), end=datetime(2017, 5, 6))
        self.assertEqual(df['routedVolume'].iloc[0], 39974788)

    def test_symbols(self):
        df = get_iex_symbols()
        self.assertTrue('GS' in df.symbol.values)

    def test_live_prices(self):
        dftickers = get_iex_symbols()
        tickers = dftickers[:5].symbol.values
        df = get_last_iex(tickers[:5])
        self.assertGreater(df["price"].mean(), 0)

if __name__ == '__main__':
    nose.runmodule(argv=[__file__, '-vvs', '-x', '--pdb', '--pdb-failure'],
                   exit=False)  # pragma: no cover
