import pytest

import pandas.util.testing as tm

from pandas import DataFrame
from datetime import datetime
from pandas_datareader.data import (DataReader, get_summary_iex, get_last_iex,
                                    get_data_iex, get_iex_symbols)


class TestIEX(object):
    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    def test_read_iex(self):
        gs = DataReader("GS", "iex-last")
        assert isinstance(gs, DataFrame)

    def test_historical(self):
        df = get_summary_iex(start=datetime(2017, 4, 1),
                             end=datetime(2017, 4, 30))
        assert df["averageDailyVolume"].iloc[0] == 137650908.9

    def test_false_ticker(self):
        df = get_last_iex("INVALID TICKER")
        tm.assert_frame_equal(df, DataFrame())

    def test_daily(self):
        df = get_data_iex(start=datetime(2017, 5, 5), end=datetime(2017, 5, 6))
        assert df['routedVolume'].iloc[0] == 39974788

    def test_symbols(self):
        df = get_iex_symbols()
        assert 'GS' in df.symbol.values

    def test_live_prices(self):
        dftickers = get_iex_symbols()
        tickers = dftickers[:5].symbol.values
        df = get_last_iex(tickers[:5])
        assert df["price"].mean() > 0

    def test_deep(self):
        dob = DataReader('GS', 'iex-book')
        assert 'GS' in dob
        assert 'asks' in dob['GS']
        assert dob['GS']['bids'][0]['price'] > 0
