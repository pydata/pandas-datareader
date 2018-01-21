from datetime import datetime

import pytest
from pandas import DataFrame

from pandas_datareader.data import (DataReader, get_summary_iex, get_last_iex,
                                    get_dailysummary_iex, get_iex_symbols,
                                    get_iex_book)


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
        assert df.T["averageDailyVolume"].iloc[0] == 137650908.9

    def test_false_ticker(self):
        df = get_last_iex("INVALID TICKER")
        assert df.shape[0] == 0

    @pytest.mark.xfail(reason='IEX daily history API is returning 500 as of '
                              'Jan 2018')
    def test_daily(self):
        df = get_dailysummary_iex(start=datetime(2017, 5, 5),
                                  end=datetime(2017, 5, 6))
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
        dob = get_iex_book('GS', service='book')
        if dob:
            assert 'GS' in dob
        else:
            pytest.xfail(reason='Can only get Book when market open')
