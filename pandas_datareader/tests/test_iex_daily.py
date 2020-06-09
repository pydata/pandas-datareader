from datetime import date, datetime, timedelta
import os

from pandas import DataFrame, MultiIndex
import pytest

import pandas_datareader.data as web
from pandas_datareader.iex.daily import IEXDailyReader


@pytest.mark.skipif(
    os.getenv("IEX_SANDBOX") != "enable", reason="All tests must be run in sandbox mode"
)
class TestIEXDaily(object):
    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    @property
    def start(self):
        return datetime(2015, 2, 9)

    @property
    def end(self):
        return datetime(2017, 5, 24)

    def test_iex_bad_symbol(self):
        with pytest.raises(Exception):
            web.DataReader("BADTICKER", "iex,", self.start, self.end)

    def test_iex_bad_symbol_list(self):
        with pytest.raises(Exception):
            web.DataReader(["AAPL", "BADTICKER"], "iex", self.start, self.end)

    def test_daily_invalid_date(self):
        start = datetime(2000, 1, 5)
        end = datetime(2017, 5, 24)
        with pytest.raises(Exception):
            web.DataReader(["AAPL", "TSLA"], "iex", start, end)

    def test_single_symbol(self):
        df = web.DataReader("AAPL", "iex", self.start, self.end)
        assert list(df) == ["open", "high", "low", "close", "volume"]
        assert len(df) == 578

    def test_multiple_symbols(self):
        syms = ["AAPL", "MSFT", "TSLA"]
        df = web.DataReader(syms, "iex", self.start, self.end)
        assert sorted(list(df.columns.levels[1])) == syms
        for sym in syms:
            assert len(df.xs(sym, level="Symbols", axis=1) == 578)

    def test_multiple_symbols_2(self):
        syms = ["AAPL", "MSFT", "TSLA"]
        good_start = datetime(2017, 2, 9)
        good_end = datetime(2017, 5, 24)
        df = web.DataReader(syms, "iex", good_start, good_end)
        assert isinstance(df, DataFrame)
        assert isinstance(df.columns, MultiIndex)
        assert len(df.columns.levels[1]) == 3
        assert sorted(list(df.columns.levels[1])) == syms

        a = df.xs("AAPL", axis=1, level="Symbols")
        t = df.xs("TSLA", axis=1, level="Symbols")

        assert len(a) == 73
        assert len(t) == 73

    def test_range_string_from_date(self):
        syms = ["AAPL"]

        assert (
            IEXDailyReader(
                symbols=syms, start=date.today() - timedelta(days=5), end=date.today()
            )._range_string_from_date()
            == "5d"
        )
        assert (
            IEXDailyReader(
                symbols=syms, start=date.today() - timedelta(days=27), end=date.today()
            )._range_string_from_date()
            == "1m"
        )
        assert (
            IEXDailyReader(
                symbols=syms, start=date.today() - timedelta(days=83), end=date.today()
            )._range_string_from_date()
            == "3m"
        )
        assert (
            IEXDailyReader(
                symbols=syms, start=date.today() - timedelta(days=167), end=date.today()
            )._range_string_from_date()
            == "6m"
        )
        assert (
            IEXDailyReader(
                symbols=syms, start=date.today() - timedelta(days=170), end=date.today()
            )._range_string_from_date()
            == "1y"
        )
        assert (
            IEXDailyReader(
                symbols=syms, start=date.today() - timedelta(days=365), end=date.today()
            )._range_string_from_date()
            == "2y"
        )
        assert (
            IEXDailyReader(
                symbols=syms, start=date.today() - timedelta(days=730), end=date.today()
            )._range_string_from_date()
            == "5y"
        )
        assert (
            IEXDailyReader(
                symbols=syms,
                start=date.today() - timedelta(days=1826),
                end=date.today(),
            )._range_string_from_date()
            == "max"
        )
