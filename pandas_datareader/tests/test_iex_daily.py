from datetime import datetime

import pytest

import pandas_datareader.data as web


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
        start = datetime(2010, 1, 5)
        end = datetime(2017, 5, 24)
        with pytest.raises(Exception):
            web.DataReader(["AAPL", "TSLA"], "iex", start, end)

    def test_single_symbol(self):
        df = web.DataReader("AAPL", "iex", self.start, self.end)
        assert list(df) == ["open", "high", "low", "close", "volume"]
        assert len(df) == 578
        assert df["volume"][-1] == 19219154

    def test_multiple_symbols(self):
        syms = ["AAPL", "MSFT", "TSLA"]
        df = web.DataReader(syms, "iex", self.start, self.end)
        assert sorted(list(df)) == syms
        for sym in syms:
            assert len(df[sym] == 578)

    def test_multiple_symbols_2(self):
        syms = ["AAPL", "MSFT", "TSLA"]
        good_start = datetime(2017, 2, 9)
        good_end = datetime(2017, 5, 24)
        df = web.DataReader(syms, "iex", good_start, good_end)
        assert isinstance(df, dict)
        assert len(df) == 3
        assert sorted(list(df)) == syms

        a = df["AAPL"]
        t = df["TSLA"]

        assert len(a) == 73
        assert len(t) == 73

        expected1 = a.loc["2017-02-09"]
        assert expected1["close"] == 132.42
        assert expected1["high"] == 132.445

        expected2 = a.loc["2017-05-24"]
        assert expected2["close"] == 153.34
        assert expected2["high"] == 154.17

        expected3 = t.loc["2017-02-09"]
        assert expected3["close"] == 269.20
        assert expected3["high"] == 271.18

        expected4 = t.loc["2017-05-24"]
        assert expected4["close"] == 310.22
        assert expected4["high"] == 311.0
