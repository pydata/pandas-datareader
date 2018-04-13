import os
import pytest

import pandas_datareader.data as web
import pandas as pd

import numpy as np

TEST_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')
TEST_API_KEY = None if not TEST_API_KEY else TEST_API_KEY


class TestAVQuotes(object):

    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    def test_invalid_symbol(self):
        with pytest.raises(ValueError):
            web.get_quote_av("BADSYMBOL")

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    def test_bad_multiple_symbol(self):
        with pytest.raises(ValueError):
            web.get_quote_av(["AAPL", "BADSYMBOL"])

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    def test_single_symbol(self):
        df = web.get_quote_av("AAPL")
        assert len(df) == 1

        expected = pd.Index(["price", "volume", "timestamp"])
        assert df.columns.equals(expected)

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    def test_multi_symbol(self):
        df = web.get_quote_av(["AAPL", "TSLA"])
        assert len(df) == 2

        expected = pd.Index(["price", "volume", "timestamp"])
        assert df.columns.equals(expected)

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    @pytest.mark.xfail(reason="May return NaN outside of market hours")
    def test_return_types(self):
        df = web.get_quote_av("AAPL")

        assert isinstance(df["AAPL"]["price"], np.int64)
        assert isinstance(df["AAPL"]["volume"], np.float64)
