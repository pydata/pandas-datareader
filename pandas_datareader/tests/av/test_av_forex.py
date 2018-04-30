import os
import pytest

import pandas as pd

from pandas_datareader import data as web

TEST_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')
TEST_API_KEY = None if not TEST_API_KEY else TEST_API_KEY


class TestAlphaVantageForex(object):

    @classmethod
    def setup_class(cls):
        pytest.importorskip('lxml')

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    def test_bad_pair_format(self):
        with pytest.raises(ValueError):
            web.DataReader("BAD FORMAT", "av-forex")

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    def test_bad_pairs_format(self):
        with pytest.raises(ValueError):
            web.DataReader(["USD/JPY", "BAD FORMAT"], "av-forex")

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    def test_one_pair(self):
        df = web.DataReader("USD/EUR", "av-forex")
        assert isinstance(df, pd.DataFrame)
        assert df.loc["To_Currency Name"][0] == "Euro"
        assert df.loc["Time Zone"][0] == 'UTC'

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    def test_multiple_pairs(self):
        pairs = ["USD/JPY", "EUR/JPY"]
        df = web.DataReader(pairs, "av-forex")
        assert isinstance(df, pd.DataFrame)
        assert df.columns.equals(pd.Index(pairs))
