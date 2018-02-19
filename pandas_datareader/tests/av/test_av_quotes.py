import pytest

import pandas_datareader.data as web
import pandas as pd
from pandas.testing import assert_index_equal

import numpy as np


class TestAVQuotes(object):

    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    def test_invalid_symbol(self):
        with pytest.raises(ValueError):
            web.get_quote_av("BADSYMBOL")

    def test_bad_multiple_symbol(self):
        with pytest.raises(ValueError):
            web.get_quote_av(["AAPL", "BADSYMBOL"])

    def test_single_symbol(self):
        df = web.get_quote_av("AAPL")
        assert len(df) == 1

        expected = pd.Index(["price", "volume", "timestamp"])
        assert_index_equal(df.columns, expected)

    def test_multi_symbol(self):
        df = web.get_quote_av(["AAPL", "TSLA"])
        assert len(df) == 2

        expected = pd.Index(["price", "volume", "timestamp"])
        assert_index_equal(df.columns, expected)

    @pytest.mark.xfail(reason="May return NaN outside of market hours")
    def test_return_types(self):
        df = web.get_quote_av("AAPL")

        assert isinstance(df["AAPL"]["price"], np.int64)
        assert isinstance(df["AAPL"]["volume"], np.float64)
