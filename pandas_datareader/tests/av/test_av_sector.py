import os
import pytest

from pandas_datareader.data import get_sector_performance_av

import pandas as pd

TEST_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')
TEST_API_KEY = None if not TEST_API_KEY else TEST_API_KEY


class TestAVSector(object):

    @classmethod
    def setup_class(cls):
        pytest.importorskip('lxml')

    @pytest.mark.skipif(TEST_API_KEY is None,
                        reason="ALPHAVANTAGE_API_KEY not set")
    def test_sector(self):
        df = get_sector_performance_av()

        cols = pd.Index(["RT", "1D", "5D", "1M", "3M", "YTD", "1Y", "3Y", "5Y",
                         "10Y"])
        assert df.columns.equals(cols)
        assert "Energy" in df.index
        assert "Real Estate" in df.index
