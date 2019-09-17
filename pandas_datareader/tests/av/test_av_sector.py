import os

import pandas as pd
import pytest

from pandas_datareader.data import get_sector_performance_av

TEST_API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
TEST_API_KEY = None if not TEST_API_KEY else TEST_API_KEY

pytestmark = [
    pytest.mark.requires_api_key,
    pytest.mark.alpha_vantage,
    pytest.mark.skipif(TEST_API_KEY is None, reason="ALPHAVANTAGE_API_KEY not set"),
]


class TestAVSector(object):
    @classmethod
    def setup_class(cls):
        pytest.importorskip("lxml")

    @pytest.mark.skipif(TEST_API_KEY is None, reason="ALPHAVANTAGE_API_KEY not set")
    def test_sector(self):
        df = get_sector_performance_av(retry_count=6, pause=20.5)

        cols = pd.Index(["RT", "1D", "5D", "1M", "3M", "YTD", "1Y", "3Y", "5Y", "10Y"])
        assert df.columns.equals(cols)
        assert "Energy" in df.index
        assert "Real Estate" in df.index
